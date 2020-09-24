/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.Progressable;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.mapreduce.IcebergWritable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveIcebergOutputFormat implements OutputFormat<NullWritable, IcebergWritable>,
    HiveOutputFormat<NullWritable, IcebergWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergOutputFormat.class);
  private static final String TASK_ATTEMPT_ID_KEY = "mapred.task.id";
  private static final String COMMITTED_EXTENSION = ".committed";

  // <TaskAttemptId, WriteData> map to store the data needed to create DataFiles
  // Stored in concurrent map, since some executor engines can share containers
  private static final Map<TaskAttemptID, WriteData> fileData = new ConcurrentHashMap<>();

  private Configuration overlayedConf = null;
  private TaskAttemptID taskAttemptId = null;
  private Schema schema = null;
  private PartitionSpec spec = null;
  private FileFormat fileFormat = null;

  @Override
  @SuppressWarnings("rawtypes")
  public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath, Class valueClass,
      boolean isCompressed, Properties tableAndSerDeProperties, Progressable progress) throws IOException {

    this.overlayedConf = createOverlayedConf(jc, tableAndSerDeProperties);
    this.taskAttemptId = TaskAttemptID.forName(overlayedConf.get(TASK_ATTEMPT_ID_KEY));
    this.schema = SchemaParser.fromJson(overlayedConf.get(InputFormatConfig.TABLE_SCHEMA));
    this.spec = PartitionSpecParser.fromJson(schema, overlayedConf.get(InputFormatConfig.PARTITION_SPEC));
    String fileFormatString =
        overlayedConf.get(InputFormatConfig.WRITE_FILE_FORMAT, InputFormatConfig.WRITE_FILE_FORMAT_DEFAULT.name());
    this.fileFormat = FileFormat.valueOf(fileFormatString);

    fileData.remove(this.taskAttemptId);

    return new IcebergRecordWriter(generateDataFileLocation(overlayedConf, taskAttemptId));
  }

  /**
   * Returns the union of the configuration and table properties with the
   * table properties taking precedence.
   */
  private static Configuration createOverlayedConf(Configuration conf, Properties tblProps) {
    Configuration newConf = new Configuration(conf);
    for (Map.Entry<Object, Object> prop : tblProps.entrySet()) {
      newConf.set((String) prop.getKey(), (String) prop.getValue());
    }
    return newConf;
  }

  /**
   * Generates query directory location based on the configuration.
   * Currently it uses tableLocation/queryId
   * @param conf The job's configuration
   * @return The directory to store the query result files
   */
  private static String generateQueryLocation(Configuration conf) {
    String tableLocation = conf.get(InputFormatConfig.TABLE_LOCATION);
    String queryId = conf.get(HiveConf.ConfVars.HIVEQUERYID.varname);
    return tableLocation + "/" + queryId;
  }

  /**
   * Generates the job temp location based on the job configuration.
   * Currently it uses QUERY_LOCATION/jobId.
   * @param conf The job's configuration
   * @param jobId The JobID for the task
   * @return The file to store the results
   */
  private static String generateJobLocation(Configuration conf, JobID jobId) {
    return generateQueryLocation(conf) + "/" + jobId;
  }

  /**
   * Generates datafile location based on the task configuration.
   * Currently it uses QUERY_LOCATION/jobId/taskAttemptId.
   * @param conf The job's configuration
   * @param taskAttemptId The TaskAttemptID for the task
   * @return The file to store the results
   */
  private static String generateDataFileLocation(Configuration conf, TaskAttemptID taskAttemptId) {
    return generateJobLocation(conf, taskAttemptId.getJobID()) + "/" + taskAttemptId.toString();
  }

  /**
   * Generates commit file location based on the task configuration and a specific task id.
   * Currently it uses QUERY_LOCATION/jobId/task-[0..numTasks].committed.
   * @param conf The job's configuration
   * @param jobId The jobId for the task
   * @param taskId The taskId for the commit file
   * @return The file to store the results
   */
  private static String generateCommitFileLocation(Configuration conf, JobID jobId, int taskId) {
    return generateJobLocation(conf, jobId) + "/task-" + taskId + COMMITTED_EXTENSION;
  }

  /**
   * Generates commit file location based on the task configuration.
   * Currently it uses QUERY_LOCATION/jobId/task-[0..numTasks].committed.
   * @param conf The job's configuration
   * @param taskAttemptId The TaskAttemptID for the task
   * @return The file to store the results
   */
  private static String generateCommitFileLocation(Configuration conf, TaskAttemptID taskAttemptId) {
    return generateCommitFileLocation(conf, taskAttemptId.getJobID(), taskAttemptId.getTaskID().getId());
  }

  @Override
  public org.apache.hadoop.mapred.RecordWriter<NullWritable, IcebergWritable> getRecordWriter(FileSystem ignored,
      JobConf job, String name, Progressable progress) {

    throw new UnsupportedOperationException("Please implement if needed");
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) {
    // Not doing any check.
  }

  protected class IcebergRecordWriter extends org.apache.hadoop.mapreduce.RecordWriter<NullWritable, IcebergWritable>
      implements FileSinkOperator.RecordWriter, org.apache.hadoop.mapred.RecordWriter<NullWritable, IcebergWritable> {

    private final String location;
    private final Map<PartitionKey, AppenderWrapper> appenders;
    private final FileIO io;
    private final GenericAppenderFactory appenderFactory;
    private final PartitionKey currentKey;

    IcebergRecordWriter(String location) {
      this.location = location;
      this.io = new HadoopFileIO(overlayedConf);
      this.appenderFactory = new GenericAppenderFactory(schema);

      this.appenders = new HashMap<>();
      this.currentKey = new PartitionKey(spec, schema);
      LOG.info("IcebergRecordWriter is created in {} with {}", location, fileFormat);
    }

    @Override
    public void write(Writable row) {
      Preconditions.checkArgument(row instanceof IcebergWritable);

      Record record = ((IcebergWritable) row).record();

      currentKey.partition(record);

      AppenderWrapper currentAppender = appenders.get(currentKey);
      if (currentAppender == null) {
        currentAppender = getAppender();
        appenders.put(currentKey.copy(), currentAppender);
      }

      currentAppender.appender.add(record);
    }

    @Override
    public void write(NullWritable key, IcebergWritable value) {
      write(value);
    }

    @Override
    public void close(boolean abort) throws IOException {
      Set<ClosedFileData> fileDataSet = new HashSet<>(appenders.size());
      for (PartitionKey key : appenders.keySet()) {
        AppenderWrapper wrapper = appenders.get(key);
        wrapper.close();
        fileDataSet.add(new ClosedFileData(key, wrapper.location, wrapper.length(), wrapper.metrics()));
      }
      if (!abort) {
        fileData.put(taskAttemptId, new WriteData(fileFormat, fileDataSet));
      }
    }

    @Override
    public void close(org.apache.hadoop.mapreduce.TaskAttemptContext context) throws IOException {
      close(false);
    }

    @Override
    public void close(Reporter reporter) throws IOException {
      close(false);
    }

    private AppenderWrapper getAppender() {
      String dataFileLocation = location + UUID.randomUUID();
      OutputFile dataFile = io.newOutputFile(dataFileLocation);

      appenderFactory.newAppender(dataFile, fileFormat);
      FileAppender<Record> appender = appenderFactory.newAppender(dataFile, fileFormat);

      return new AppenderWrapper(appender, dataFileLocation);
    }
  }

  /**
   * A dummy committer - not related to the Hive transactions.
   */
  public static final class IcebergOutputCommitter extends OutputCommitter {

    @Override
    public void setupJob(JobContext jobContext) {
      // do nothing.
    }

    @Override
    public void setupTask(TaskAttemptContext taskAttemptContext) {
      // do nothing.
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext context) {
      // We need to commit if this is the last phase of a MapReduce process
      return TaskType.REDUCE.equals(context.getTaskAttemptID().getTaskID().getTaskType()) ||
          context.getJobConf().getNumReduceTasks() == 0;
    }

    @Override
    public void commitTask(TaskAttemptContext context) throws IOException {
      String commitFileLocation = generateCommitFileLocation(context.getJobConf(), context.getTaskAttemptID());

      WriteData writeData = fileData.remove(context.getTaskAttemptID());

      // Generate empty closed file data
      if (writeData == null) {
        writeData = new WriteData();
      }

      // Create the committed file for the task
      createCommittedFileFor(new HadoopFileIO(context.getJobConf()), writeData, commitFileLocation);
    }

    @Override
    public void abortTask(TaskAttemptContext context) {
      FileIO io = new HadoopFileIO(context.getJobConf());

      // Clean up local cache for metadata
      fileData.remove(context.getTaskAttemptID());

      // Remove the result file for the failed task
      Tasks.foreach(generateDataFileLocation(context.getJobConf(), context.getTaskAttemptID()))
          .retry(3)
          .suppressFailureWhenFinished()
          .onFailure((file, exc) -> LOG.debug("Failed on to remove file {} on abort task", file, exc))
          .run(io::deleteFile);
    }

    @Override
    public void commitJob(JobContext jobContext) throws IOException {
      JobConf conf = jobContext.getJobConf();
      // If there are reducers, then every reducer will generate a result file.
      // If this is a map only task, then every mapper will generate a result file.
      int expectedFiles = conf.getNumReduceTasks() != 0 ? conf.getNumReduceTasks() : conf.getNumMapTasks();
      Table table = Catalogs.loadTable(conf);

      ExecutorService executor = null;
      try {
        // Creating executor service for parallel handling of file reads
        executor = Executors.newFixedThreadPool(
            conf.getInt(InputFormatConfig.WRITE_THREAD_POOL_SIZE, InputFormatConfig.WRITE_THREAD_POOL_SIZE_DEFAULT),
            new ThreadFactoryBuilder()
                .setDaemon(false)
                .setPriority(Thread.NORM_PRIORITY)
                .setNameFormat("iceberg-commit-pool-%d")
                .build());

        Set<DataFile> dataFiles = new ConcurrentHashMap<>().newKeySet();

        // Reading the committed files. The assumption here is that the taskIds are generated in sequential order
        // starting from 0.
        Tasks.range(expectedFiles)
            .executeWith(executor)
            .retry(3)
            .run(taskId -> {
              String taskFileName = generateCommitFileLocation(conf, jobContext.getJobID(), taskId);
              WriteData writeData = readCommittedFile(table.io(), taskFileName);

              // If the data is not empty add to the table
              if (!writeData.empty) {
                writeData.fileDataSet.forEach(file -> {
                  DataFiles.Builder builder = DataFiles.builder(table.spec())
                      .withPath(file.fileName)
                      .withFormat(writeData.fileFormat)
                      .withFileSizeInBytes(file.length)
                      .withPartition(file.partitionKey)
                      .withMetrics(file.metrics);
                  dataFiles.add(builder.build());
                });
              }
            });

        if (dataFiles.size() > 0) {
          // Appending data files to the table
          AppendFiles append = table.newAppend();
          Set<String> addedFiles = new HashSet<>(dataFiles.size());
          dataFiles.forEach(dataFile -> {
            append.appendFile(dataFile);
            addedFiles.add(dataFile.path().toString());
          });
          append.commit();
          LOG.info("Iceberg write is committed for {} with files {}", table, addedFiles);
        } else {
          LOG.info("Iceberg write is committed for {} with no new files", table);
        }

        // Calling super to cleanupJob if something more is needed
        cleanupJob(jobContext);

      } finally {
        if (executor != null) {
          executor.shutdown();
        }
      }
    }

    @Override
    public void abortJob(JobContext context, int status) throws IOException {
      // Remove the result directory for the failed job
      Tasks.foreach(generateJobLocation(context.getJobConf(), context.getJobID()))
          .retry(3)
          .suppressFailureWhenFinished()
          .onFailure((file, exc) -> LOG.debug("Failed on to remove directory {} on abort job", file, exc))
          .run(file -> {
            Path toDelete = new Path(file);
            FileSystem fs = Util.getFs(toDelete, context.getJobConf());
            try {
              fs.delete(toDelete, true /* recursive */);
            } catch (IOException e) {
              throw new RuntimeIOException(e, "Failed to delete job directory: %s", file);
            }
          });
      cleanupJob(context);
    }
  }

  private static void createCommittedFileFor(FileIO io, WriteData writeData, String location)
      throws IOException {

    OutputFile commitFile = io.newOutputFile(location);
    ObjectOutputStream oos = new ObjectOutputStream(commitFile.createOrOverwrite());
    oos.writeObject(writeData);
    oos.close();
    LOG.debug("Iceberg committed file is created {}", commitFile);
  }

  private static WriteData readCommittedFile(FileIO io, String committedFileLocation) {
    try (ObjectInputStream ois = new ObjectInputStream(io.newInputFile(committedFileLocation).newStream())) {
      return (WriteData) ois.readObject();
    } catch (ClassNotFoundException | IOException e) {
      throw new NotFoundException("Can not read or parse committed file: " + committedFileLocation, e);
    }
  }

  private static final class AppenderWrapper {
    private final FileAppender<Record> appender;
    private final String location;

    AppenderWrapper(FileAppender<Record> appender, String location) {
      this.appender = appender;
      this.location = location;
    }

    public long length() {
      return appender.length();
    }

    public Metrics metrics() {
      return appender.metrics();
    }

    public void close() throws IOException {
      appender.close();
    }
  }

  private static final class WriteData implements Serializable {
    private final boolean empty;
    private final FileFormat fileFormat;
    private final Set<ClosedFileData> fileDataSet;

    WriteData(FileFormat fileFormat, Set<ClosedFileData> fileDataSet) {
      this.empty = false;
      this.fileFormat = fileFormat;
      this.fileDataSet = fileDataSet;
    }

    WriteData() {
      empty = true;
      fileFormat = null;
      fileDataSet = null;
    }
  }

  private static final class ClosedFileData implements Serializable {
    private final PartitionKey partitionKey;
    private final String fileName;
    private final Long length;
    private final Metrics metrics;

    ClosedFileData(PartitionKey partitionKey, String fileName, Long length, Metrics metrics) {
      this.partitionKey = partitionKey;
      this.fileName = fileName;
      this.length = length;
      this.metrics = metrics;
    }
  }
}

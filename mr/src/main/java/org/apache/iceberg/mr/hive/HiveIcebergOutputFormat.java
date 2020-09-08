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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
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
import org.apache.hadoop.util.Progressable;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.mapreduce.IcebergWritable;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveIcebergOutputFormat implements OutputFormat<NullWritable, IcebergWritable>,
    HiveOutputFormat<NullWritable, IcebergWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergOutputFormat.class);
  private static final String TASK_ATTEMPT_ID_KEY = "mapred.task.id";
  private static final String COMMITTED_EXTENSION = ".committed";

  // <TaskAttemptId, ClosedFileData> map to store the data needed to create DataFiles
  // Stored in concurrent map, since some executor engines can share containers
  private static final Map<String, ClosedFileData> fileData = new ConcurrentHashMap<>();

  private Configuration overlayedConf = null;
  private String taskAttemptId = null;
  private Schema schema = null;
  private String location = null;
  private FileFormat fileFormat = null;

  @Override
  @SuppressWarnings("rawtypes")
  public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath,
                                                           Class valueClass, boolean isCompressed,
                                                           Properties tableAndSerDeProperties,
                                                           Progressable progress)
      throws IOException {
    this.overlayedConf = createOverlayedConf(jc, tableAndSerDeProperties);
    this.taskAttemptId = overlayedConf.get(TASK_ATTEMPT_ID_KEY);
    this.schema = SchemaParser.fromJson(overlayedConf.get(InputFormatConfig.TABLE_SCHEMA));
    this.location = generateTaskLocation(overlayedConf);
    this.fileFormat = FileFormat.valueOf(overlayedConf.get(InputFormatConfig.WRITE_FILE_FORMAT).toUpperCase());
    fileData.remove(this.taskAttemptId);
    return new IcebergRecordWriter();
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
   * Generates file location based on the task configuration.
   * Currently it uses QUERY_LOCATION/taskAttemptId.
   * @param conf The job's configuration
   * @return The file to store the results
   */
  private static String generateTaskLocation(Configuration conf) {
    String taskAttemptId = conf.get(TASK_ATTEMPT_ID_KEY);
    return generateQueryLocation(conf) + "/" + taskAttemptId;
  }

  @Override
  public org.apache.hadoop.mapred.RecordWriter<NullWritable, IcebergWritable> getRecordWriter(
      FileSystem ignored, JobConf job, String name, Progressable progress)
      throws IOException {
    return new IcebergRecordWriter();
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) {
    // Not doing any check.
  }

  protected class IcebergRecordWriter extends org.apache.hadoop.mapreduce.RecordWriter<NullWritable, IcebergWritable>
          implements FileSinkOperator.RecordWriter,
          org.apache.hadoop.mapred.RecordWriter<NullWritable, IcebergWritable> {

    private final FileAppender<Record> appender;
    private final FileIO io;

    IcebergRecordWriter() throws IOException {
      io = new HadoopFileIO(overlayedConf);
      OutputFile dataFile = io.newOutputFile(location);

      switch (fileFormat) {
        case AVRO:
          this.appender = Avro.write(dataFile)
              .schema(schema)
              .createWriterFunc(DataWriter::create)
              .named(fileFormat.name())
              .build();
          break;

        case PARQUET:
          this.appender = Parquet.write(dataFile)
              .schema(schema)
              .createWriterFunc(GenericParquetWriter::buildWriter)
              .named(fileFormat.name())
              .build();
          break;

        case ORC:
          this.appender = ORC.write(dataFile)
              .schema(schema)
              .createWriterFunc(GenericOrcWriter::buildWriter)
              .build();
          break;

        default:
          throw new UnsupportedOperationException("Cannot write format: " + fileFormat);
      }
      LOG.info("IcebergRecordWriter is created in {} with {}", location, fileFormat);
    }

    @Override
    public void write(Writable row) {
      Preconditions.checkArgument(row instanceof IcebergWritable);

      // TODO partition handling
      appender.add(((IcebergWritable) row).record());
    }

    @Override
    public void write(NullWritable key, IcebergWritable value) {
      write(value);
    }

    @Override
    public void close(boolean abort) throws IOException {
      appender.close();
      if (!abort) {
        fileData.put(taskAttemptId, new ClosedFileData(location, fileFormat, appender.length(), appender.metrics()));
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
    public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) {
      return true;
    }

    @Override
    public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {
      ClosedFileData closedFileData = fileData.remove(taskAttemptContext.getTaskAttemptID().toString());
      createCommittedFileFor(new HadoopFileIO(taskAttemptContext.getJobConf()), closedFileData);
    }

    @Override
    public void abortTask(TaskAttemptContext taskAttemptContext) {
      fileData.remove(taskAttemptContext.getTaskAttemptID().toString());
    }

    @Override
    public void commitJob(JobContext jobContext) throws IOException {
      Configuration conf = jobContext.getJobConf();
      Path queryResultPath = new Path(generateQueryLocation(conf));
      Table table = Catalogs.loadTable(conf);

      ExecutorService executor = null;
      try {
        // Creating executor service for parallel handling of file reads and deletes
        executor = Executors.newFixedThreadPool(
            conf.getInt(InputFormatConfig.WRITE_THREAD_POOL_SIZE, InputFormatConfig.WRITE_THREAD_POOL_SIZE_DEFAULT),
            new ThreadFactoryBuilder()
                .setDaemon(false)
                .setPriority(Thread.NORM_PRIORITY)
                .setNameFormat("iceberg-commit-pool-%d")
                .build());

        Set<String> taskTmpFiles = new HashSet<>();
        Set<Future<DataFile>> dataFiles = new HashSet<>();
        FileSystem fs = Util.getFs(queryResultPath, conf);

        // Listing the task result directory and reading .committed files
        RemoteIterator<LocatedFileStatus> taskFileStatuses = fs.listFiles(queryResultPath, false);
        while (taskFileStatuses.hasNext()) {
          LocatedFileStatus taskFile = taskFileStatuses.next();
          String taskFileName = queryResultPath + "/" + taskFile.getPath().getName();
          taskTmpFiles.add(taskFileName);
          if (taskFileName.endsWith(COMMITTED_EXTENSION)) {
            dataFiles.add(executor.submit(() -> {
              LOG.debug("Reading committed file {}", taskFileName);
              ClosedFileData cfd = readCommittedFile(table.io(), taskFileName);
              DataFiles.Builder builder = DataFiles.builder(PartitionSpec.unpartitioned())
                  .withPath(cfd.fileName)
                  .withFormat(cfd.fileFormat)
                  .withFileSizeInBytes(cfd.length)
                  .withMetrics(cfd.serializableMetrics.metrics());
              DataFile dataFile = builder.build();
              return dataFile;
            }));
          }
        }

        // Appending data files to the table
        AppendFiles append = table.newAppend();
        Set<String> filesToKeep = new HashSet<>(dataFiles.size());
        for (Future<DataFile> task : dataFiles) {
          try {
            DataFile dataFile = task.get();
            append.appendFile(dataFile);
            String dataFilePath = dataFile.path().toString();
            filesToKeep.add(dataFilePath);
            taskTmpFiles.remove(dataFilePath);
          } catch (InterruptedException | ExecutionException e) {
            throw new IOException("Committing task is interrupted", e);
          }
        }
        append.commit();
        LOG.info("Iceberg write is committed for {} with files {}", table, filesToKeep);

        // Cleaning up temporary files
        LOG.debug("Removing unused files: {}", taskTmpFiles);
        Collection<Future<?>> deleteTasks = new ArrayList<>(taskTmpFiles.size());
        for (String file : taskTmpFiles) {
          deleteTasks.add(executor.submit(() -> table.io().deleteFile(file)));
        }

        for (Future<?> task : deleteTasks) {
          try {
            task.get();
          } catch (InterruptedException | ExecutionException e) {
            LOG.warn("Deleting files is unsuccessful", e);
          }
        }

        // Calling super to cleanupJob if something more is needed
        cleanupJob(jobContext);

      } finally {
        if (executor != null) {
          executor.shutdown();
        }
      }
    }
  }

  private static void createCommittedFileFor(FileIO io, ClosedFileData closedFileData) throws IOException {
    OutputFile commitFile = io.newOutputFile(closedFileData.fileName + COMMITTED_EXTENSION);
    ObjectOutputStream oos = new ObjectOutputStream(commitFile.createOrOverwrite());
    oos.writeObject(closedFileData);
    oos.close();
    LOG.debug("Iceberg committed file is created {}", commitFile);
  }

  private static ClosedFileData readCommittedFile(FileIO io, String committedFileLocation) throws IOException {
    try (ObjectInputStream ois = new ObjectInputStream(io.newInputFile(committedFileLocation).newStream())) {
      return (ClosedFileData) ois.readObject();
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("Can not parse committed file: " + committedFileLocation, cnfe);
    }
  }

  private static final class ClosedFileData implements Serializable {
    private String fileName;
    private FileFormat fileFormat;
    private Long length;
    private SerializableMetrics serializableMetrics;

    private ClosedFileData(String fileName, FileFormat fileFormat, Long length, Metrics metrics) {
      this.fileName = fileName;
      this.fileFormat = fileFormat;
      this.length = length;
      this.serializableMetrics = new SerializableMetrics(metrics);
    }
  }

  /**
   * We need this class, since Metrics in not Serializable (even though it implements the interface)
   */
  private static final class SerializableMetrics implements Serializable {
    private Long rowCount;
    private Map<Integer, Long> columnSizes;
    private Map<Integer, Long> valueCounts;
    private Map<Integer, Long> nullValueCounts;
    private Map<Integer, byte[]> lowerBounds = null;
    private Map<Integer, byte[]> upperBounds = null;

    private SerializableMetrics(Metrics original) {
      rowCount = original.recordCount();
      columnSizes = original.columnSizes();
      valueCounts = original.valueCounts();
      nullValueCounts = original.nullValueCounts();
      if (original.lowerBounds() != null) {
        lowerBounds = new HashMap<>(original.lowerBounds().size());
        original.lowerBounds().forEach((k, v) -> lowerBounds.put(k, v.array()));
      }
      if (original.upperBounds() != null) {
        upperBounds = new HashMap<>(original.upperBounds().size());
        original.upperBounds().forEach((k, v) -> upperBounds.put(k, v.array()));
      }
    }

    private Metrics metrics() {
      final Map<Integer, ByteBuffer> metricsLowerBounds =
          lowerBounds != null ? new HashMap<>(lowerBounds.size()) : null;
      final Map<Integer, ByteBuffer> metricsUpperBounds =
          lowerBounds != null ? new HashMap<>(upperBounds.size()) : null;

      if (lowerBounds != null) {
        lowerBounds.forEach((k, v) -> metricsLowerBounds.put(k, ByteBuffer.wrap(v)));
      }
      if (upperBounds != null) {
        upperBounds.forEach((k, v) -> metricsUpperBounds.put(k, ByteBuffer.wrap(v)));
      }

      return new Metrics(rowCount, columnSizes, valueCounts, nullValueCounts, metricsLowerBounds, metricsUpperBounds);
    }
  }
}

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
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.HiveSerDeConfig;
import org.apache.iceberg.mr.mapreduce.IcebergWritable;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class HiveIcebergOutputFormat extends OutputFormat<NullWritable, IcebergWritable>
    implements HiveOutputFormat<NullWritable, IcebergWritable> {
  private Configuration overlayedConf = null;
  private Table table = null;

  @Override
  @SuppressWarnings("rawtypes")
  public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath,
                                                           Class valueClass, boolean isCompressed,
                                                           Properties tableAndSerDeProperties,
                                                           Progressable progress)
      throws IOException {
    this.overlayedConf = createOverlayedConf(jc, tableAndSerDeProperties);
    this.table = Catalogs.loadTable(this.overlayedConf, tableAndSerDeProperties);
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

  @Override
  public org.apache.hadoop.mapred.RecordWriter<NullWritable, IcebergWritable> getRecordWriter(
      FileSystem ignored, JobConf job, String name, Progressable progress)
      throws IOException {
    return new IcebergRecordWriter();
  }

  @Override
  public org.apache.hadoop.mapreduce.RecordWriter<NullWritable, IcebergWritable> getRecordWriter(
      TaskAttemptContext context)
      throws IOException {
    return new IcebergRecordWriter();
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) {
    // Not doing any check.
  }

  @Override
  public void checkOutputSpecs(JobContext context) {
    // Not doing any check.
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
    return new IcebergOutputCommitter();
  }

  protected class IcebergRecordWriter extends org.apache.hadoop.mapreduce.RecordWriter<NullWritable, IcebergWritable>
          implements FileSinkOperator.RecordWriter,
          org.apache.hadoop.mapred.RecordWriter<NullWritable, IcebergWritable> {

    private final FileAppender<Record> appender;
    private final String location;
    private final FileFormat fileFormat;

    IcebergRecordWriter() throws IOException {
      this.fileFormat = FileFormat.valueOf(overlayedConf.get(HiveSerDeConfig.WRITE_FILE_FORMAT).toUpperCase());

      String queryId = overlayedConf.get(HiveConf.ConfVars.HIVEQUERYID.varname);
      this.location = table.location() + "/" + queryId + UUID.randomUUID();

      FileIO io = new HadoopFileIO(overlayedConf);
      OutputFile dataFile = io.newOutputFile(location);

      switch (fileFormat) {
        case AVRO:
          this.appender = Avro.write(dataFile)
              .schema(table.schema())
              .createWriterFunc(DataWriter::create)
              .named(fileFormat.name())
              .build();
          break;

        case PARQUET:
          this.appender = Parquet.write(dataFile)
              .schema(table.schema())
              .createWriterFunc(GenericParquetWriter::buildWriter)
              .named(fileFormat.name())
              .build();
          break;

        case ORC:
          this.appender = ORC.write(dataFile)
              .schema(table.schema())
              .createWriterFunc(GenericOrcWriter::buildWriter)
              .build();
          break;

        default:
          throw new UnsupportedOperationException("Cannot write format: " + fileFormat);
      }
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

      DataFiles.Builder builder = DataFiles.builder(table.spec())
          .withPath(location)
          .withFormat(fileFormat)
          .withFileSizeInBytes(appender.length())
          .withMetrics(appender.metrics());

      AppendFiles append = table.newAppend();
      append = append.appendFile(builder.build());
      append.commit();
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException {
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
  private static final class IcebergOutputCommitter extends OutputCommitter {

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
      return false;
    }

    @Override
    public void commitTask(TaskAttemptContext taskAttemptContext) {
      // do nothing.
    }

    @Override
    public void abortTask(TaskAttemptContext taskAttemptContext) {
      // do nothing.
    }
  }
}

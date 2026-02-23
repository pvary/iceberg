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
package org.apache.iceberg.spark.source;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.ColumnUpdate;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.DataWriteResult;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.RollingDataWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkWriteConf;
import org.apache.iceberg.spark.SparkWriteRequirements;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DataFileSet;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.write.DeltaBatchWrite;
import org.apache.spark.sql.connector.write.DeltaWrite;
import org.apache.spark.sql.connector.write.DeltaWriter;
import org.apache.spark.sql.connector.write.DeltaWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering;
import org.apache.spark.sql.connector.write.WriteSummary;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkColumnUpdateWrite extends BaseSparkWrite
    implements DeltaWrite, RequiresDistributionAndOrdering {
  private static final Logger LOG = LoggerFactory.getLogger(SparkColumnUpdateWrite.class);

  private final JavaSparkContext sparkContext;
  private final SparkCopyOnWriteScan scan;
  private final SparkWriteConf writeConf;
  private final Table table;
  private final String queryId;
  private final FileFormat format;
  private final String applicationId;
  private final boolean wapEnabled;
  private final String wapId;
  private final int outputSpecId;
  private final String branch;
  private final long targetFileSize;
  private final Schema readSchema;
  private final StructType dsSchema;
  private final Map<String, String> extraSnapshotMetadata;
  private final SparkWriteRequirements writeRequirements;
  private final Map<String, String> writeProperties;
  private final List<Integer> updatedFieldIds;

  private boolean cleanupOnAbort = false;

  SparkColumnUpdateWrite(
      SparkSession spark,
      SparkCopyOnWriteScan scan,
      Table table,
      SparkWriteConf writeConf,
      LogicalWriteInfo writeInfo,
      String applicationId,
      Schema readSchema,
      StructType dsSchema,
      SparkWriteRequirements writeRequirements,
      List<Integer> updatedFieldIds) {
    this.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    this.scan = scan;
    this.table = table;
    this.writeConf = writeConf;
    this.queryId = writeInfo.queryId();
    this.format = writeConf.dataFileFormat();
    this.applicationId = applicationId;
    this.wapEnabled = writeConf.wapEnabled();
    this.wapId = writeConf.wapId();
    this.branch = writeConf.branch();
    this.targetFileSize = writeConf.targetDataFileSize();
    this.readSchema = readSchema;
    this.dsSchema = dsSchema;
    this.extraSnapshotMetadata = writeConf.extraSnapshotMetadata();
    this.writeRequirements = writeRequirements;
    this.outputSpecId = writeConf.outputSpecId();
    this.writeProperties = writeConf.writeProperties();
    this.updatedFieldIds = updatedFieldIds;
  }

  private static final NamedReference FILE_PATH_REF =
      Expressions.column(MetadataColumns.FILE_PATH.name());
  private static final NamedReference ROW_POSITION_REF =
      Expressions.column(MetadataColumns.ROW_POSITION.name());

  @Override
  public Distribution requiredDistribution() {
    // Cluster by FILE_PATH to ensure all rows for the same base file go to the same partition
    Distribution distribution = Distributions.clustered(new NamedReference[] {FILE_PATH_REF});
    LOG.debug("Requesting {} as write distribution for table {}", distribution, table.name());
    return distribution;
  }

  // TODO gaborkaszab: for sparse updates also sort by _pos
  @Override
  public SortOrder[] requiredOrdering() {
    // Sort by FILE_PATH and ROW_POSITION to ensure rows for the same base file are contiguous
    // and in order
    SortOrder[] ordering =
        new SortOrder[] {
          Expressions.sort(FILE_PATH_REF, SortDirection.ASCENDING),
          Expressions.sort(ROW_POSITION_REF, SortDirection.ASCENDING)
        };
    LOG.debug("Requesting {} as write ordering for table {}", ordering, table.name());
    return ordering;
  }

  private DataFileSet baseFiles() {
    if (scan == null) {
      return DataFileSet.create();
    } else {
      return scan.tasks().stream()
          .map(FileScanTask::file)
          .collect(Collectors.toCollection(DataFileSet::create));
    }
  }

  private Map<String, Long> baseFileRowCounts() {
    if (scan == null) {
      return Maps.newHashMap();
    } else {
      return scan.tasks().stream()
          .map(FileScanTask::file)
          .collect(Collectors.toMap(file -> file.location(), file -> file.recordCount()));
    }
  }

  private Map<String, DataFile> writtenFiles(WriterCommitMessage[] messages) {
    Map<String, DataFile> result = Maps.newHashMap();

    for (WriterCommitMessage message : messages) {
      if (message != null) {
        TaskCommit taskCommit = (TaskCommit) message;
        for (Map.Entry<String, DataFile> entry : taskCommit.updateFilesByBasePath().entrySet()) {
          Preconditions.checkState(
              !result.containsKey(entry.getKey()),
              "Duplicate update file for base file: %s",
              entry.getKey());
          result.put(entry.getKey(), entry.getValue());
        }
      }
    }

    return result;
  }

  @Override
  public DeltaBatchWrite toBatch() {
    return new ColumnUpdateOperation(scan, updatedFieldIds);
  }

  private class ColumnUpdateOperation implements DeltaBatchWrite {
    private final SparkCopyOnWriteScan scan;
    private final List<Integer> updatedFieldIds;

    private ColumnUpdateOperation(SparkCopyOnWriteScan scan, List<Integer> updatedFieldIds) {
      this.scan = scan;
      this.updatedFieldIds = updatedFieldIds;
    }

    @Override
    public DeltaWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
      // broadcast the table metadata as the writer factory will be sent to executors
      Broadcast<Table> tableBroadcast =
          sparkContext.broadcast(SerializableTableWithSize.copyOf(table));
      // TODO gaborkaszab: This is broadcasted to every executors. Can't we send it clustered by
      // file path?
      Broadcast<Map<String, Long>> baseFileRowCountsBroadcast =
          sparkContext.broadcast(baseFileRowCounts());
      return new ColumnUpdateWriteFactory(
          tableBroadcast,
          baseFileRowCountsBroadcast,
          queryId,
          format,
          outputSpecId,
          targetFileSize,
          readSchema,
          dsSchema,
          writeProperties);
    }

    @Override
    public boolean useCommitCoordinator() {
      return false;
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
      // TODO gaborkaszab: implement
    }

    @Override
    public String toString() {
      return String.format("SparkColumnUpdateWrite(table=%s, format=%s)", table, format);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
      commit(messages, null);
    }

    @Override
    public void commit(WriterCommitMessage[] messages, WriteSummary summary) {
      DataFileSet baseFiles = baseFiles();

      // Create a mapping from base file path to base file for quick lookup
      Map<String, DataFile> baseFilesByPath =
          baseFiles.stream()
              .collect(Collectors.toMap(file -> file.location(), Function.identity()));

      Map<String, DataFile> writtenFiles = writtenFiles(messages);

      for (String basePath : baseFilesByPath.keySet()) {
        Preconditions.checkState(
            writtenFiles.containsKey(basePath), "Missing update file for base file: %s", basePath);
      }

      // Create mapping from base file to update file
      Map<DataFile, DataFile> baseToUpdateFile =
          writtenFiles.entrySet().stream()
              .collect(
                  Collectors.toMap(
                      entry -> baseFilesByPath.get(entry.getKey()), Map.Entry::getValue));

      // TODO gaborkaszab: conflict detection with isolationLevel similar to CoWOperation?

      ColumnUpdate columnUpdate = table.newColumnUpdate().withFieldIds(updatedFieldIds);
      baseToUpdateFile.forEach(columnUpdate::addColumnUpdate);
      columnUpdate.commit();
    }
  }

  public static class TaskCommit implements WriterCommitMessage {
    private final Map<String, DataFile> updateFilesByBasePath;

    TaskCommit(Map<String, DataFile> result) {
      this.updateFilesByBasePath = Maps.newHashMap();
      this.updateFilesByBasePath.putAll(result);
    }

    Map<String, DataFile> updateFilesByBasePath() {
      return updateFilesByBasePath;
    }
  }

  private static class ColumnUpdateWriteFactory implements DeltaWriterFactory {
    private final Broadcast<Table> tableBroadcast;
    private final Broadcast<Map<String, Long>> baseFileRowCountsBroadcast;
    private final FileFormat format;
    private final int outputSpecId;
    private final long targetFileSize;
    private final Schema readSchema;
    private final StructType dsSchema;
    private final String queryId;
    private final Map<String, String> writeProperties;

    protected ColumnUpdateWriteFactory(
        Broadcast<Table> tableBroadcast,
        Broadcast<Map<String, Long>> baseFileRowCountsBroadcast,
        String queryId,
        FileFormat format,
        int outputSpecId,
        long targetFileSize,
        Schema readSchema,
        StructType dsSchema,
        Map<String, String> writeProperties) {
      this.tableBroadcast = tableBroadcast;
      this.baseFileRowCountsBroadcast = baseFileRowCountsBroadcast;
      this.format = format;
      this.outputSpecId = outputSpecId;
      this.targetFileSize = targetFileSize;
      this.readSchema = readSchema;
      this.dsSchema = dsSchema;
      this.queryId = queryId;
      this.writeProperties = writeProperties;
    }

    @Override
    public DeltaWriter<InternalRow> createWriter(int partitionId, long taskId) {
      Table table = tableBroadcast.value();
      PartitionSpec spec = table.specs().get(outputSpecId);
      FileIO io = table.io();

      OutputFileFactory fileFactory =
          OutputFileFactory.builderFor(table, partitionId, taskId)
              .format(format)
              .operationId(queryId)
              .suffix("update")
              .build();

      SparkFileWriterFactory writerFactory =
          SparkFileWriterFactory.builderFor(table)
              .dataFileFormat(format)
              .dataSchema(readSchema)
              .dataSparkType(dsSchema)
              .writeProperties(writeProperties)
              .build();

      return new ColumnUpdateDataWriter(
          writerFactory,
          fileFactory,
          io,
          spec,
          Partitioning.partitionType(table),
          targetFileSize,
          baseFileRowCountsBroadcast.value());
    }
  }

  private static class ColumnUpdateDataWriter implements DeltaWriter<InternalRow> {
    private static final Integer FILE_PATH_ORDINAL_IN_ID = 0;
    private static final Integer POSITION_ORDINAL_IN_ID = 1;
    private static final Integer PARTITION_ORDINAL_IN_METADATA = 0;

    private final SparkFileWriterFactory writerFactory;
    private final OutputFileFactory fileFactory;
    private final FileIO io;
    private final PartitionSpec spec;
    private final long targetFileSizeInBytes;
    private final InternalRowWrapper partitionRowWrapper;
    private final Map<String, Long> baseFileRowCounts;

    private RollingDataWriter<InternalRow> currentWriter;
    private String currentFilePath;
    private Long previousPosition = -1L;
    private boolean closed;
    private int numFields = -1;

    private final Map<String, DataFile> updateFilesByBasePath;

    private ColumnUpdateDataWriter(
        SparkFileWriterFactory writerFactory,
        OutputFileFactory fileFactory,
        FileIO io,
        PartitionSpec spec,
        Types.StructType partitionType,
        long targetFileSize,
        Map<String, Long> baseFileRowCounts) {
      this.writerFactory = writerFactory;
      this.fileFactory = fileFactory;
      this.io = io;
      this.spec = spec;
      this.targetFileSizeInBytes = targetFileSize;
      this.updateFilesByBasePath = Maps.newHashMap();
      this.closed = false;
      this.baseFileRowCounts = baseFileRowCounts;

      if (spec.isPartitioned()) {
        StructType sparkPartitionType = (StructType) SparkSchemaUtil.convert(partitionType);
        this.partitionRowWrapper = new InternalRowWrapper(sparkPartitionType, partitionType);
      } else {
        this.partitionRowWrapper = null;
      }
    }

    @Override
    public void delete(InternalRow metadata, InternalRow id) throws IOException {
      throw new UnsupportedOperationException(
          this.getClass().getName() + " does not implement delete");
    }

    @Override
    public void update(InternalRow metadata, InternalRow id, InternalRow row) throws IOException {
      String filePath = id.getString(FILE_PATH_ORDINAL_IN_ID);
      Preconditions.checkState(filePath != null, "File path is null");

      if (!filePath.equals(currentFilePath)) {
        openWriter(filePath, metadata);
      }

      if (numFields < 0) {
        numFields = row.numFields();
      }

      Long position = id.getLong(POSITION_ORDINAL_IN_ID);
      Preconditions.checkState(
          previousPosition <= position,
          "Current positions is smaller than last position: %s < %s",
          position,
          previousPosition);
      while (previousPosition < position - 1) {
        currentWriter.write(new GenericInternalRow(row.numFields()));
        ++previousPosition;
      }
      previousPosition = position;

      currentWriter.write(row);
    }

    @Override
    public void insert(InternalRow row) throws IOException {
      throw new UnsupportedOperationException(
          this.getClass().getName() + " does not implement insert");
    }

    private void openWriter(String filePath, InternalRow metadata) throws IOException {
      closeCurrentWriter();

      StructLike partition = null;
      if (spec.isPartitioned()) {
        InternalRow partitionRow =
            metadata.getStruct(PARTITION_ORDINAL_IN_METADATA, partitionRowWrapper.size());
        partition = partitionRowWrapper.wrap(partitionRow);
      }

      // TODO gaborkaszab: DataWriter instead of RollingDataWriter? In this case there is no need to
      // roll.
      currentWriter =
          new RollingDataWriter<>(
              writerFactory, fileFactory, io, targetFileSizeInBytes, spec, partition);
      currentFilePath = filePath;
    }

    private void closeCurrentWriter() throws IOException {
      if (currentWriter != null) {
        // Write trailing null rows for missing positions at the end of the file
        Long baseFileRowCount = baseFileRowCounts.get(currentFilePath);
        Preconditions.checkState(
            baseFileRowCount != null, "Unable to find row count for base file " + currentFilePath);
        while (previousPosition < baseFileRowCount - 1) {
          currentWriter.write(new GenericInternalRow(numFields));
          ++previousPosition;
        }

        currentWriter.close();

        DataWriteResult result = currentWriter.result();
        Preconditions.checkState(
            !result.dataFiles().isEmpty(),
            "Unable to create update file for base file " + currentFilePath);
        Preconditions.checkState(
            result.dataFiles().size() == 1,
            "Multiple update files created for base file " + currentFilePath);
        Preconditions.checkState(
            !updateFilesByBasePath.containsKey(currentFilePath),
            "Multiple update files created for base file " + currentFilePath);

        updateFilesByBasePath.put(currentFilePath, result.dataFiles().getFirst());

        this.currentWriter = null;
        this.currentFilePath = null;
        this.previousPosition = -1L;
      }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      close();

      return new TaskCommit(updateFilesByBasePath);
    }

    @Override
    public void abort() throws IOException {
      close();

      for (DataFile file : updateFilesByBasePath.values()) {
        io.deleteFile(file.location());
      }
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        closeCurrentWriter();
        closed = true;
      }
    }
  }
}

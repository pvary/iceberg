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
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.source.metrics.TaskNumDeletes;
import org.apache.iceberg.spark.source.metrics.TaskNumSplits;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.rdd.InputFileBlockHolder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RowDataReader extends BaseRowReader<FileScanTask> implements PartitionReader<InternalRow> {
  private static final Logger LOG = LoggerFactory.getLogger(RowDataReader.class);

  private final long numSplits;

  RowDataReader(SparkInputPartition partition) {
    this(
        partition.table(),
        partition.taskGroup(),
        SnapshotUtil.schemaFor(partition.table(), partition.branch()),
        partition.expectedSchema(),
        partition.isCaseSensitive(),
        partition.cacheDeleteFilesOnExecutors());
  }

  RowDataReader(
      Table table,
      ScanTaskGroup<FileScanTask> taskGroup,
      Schema tableSchema,
      Schema expectedSchema,
      boolean caseSensitive,
      boolean cacheDeleteFilesOnExecutors) {

    super(
        table, taskGroup, tableSchema, expectedSchema, caseSensitive, cacheDeleteFilesOnExecutors);

    numSplits = taskGroup.tasks().size();
    LOG.debug("Reading {} file split(s) for table {}", numSplits, table.name());
  }

  @Override
  public CustomTaskMetric[] currentMetricsValues() {
    return new CustomTaskMetric[] {
      new TaskNumSplits(numSplits), new TaskNumDeletes(counter().get())
    };
  }

  @Override
  protected Stream<ContentFile<?>> referencedFiles(FileScanTask task) {
    return Stream.concat(Stream.of(task.file()), task.deletes().stream());
  }

  @Override
  protected CloseableIterator<InternalRow> open(FileScanTask task) {
    if (task.immediateDataFileRead()) {
      return openWithImmediateDataFileRead(task);
    }

    String filePath = task.file().location();
    LOG.debug("Opening data file {}", filePath);
    SparkDeleteFilter deleteFilter =
        new SparkDeleteFilter(filePath, task.deletes(), counter(), true);

    // schema or rows returned by readers
    Schema requiredSchema = deleteFilter.requiredSchema();
    Map<Integer, ?> idToConstant = constantsMap(task, requiredSchema);

    // update the current file for Spark's filename() function
    InputFileBlockHolder.set(filePath, task.start(), task.length());

    return deleteFilter.filter(open(task, requiredSchema, idToConstant)).iterator();
  }

  /**
   * Reads the rows referenced by an index instead of scanning the task's own data file.
   *
   * <p>The task's file is an index file that records, for each indexed row, the location of the
   * original data file ({@link MetadataColumns#INDEX_FILE_PATH}) and the row position within it
   * ({@link MetadataColumns#INDEX_ROW_POSITION}). Those references are resolved by reading the
   * original data files and returning the projected rows at the recorded positions.
   */
  private CloseableIterator<InternalRow> openWithImmediateDataFileRead(FileScanTask task) {
    Map<String, Set<Long>> positionsByDataFile = readIndexPositions(task);
    return readDataFileRows(task, positionsByDataFile);
  }

  /**
   * Reads the index file referenced by the task and returns, for each original data file, the set
   * of row positions that must be read.
   *
   * <p>The residual is applied explicitly on the index rows so filtering is exact and independent
   * of the index file format, which may not push the residual down.
   */
  private Map<String, Set<Long>> readIndexPositions(FileScanTask task) {
    String indexFilePath = task.file().location();
    LOG.debug("Opening index file {} for immediate data file read", indexFilePath);
    InputFile indexFile = getInputFile(indexFilePath);
    Preconditions.checkNotNull(indexFile, "Could not find InputFile associated with index file");

    // the index stores the columns referenced by the filter, so the residual is pushed down to the
    // index read to skip rows before the referenced data files are read
    Schema indexSchema = indexSchema(task.residual());
    int fileLocationOrdinal = fieldOrdinal(indexSchema, MetadataColumns.INDEX_FILE_PATH.fieldId());
    int rowPositionOrdinal =
        fieldOrdinal(indexSchema, MetadataColumns.INDEX_ROW_POSITION.fieldId());

    Evaluator residual = new Evaluator(indexSchema.asStruct(), task.residual(), caseSensitive());
    InternalRowWrapper indexRowWrapper =
        new InternalRowWrapper(SparkSchemaUtil.convert(indexSchema), indexSchema.asStruct());

    Map<String, Set<Long>> positionsByDataFile = Maps.newLinkedHashMap();
    try (CloseableIterable<InternalRow> indexRows =
        newIterable(
            indexFile,
            task.file().format(),
            task.start(),
            task.length(),
            // the residual is pushed down as a best-effort pre-filter; the Evaluator below is the
            // authoritative, format-independent filter applied to every index row
            task.residual(),
            indexSchema,
            ImmutableMap.of())) {
      for (InternalRow indexRow : indexRows) {
        if (!residual.eval(indexRowWrapper.wrap(indexRow))) {
          continue;
        }

        String dataFileLocation = indexRow.getUTF8String(fileLocationOrdinal).toString();
        long rowPosition = indexRow.getLong(rowPositionOrdinal);
        positionsByDataFile
            .computeIfAbsent(dataFileLocation, ignored -> Sets.newHashSet())
            .add(rowPosition);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read index file: " + indexFilePath, e);
    }

    return positionsByDataFile;
  }

  /**
   * Reads the referenced rows from the original data files, returning only the projected rows at
   * the positions recorded in the index.
   */
  private CloseableIterator<InternalRow> readDataFileRows(
      FileScanTask task, Map<String, Set<Long>> positionsByDataFile) {
    Schema readSchema = schemaWithRowPosition(expectedSchema());
    int positionOrdinal = fieldOrdinal(readSchema, MetadataColumns.ROW_POSITION.fieldId());

    List<CloseableIterable<InternalRow>> dataFileRows =
        Lists.newArrayListWithExpectedSize(positionsByDataFile.size());
    for (Map.Entry<String, Set<Long>> entry : positionsByDataFile.entrySet()) {
      String dataFileLocation = entry.getKey();
      Set<Long> positions = entry.getValue();
      InputFile dataFile = table().io().newInputFile(dataFileLocation);
      FileFormat format = FileFormat.fromFileName(dataFileLocation);
      Preconditions.checkArgument(
          format != null, "Cannot determine file format for %s", dataFileLocation);
      LOG.debug(
          "Immediately reading {} row(s) from data file {}", positions.size(), dataFileLocation);
      CloseableIterable<InternalRow> rows =
          newIterable(
              dataFile,
              format,
              0,
              dataFile.getLength(),
              task.residual(),
              readSchema,
              ImmutableMap.of());
      dataFileRows.add(new RowsAtPositions(rows, positions, positionOrdinal));
    }

    return CloseableIterable.concat(dataFileRows).iterator();
  }

  /**
   * A {@link CloseableIterable} over the rows at the given positions, stopping the underlying read
   * once the last requested position has been emitted.
   *
   * <p>Rows within a data file are read in ascending {@link MetadataColumns#ROW_POSITION} order, so
   * the scan can be short-circuited as soon as all requested positions have been returned instead
   * of reading the remainder of the file.
   */
  private static final class RowsAtPositions implements CloseableIterable<InternalRow> {
    private final CloseableIterable<InternalRow> rows;
    private final Set<Long> positions;
    private final int positionOrdinal;

    RowsAtPositions(CloseableIterable<InternalRow> rows, Set<Long> positions, int positionOrdinal) {
      this.rows = rows;
      this.positions = positions;
      this.positionOrdinal = positionOrdinal;
    }

    @Override
    public CloseableIterator<InternalRow> iterator() {
      return new PositionIterator(rows.iterator());
    }

    @Override
    public void close() throws IOException {
      rows.close();
    }

    private final class PositionIterator implements CloseableIterator<InternalRow> {
      private final CloseableIterator<InternalRow> underlying;
      private InternalRow next;
      private int remaining = positions.size();
      private boolean done = false;

      PositionIterator(CloseableIterator<InternalRow> underlying) {
        this.underlying = underlying;
      }

      @Override
      public boolean hasNext() {
        if (next != null) {
          return true;
        }

        if (done) {
          return false;
        }

        while (underlying.hasNext()) {
          InternalRow row = underlying.next();
          if (positions.contains(row.getLong(positionOrdinal))) {
            next = row;
            remaining -= 1;
            if (remaining == 0) {
              done = true;
            }

            return true;
          }
        }

        done = true;
        return false;
      }

      @Override
      public InternalRow next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        InternalRow result = next;
        next = null;
        return result;
      }

      @Override
      public void close() throws IOException {
        underlying.close();
      }
    }
  }

  private Schema indexSchema(Expression residual) {
    Set<Integer> filterIds =
        Binder.boundReferences(
            table().schema().asStruct(), ImmutableList.of(residual), caseSensitive());
    List<NestedField> columns = Lists.newArrayList();
    for (int filterId : filterIds) {
      columns.add(table().schema().findField(filterId));
    }

    columns.add(MetadataColumns.INDEX_FILE_PATH);
    columns.add(MetadataColumns.INDEX_ROW_POSITION);
    return new Schema(columns);
  }

  private static Schema schemaWithRowPosition(Schema schema) {
    if (schema.findField(MetadataColumns.ROW_POSITION.fieldId()) != null) {
      return schema;
    }

    List<NestedField> columns = Lists.newArrayList(schema.columns());
    columns.add(MetadataColumns.ROW_POSITION);
    return new Schema(columns);
  }

  private static int fieldOrdinal(Schema schema, int fieldId) {
    List<NestedField> columns = schema.columns();
    for (int ordinal = 0; ordinal < columns.size(); ordinal++) {
      if (columns.get(ordinal).fieldId() == fieldId) {
        return ordinal;
      }
    }

    throw new IllegalStateException("Schema does not contain field " + fieldId);
  }

  protected CloseableIterable<InternalRow> open(
      FileScanTask task, Schema readSchema, Map<Integer, ?> idToConstant) {
    if (task.isDataTask()) {
      return newDataIterable(task.asDataTask(), readSchema);
    } else {
      InputFile inputFile = getInputFile(task.file().location());
      Preconditions.checkNotNull(
          inputFile, "Could not find InputFile associated with FileScanTask");
      return newIterable(
          inputFile,
          task.file().format(),
          task.start(),
          task.length(),
          task.residual(),
          readSchema,
          idToConstant);
    }
  }

  private CloseableIterable<InternalRow> newDataIterable(DataTask task, Schema readSchema) {
    StructInternalRow row = new StructInternalRow(readSchema.asStruct());
    return CloseableIterable.transform(task.asDataTask().rows(), row::setStruct);
  }
}

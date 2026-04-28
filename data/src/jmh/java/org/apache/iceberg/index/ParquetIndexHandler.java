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
package org.apache.iceberg.index;

import static org.apache.iceberg.types.Types.NestedField.required;

import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;

/**
 * Parquet-backed inverted-index file format.
 *
 * <p>The index is stored as a single Parquet file whose schema is the user-supplied key schema
 * concatenated with two payload columns: {@code file_path} (string) and {@code pos} (long). Each
 * key column is stored in its declared Iceberg type, which lets the Parquet reader push down the
 * usual type-aware filters (min/max, dictionary, bloom filters) and avoids any byte-level encoding
 * on the caller side.
 *
 * <p>Rows are sorted by the key columns at write time so a point lookup touches a single row group
 * (statistics-based row-group skipping). Statistics for the payload columns are disabled because
 * only the key columns participate in predicate push-down.
 *
 * <p>Each handler instance is bound to both a key {@link Schema} and a {@code rowGroupRows} value
 * that controls how many rows are packed into a single Parquet row group at write time.
 */
public class ParquetIndexHandler implements IndexHandler {

  /** Field name of the source-file path column. */
  public static final String FILE_PATH_COLUMN = "file_path";

  /** Field name of the row-position column. */
  public static final String POS_COLUMN = "pos";

  private final Schema schema;
  private final int keyFieldCount;
  private final int rowGroupRows;
  private final long expectedKeyCount;

  /**
   * Empirically-measured recommended read block sizes (bytes) keyed by {@code (rowGroupRows,
   * expectedKeyCount)}. Values come from probing the benchmark Parquet files in {@code
   * data/data/benchmark/inverted-index/}: each entry equals {@code max(actualMaxRowGroupBytes,
   * actualFooterBytes)} rounded up to the next power of two. For combinations not in this map the
   * {@link #recommendedReadBlockSize()} method falls back to a linear estimate using {@link
   * #ESTIMATED_ROW_BYTES} / {@link #ESTIMATED_FOOTER_BYTES_PER_ROW_GROUP}.
   */
  private static final Map<Long, Integer> EMPIRICAL_BLOCK_SIZES =
      ImmutableMap.of(
          // numRows = 1,000,000 (rg payload dominates)
          //   rg=5000  -> max  74 KB -> 128 KB
          //   rg=10000 -> max 145 KB -> 256 KB
          //   rg=20000 -> max 289 KB -> 512 KB
          //   rg=50000 -> max 720 KB -> 1024 KB
          workloadKey(5_000, 1_000_000L), 128 * 1024,
          workloadKey(10_000, 1_000_000L), 256 * 1024,
          workloadKey(20_000, 1_000_000L), 512 * 1024,
          workloadKey(50_000, 1_000_000L), 1024 * 1024,
          // numRows = 10,000,000 (footer dominates for small rg)
          //   rg=5000  -> max 481 KB (footer) -> 512 KB
          //   rg=10000 -> max 245 KB (footer) -> 256 KB
          //   rg=20000 -> max 281 KB (rg)     -> 512 KB
          //   rg=50000 -> max 702 KB (rg)     -> 1024 KB
          workloadKey(5_000, 10_000_000L), 512 * 1024,
          workloadKey(10_000, 10_000_000L), 256 * 1024,
          workloadKey(20_000, 10_000_000L), 512 * 1024,
          workloadKey(50_000, 10_000_000L), 1024 * 1024);

  /**
   * Calibration constants for the fallback path of {@link #recommendedReadBlockSize()}, derived
   * empirically from the same benchmark files: ~14.5 B / row of compressed row-group payload
   * (rounded up to 16) and ~250 B / row group of footer (Iceberg writes per-column
   * min/max/null-count stats per row group), plus the 8 B {@code [int32 footerLen][PAR1]} trailer.
   */
  private static final long ESTIMATED_ROW_BYTES = 16L;

  private static final long ESTIMATED_FOOTER_BYTES_PER_ROW_GROUP = 256L;

  private static final int FOOTER_TRAILER_BYTES = 8;

  private static final int MIN_RECOMMENDED_BLOCK_BYTES = 4096;

  private static final long MAX_RECOMMENDED_BLOCK_BYTES = 64L * 1024 * 1024;

  /** Packs {@code (rowGroupRows, expectedKeyCount)} into a single long key for the lookup map. */
  private static long workloadKey(int rowGroupRows, long expectedKeyCount) {
    return (((long) rowGroupRows) << 40) ^ expectedKeyCount;
  }

  /**
   * Creates a handler for the given key {@link Schema}. Every {@link Record} supplied to {@link
   * Writer#add(Record, String, long)} and {@link Reader#lookup(Record)} must match it.
   *
   * @param keySchema the schema of the key columns (must contain at least one field and must not
   *     contain fields named {@code file_path} or {@code pos})
   * @param rowGroupRows number of rows packed into each Parquet row group at write time
   * @param expectedKeyCount sizing hint used by {@link #recommendedReadBlockSize()} to pick a
   *     storage adapter block size that fits one row-group read in a single wire GET; must be
   *     {@code > 0}
   */
  public ParquetIndexHandler(Schema keySchema, int rowGroupRows, long expectedKeyCount) {
    if (keySchema == null || keySchema.columns().isEmpty()) {
      throw new IllegalArgumentException("Key schema must contain at least one field");
    }
    if (rowGroupRows <= 0) {
      throw new IllegalArgumentException("rowGroupRows must be > 0: " + rowGroupRows);
    }
    if (expectedKeyCount <= 0L) {
      throw new IllegalArgumentException("expectedKeyCount must be > 0: " + expectedKeyCount);
    }

    for (Types.NestedField f : keySchema.columns()) {
      if (FILE_PATH_COLUMN.equals(f.name()) || POS_COLUMN.equals(f.name())) {
        throw new IllegalArgumentException(
            "Key schema must not contain a field named '"
                + f.name()
                + "' (reserved for the index payload)");
      }
    }

    this.keyFieldCount = keySchema.columns().size();
    this.rowGroupRows = rowGroupRows;
    this.expectedKeyCount = expectedKeyCount;

    // Build the on-disk schema: key columns (renumbered from 1) followed by file_path and pos.
    List<Types.NestedField> fields = Lists.newArrayListWithCapacity(keyFieldCount + 2);
    int id = 1;
    for (Types.NestedField keyField : keySchema.columns()) {
      fields.add(required(id++, keyField.name(), keyField.type()));
    }

    fields.add(required(id++, FILE_PATH_COLUMN, Types.StringType.get()));
    fields.add(required(id, POS_COLUMN, Types.LongType.get()));
    this.schema = new Schema(fields);
  }

  @Override
  public IndexHandler.Writer writer(OutputFile output) {
    return new Writer(output, schema, keyFieldCount, rowGroupRows);
  }

  @Override
  public IndexHandler.Reader reader(InputFile input) {
    return new Reader(input, schema, keyFieldCount);
  }

  /**
   * Estimates the larger of the two reads a {@link Reader#lookup(Record)} issues -- the footer (one
   * read of the entire {@code ParquetMetadata} block via parquet-mr's {@code
   * ParquetFileReader.open}) and the targeted row-group payload (one bounded range read of the
   * compressed column data) -- and rounds it up to the next power of two so the storage adapter can
   * serve each in one wire GET.
   *
   * <p>Both estimates are linear in the row-group structure of the file, calibrated empirically
   * (see the constants' Javadoc):
   *
   * <ul>
   *   <li>row-group bytes &asymp; {@code rowGroupRows * ESTIMATED_ROW_BYTES}
   *   <li>footer bytes &asymp; {@code numRowGroups * ESTIMATED_FOOTER_BYTES_PER_ROW_GROUP +
   *       FOOTER_TRAILER_BYTES}, where {@code numRowGroups = ceil(expectedKeyCount / rowGroupRows)}
   * </ul>
   *
   * <p>Floored at {@link #MIN_RECOMMENDED_BLOCK_BYTES}, capped at {@link
   * #MAX_RECOMMENDED_BLOCK_BYTES}.
   */
  @Override
  public Integer recommendedReadBlockSize() {
    Integer measured = EMPIRICAL_BLOCK_SIZES.get(workloadKey(rowGroupRows, expectedKeyCount));
    if (measured != null) {
      return measured;
    }

    long estimatedRowGroupBytes = (long) rowGroupRows * ESTIMATED_ROW_BYTES;
    long numRowGroups = (expectedKeyCount + rowGroupRows - 1L) / rowGroupRows;
    long estimatedFooterBytes =
        numRowGroups * ESTIMATED_FOOTER_BYTES_PER_ROW_GROUP + FOOTER_TRAILER_BYTES;
    long candidate = Math.max(estimatedRowGroupBytes, estimatedFooterBytes);
    long capped =
        Math.min(Math.max(candidate, MIN_RECOMMENDED_BLOCK_BYTES), MAX_RECOMMENDED_BLOCK_BYTES);
    // Round up to the next power of two so the recommendation always lands on a "nice" size
    // (4 KB, 8 KB, 16 KB, 32 KB, ...).
    int rounded = Integer.highestOneBit(Math.toIntExact(capped - 1)) << 1;
    return Math.max(rounded, MIN_RECOMMENDED_BLOCK_BYTES);
  }

  // -----------------------------------------------------------------------
  // Writer
  // -----------------------------------------------------------------------

  private static final class Writer implements IndexHandler.Writer {
    private final OutputFile output;
    private final Schema schema;
    private final int keyFieldCount;
    private final int rowGroupRows;
    private final List<Object[]> keyValues = Lists.newArrayList();
    private final List<String> filePaths = Lists.newArrayList();
    private final LongArrayList positions = new LongArrayList();
    private boolean closed;

    Writer(OutputFile output, Schema schema, int keyFieldCount, int rowGroupRows) {
      this.output = output;
      this.schema = schema;
      this.keyFieldCount = keyFieldCount;
      this.rowGroupRows = rowGroupRows;
    }

    @Override
    public void add(Record key, String filePath, long pos) {
      if (closed) {
        throw new IllegalStateException("Writer already closed");
      }

      if (key == null) {
        throw new IllegalArgumentException("Key record cannot be null");
      }

      // Snapshot the key values so the caller is free to reuse the Record instance.
      Object[] snapshot = new Object[keyFieldCount];
      for (int i = 0; i < keyFieldCount; i++) {
        snapshot[i] = key.get(i);
      }

      keyValues.add(snapshot);
      filePaths.add(filePath);
      positions.add(pos);
    }

    /** Sorts the buffered entries by key and writes the Parquet file. */
    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }

      closed = true;

      int n = keyValues.size();
      // Primitive int[] order keeps the sorting workspace at 4 bytes/row.
      int[] order = new int[n];
      for (int i = 0; i < n; i++) {
        order[i] = i;
      }

      IntArrays.quickSort(order, (a, b) -> compareKeys(keyValues.get(a), keyValues.get(b)));

      try (FileAppender<Record> writer = newAppender()) {
        GenericRecord template = GenericRecord.create(schema);
        for (int sortedRow = 0; sortedRow < n; sortedRow++) {
          int origRow = order[sortedRow];
          Record record = template.copy();
          Object[] ks = keyValues.get(origRow);
          for (int i = 0; i < keyFieldCount; i++) {
            record.set(i, ks[i]);
          }

          record.set(keyFieldCount, filePaths.get(origRow));
          record.set(keyFieldCount + 1, positions.getLong(origRow));
          writer.add(record);
        }
      }
    }

    private FileAppender<Record> newAppender() throws IOException {
      // Force exactly `rowGroupRows` rows per row group: set the size target to a value the
      // writer is guaranteed to exceed in any single record (1 byte) and force the size check to
      // fire on every Nth record by pinning min == max == rowGroupRows.
      String rgRows = Integer.toString(rowGroupRows);
      return Parquet.write(output)
          .schema(schema)
          .createWriterFunc(GenericParquetWriter::create)
          .set(TableProperties.PARQUET_COMPRESSION, "zstd")
          .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "1")
          .set(TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT, rgRows)
          .set(TableProperties.PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT, rgRows)
          // Skip min/max stats for the payload columns -- they are never used for predicate
          // push-down (only the key columns are filtered on) so writing them just bloats the
          // file.
          .set(TableProperties.PARQUET_COLUMN_STATS_ENABLED_PREFIX + FILE_PATH_COLUMN, "false")
          .set(TableProperties.PARQUET_COLUMN_STATS_ENABLED_PREFIX + POS_COLUMN, "false")
          .overwrite()
          .build();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private int compareKeys(Object[] a, Object[] b) {
      for (int i = 0; i < keyFieldCount; i++) {
        Object av = a[i];
        Object bv = b[i];
        if (av == null && bv == null) {
          // go to the next field
          continue;
        } else if (av == null) {
          return -1;
        } else if (bv == null) {
          return 1;
        } else if (av instanceof Comparable) {
          int c = ((Comparable) av).compareTo(bv);
          if (c != 0) {
            return c;
          }
        } else {
          throw new IllegalStateException(
              "Key field " + i + " is not Comparable: " + av.getClass().getName());
        }
      }

      return 0;
    }
  }

  // -----------------------------------------------------------------------
  // Reader
  // -----------------------------------------------------------------------

  /**
   * Stateless reader: a fresh Parquet reader is opened on every {@link #lookup(Record)} call
   * because parquet-mr's reader is built around a one-shot scan and lookups don't share state. The
   * wrapped {@link InputFile} is reused across calls.
   *
   * <p>Iceberg's {@code Parquet.read(...).filter(...)} only uses the predicate for <b>row-group
   * level</b> statistics-based pruning -- it does not filter individual rows out of the matched row
   * group. We rely on that pruning to land on the single row group that can contain the key, then
   * walk it ourselves: rows are sorted by key at write time so we can early-exit as soon as we
   * encounter a row whose key is greater than the target.
   */
  private static final class Reader implements IndexHandler.Reader {
    private final InputFile input;
    private final Schema schema;
    private final int keyFieldCount;
    private final String[] keyColumnNames;

    Reader(InputFile input, Schema schema, int keyFieldCount) {
      this.input = input;
      this.schema = schema;
      this.keyFieldCount = keyFieldCount;
      this.keyColumnNames = new String[keyFieldCount];
      for (int i = 0; i < keyFieldCount; i++) {
        this.keyColumnNames[i] = schema.columns().get(i).name();
      }
    }

    @Override
    public IndexHandler.Hit lookup(Record key) throws IOException {
      if (key == null) {
        throw new IllegalArgumentException("Lookup key cannot be null");
      }

      // Build the equality filter -- only used by Iceberg/parquet-mr for row-group pruning, not
      // for per-row filtering. Start with the first equality directly so we don't pay for
      // alwaysTrue + AND folding (the common keyFieldCount==1 case avoids the AND entirely).
      Object firstVal = requireNonNullKeyField(key, 0);
      Expression filter = Expressions.equal(keyColumnNames[0], firstVal);
      for (int i = 1; i < keyFieldCount; i++) {
        filter =
            Expressions.and(
                filter, Expressions.equal(keyColumnNames[i], requireNonNullKeyField(key, i)));
      }

      try (CloseableIterable<Record> records =
          Parquet.read(input)
              .project(schema)
              .filter(filter)
              .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
              .reuseContainers()
              .build()) {
        for (Record record : records) {
          // Rows are sorted by the key columns at write time, so within the matched row group we
          // can:
          //   cmp <  0  -> not yet reached the target, keep scanning
          //   cmp == 0  -> match, return the hit
          //   cmp >  0  -> passed the target without finding it, key is absent
          int cmp = compareKeyColumns(record, key);
          if (cmp == 0) {
            String filePath = (String) record.getField(FILE_PATH_COLUMN);
            long pos = (long) record.getField(POS_COLUMN);
            return new HitImpl(filePath, pos);
          } else if (cmp > 0) {
            return null;
          }
        }
      }

      return null;
    }

    /**
     * Compares the key columns of a scanned {@code row} against the target {@code key}, in
     * declaration order. Assumes both records use the same key column types as {@link
     * ParquetIndexHandler#schema}; null fields sort first, mirroring the writer's {@link
     * Writer#compareKeys} ordering.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private int compareKeyColumns(Record row, Record key) {
      for (int i = 0; i < keyFieldCount; i++) {
        Object rv = row.get(i);
        Object kv = key.get(i);
        if (rv == null && kv == null) {
          continue;
        } else if (rv == null) {
          return -1;
        } else if (kv == null) {
          return 1;
        }

        int c = ((Comparable) rv).compareTo(kv);
        if (c != 0) {
          return c;
        }
      }

      return 0;
    }

    private Object requireNonNullKeyField(Record key, int position) {
      Object val = key.get(position);
      if (val == null) {
        throw new IllegalArgumentException("Key field " + position + " is null");
      }

      return val;
    }

    @Override
    public void close() {
      // Nothing to close: each lookup opens and closes its own Parquet reader.
    }
  }
}

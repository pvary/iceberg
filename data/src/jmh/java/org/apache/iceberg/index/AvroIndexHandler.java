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
import java.util.Locale;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;

/**
 * Avro-backed inverted-index file format -- the Avro counterpart of {@link ParquetIndexHandler}.
 *
 * <p>The index is stored as a single Avro file whose schema is the user-supplied key schema
 * concatenated with two payload columns: {@code file_path} (string) and {@code pos} (long). Each
 * key column is stored in its declared Iceberg type.
 *
 * <p>Rows are sorted by the key columns at write time so a point lookup can early-exit on the first
 * record whose key is greater than the target. Avro does <em>not</em> support row-group level
 * statistics pruning the way Parquet does, so {@link Reader#lookup(Record)} performs a sequential
 * scan from the start of the file; the cost is therefore {@code O(rowsScanned)} per lookup. The
 * {@link #bucketRows} parameter controls the Avro sync block size in <em>rows</em> (translated to
 * bytes using an empirical row-width estimate) so callers can mirror the same "bucket size" knob
 * exposed by {@link ParquetIndexHandler}; it does not enable any kind of pruning but keeps
 * individual decompression units small.
 *
 * <p>Each handler instance is bound to both a key {@link Schema} and a {@code bucketRows} value.
 */
public class AvroIndexHandler implements IndexHandler {

  /** Field name of the source-file path column. */
  public static final String FILE_PATH_COLUMN = "file_path";

  /** Field name of the row-position column. */
  public static final String POS_COLUMN = "pos";

  /**
   * Approximate compressed bytes per index row, derived from probing the Avro index files written
   * by this handler. Used to translate the {@code bucketRows} parameter into the Avro sync block
   * size in bytes ({@code "write.avro.row-group-size-bytes"} -- the Avro writer flushes a sync
   * block roughly every {@code syncIntervalBytes} bytes of compressed output).
   */
  private static final long ESTIMATED_AVRO_ROW_BYTES = 20L;

  /**
   * Avro sync (block) interval in bytes. The Avro {@code DataFileWriter} flushes a compressed sync
   * block roughly every {@code SYNC_INTERVAL_BYTES} of uncompressed input. Set to 16 MiB (vs.
   * Avro's 64 KiB default) to amortize codec / sync overhead over many more index rows --
   * appropriate for write-once, sequentially-scanned index files where small blocks waste IO but
   * offer no pruning benefit.
   */
  private static final int SYNC_INTERVAL_BYTES = 16 * 1024 * 1024;

  private final Schema schema;
  private final int keyFieldCount;
  private final int bucketRows;
  private final long expectedKeyCount;
  private final String codec;

  /** Convenience constructor that defaults the codec to {@code zstd}. */
  public AvroIndexHandler(Schema keySchema, int bucketRows, long expectedKeyCount) {
    this(keySchema, bucketRows, expectedKeyCount, "zstd");
  }

  /**
   * @param keySchema the schema of the key columns (must contain at least one field and must not
   *     contain fields named {@code file_path} or {@code pos})
   * @param bucketRows number of rows per Avro sync block (converted to bytes via {@link
   *     #ESTIMATED_AVRO_ROW_BYTES}); must be {@code > 0}
   * @param expectedKeyCount sizing hint; must be {@code > 0}
   * @param codec Avro compression codec -- one of {@code uncompressed}, {@code snappy}, {@code
   *     zstd}, {@code gzip}
   */
  public AvroIndexHandler(Schema keySchema, int bucketRows, long expectedKeyCount, String codec) {
    if (keySchema == null || keySchema.columns().isEmpty()) {
      throw new IllegalArgumentException("Key schema must contain at least one field");
    }
    if (bucketRows <= 0) {
      throw new IllegalArgumentException("bucketRows must be > 0: " + bucketRows);
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
    this.bucketRows = bucketRows;
    this.expectedKeyCount = expectedKeyCount;
    this.codec = codec;

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
    return new Writer(output, schema, keyFieldCount, codec);
  }

  @Override
  public IndexHandler.Reader reader(InputFile input) {
    return new Reader(input, schema, keyFieldCount);
  }

  /**
   * Estimates a single sync-block worth of compressed Avro bytes ({@code bucketRows *
   * ESTIMATED_AVRO_ROW_BYTES}) and rounds up to the next power of two. Floored at 4 KiB, capped at
   * 16 MiB.
   *
   * <p>Avro readers always scan from the start, so this hint primarily helps the storage adapter
   * pick a sensible first-GET size when the file is small enough that a single read covers it.
   */
  @Override
  public Integer recommendedReadBlockSize() {
    long candidate = (long) bucketRows * ESTIMATED_AVRO_ROW_BYTES;
    long capped = Math.min(Math.max(candidate, 4 * 1024L), 16L * 1024 * 1024);
    int rounded = Integer.highestOneBit(Math.toIntExact(capped - 1)) << 1;
    return Math.max(rounded, 4 * 1024);
  }

  long expectedKeyCount() {
    return expectedKeyCount;
  }

  // -----------------------------------------------------------------------
  // Writer
  // -----------------------------------------------------------------------

  private static final class Writer implements IndexHandler.Writer {
    private final OutputFile output;
    private final Schema schema;
    private final int keyFieldCount;
    private final String codec;
    private final List<Object[]> keyValues = Lists.newArrayList();
    private final List<String> filePaths = Lists.newArrayList();
    private final LongArrayList positions = new LongArrayList();
    private boolean closed;

    Writer(OutputFile output, Schema schema, int keyFieldCount, String codec) {
      this.output = output;
      this.schema = schema;
      this.keyFieldCount = keyFieldCount;
      this.codec = codec;
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

    /** Sorts the buffered entries by key and writes the Avro file. */
    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }

      closed = true;

      int n = keyValues.size();
      int[] order = new int[n];
      for (int i = 0; i < n; i++) {
        order[i] = i;
      }

      IntArrays.quickSort(order, (a, b) -> compareKeys(keyValues.get(a), keyValues.get(b)));

      // Bypass Iceberg's Avro WriteBuilder so we can call setSyncInterval(): Iceberg's wrapper
      // doesn't expose that knob, and the default 64 KiB block is far too small for the dense
      // index payloads we produce.
      org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema, "index");
      DataWriter<Record> datumWriter = DataWriter.create(avroSchema);
      try (PositionOutputStream out = output.createOrOverwrite();
          DataFileWriter<Record> writer =
              new DataFileWriter<>(datumWriter)
                  .setCodec(toCodecFactory(codec))
                  .setSyncInterval(SYNC_INTERVAL_BYTES)) {
        writer.create(avroSchema, out);
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
          writer.append(record);
        }
      }
    }

    private static CodecFactory toCodecFactory(String codec) {
      switch (codec.toLowerCase(Locale.ROOT)) {
        case "uncompressed":
          return CodecFactory.nullCodec();
        case "snappy":
          return CodecFactory.snappyCodec();
        case "zstd":
          return CodecFactory.zstandardCodec(CodecFactory.DEFAULT_ZSTANDARD_LEVEL);
        case "gzip":
        case "deflate":
          return CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL);
        default:
          throw new IllegalArgumentException("Unsupported Avro codec: " + codec);
      }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private int compareKeys(Object[] a, Object[] b) {
      for (int i = 0; i < keyFieldCount; i++) {
        Object av = a[i];
        Object bv = b[i];
        if (av == null && bv == null) {
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
   * Stateless reader. Avro has no row-group level pruning, so every {@link #lookup(Record)} call
   * performs a sequential scan of the file. Rows are sorted by the key columns at write time, so
   * the scan early-exits on the first record whose key is greater than the target.
   */
  private static final class Reader implements IndexHandler.Reader {
    private final InputFile input;
    private final Schema schema;
    private final int keyFieldCount;

    Reader(InputFile input, Schema schema, int keyFieldCount) {
      this.input = input;
      this.schema = schema;
      this.keyFieldCount = keyFieldCount;
    }

    @Override
    public IndexHandler.Hit lookup(Record key) throws IOException {
      if (key == null) {
        throw new IllegalArgumentException("Lookup key cannot be null");
      }

      try (AvroIterable<Record> records =
          Avro.read(input)
              .project(schema)
              .createResolvingReader(PlannedDataReader::create)
              .reuseContainers()
              .build()) {
        for (Record record : records) {
          // Rows are sorted by the key columns at write time:
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

    @Override
    public void close() {
      // Nothing to close: each lookup opens and closes its own Avro iterator.
    }
  }
}

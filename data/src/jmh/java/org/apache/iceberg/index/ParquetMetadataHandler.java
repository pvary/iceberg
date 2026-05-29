/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */
package org.apache.iceberg.index;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ParquetProperties.WriterVersion;

/**
 * Parquet-backed metadata format.
 *
 * <p>Regular schema: {@code (file_path STRING, update_file_path STRING, min_value LONG, max_value
 * LONG)}. Bitmap-enabled schema adds {@code serialized_mumbling_bitmap BINARY}. The entire file is
 * written as a <em>single</em> Parquet row group so column-level dictionary / RLE / delta encodings
 * see every value at once. Codec is configurable; {@code zstd} by default.
 */
public class ParquetMetadataHandler implements MetadataHandler {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "file_path", Types.StringType.get()),
          required(2, "update_file_path", Types.StringType.get()),
          required(3, "min_value", Types.LongType.get()),
          required(4, "max_value", Types.LongType.get()));

  private static final Schema SCHEMA_WITH_MUMBLING_BITMAP =
      new Schema(
          required(1, "file_path", Types.StringType.get()),
          required(2, "update_file_path", Types.StringType.get()),
          required(3, "min_value", Types.LongType.get()),
          required(4, "max_value", Types.LongType.get()),
          optional(5, "serialized_mumbling_bitmap", Types.BinaryType.get()));

  private final String codec;
  private final boolean withSerializedMumblingBitmap;

  public ParquetMetadataHandler(String codec) {
    this(codec, false);
  }

  public ParquetMetadataHandler(String codec, boolean withSerializedMumblingBitmap) {
    this.codec = codec;
    this.withSerializedMumblingBitmap = withSerializedMumblingBitmap;
  }

  @Override
  public Writer writer(OutputFile output) {
    if (withSerializedMumblingBitmap) {
      return new ParquetWriterWithMumblingBitmap(output, codec);
    }

    return new ParquetWriter(output, codec);
  }

  private abstract static class BaseParquetWriter implements Writer {
    private final OutputFile output;
    private final String codec;
    private final Schema schema;
    private final List<String> paths = Lists.newArrayList();
    private final List<String> updatePaths = Lists.newArrayList();
    private long[] mins = new long[1024];
    private long[] maxes = new long[1024];
    private int size;
    private boolean closed;

    BaseParquetWriter(OutputFile output, String codec, Schema schema) {
      this.output = output;
      this.codec = codec;
      this.schema = schema;
    }

    @Override
    public void add(
        String filePath,
        String updateFilePath,
        long minValue,
        long maxValue,
        byte[] serializedMumblingBitmap) {
      if (closed) {
        throw new IllegalStateException("Writer already closed");
      }

      ensureCapacity(size + 1);
      paths.add(filePath);
      updatePaths.add(updateFilePath);
      mins[size] = minValue;
      maxes[size] = maxValue;
      addSerializedMumblingBitmap(size, serializedMumblingBitmap);
      size++;
    }

    private void ensureCapacity(int cap) {
      if (cap <= mins.length) {
        return;
      }

      int newCap = Math.max(cap, mins.length * 2);
      mins = java.util.Arrays.copyOf(mins, newCap);
      maxes = java.util.Arrays.copyOf(maxes, newCap);
      ensureAdditionalCapacity(newCap);
    }

    protected void ensureAdditionalCapacity(int newCap) {}

    protected void addSerializedMumblingBitmap(int index, byte[] serializedMumblingBitmap) {
      if (serializedMumblingBitmap != null) {
        throw new IllegalArgumentException(
            "This writer was not configured to store serialized MumblingBitmap values");
      }
    }

    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }
      closed = true;

      // Extract the longest common prefix of all file paths and stash it once in the Parquet
      // file-level key/value metadata so path columns only hold the per-row suffix.
      List<String> allPaths = Lists.newArrayList(paths);
      allPaths.addAll(updatePaths);
      String prefix = FilePathPrefix.longestCommonPrefix(allPaths);
      int prefixCharLen = prefix.length();
      // Force everything into a single Parquet row group:
      //   - size target = Integer.MAX_VALUE bytes (~2 GiB; the property is parsed as int, so
      //     Long.MAX_VALUE would overflow) so the byte-based check effectively never trips
      //   - check.min/max-record-count = Integer.MAX_VALUE so the record-count-based check
      //     between size-checks never trips either
      String never = Integer.toString(Integer.MAX_VALUE);
      try (FileAppender<Record> writer =
          Parquet.write(output)
              .schema(schema)
              .createWriterFunc(GenericParquetWriter::create)
              .writerVersion(WriterVersion.PARQUET_2_0)
              .meta(FilePathPrefix.META_KEY, prefix)
              .set(TableProperties.PARQUET_COMPRESSION, codec)
              .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, never)
              .set(TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT, never)
              .set(TableProperties.PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT, never)
              .overwrite()
              .build()) {
        GenericRecord template = GenericRecord.create(schema);
        for (int i = 0; i < size; i++) {
          Record rec = template.copy();
          rec.set(0, paths.get(i).substring(prefixCharLen));
          rec.set(1, updatePaths.get(i).substring(prefixCharLen));
          rec.set(2, mins[i]);
          rec.set(3, maxes[i]);
          setAdditionalFields(rec, i);
          writer.add(rec);
        }
      }
    }

    protected void setAdditionalFields(Record rec, int index) {}
  }

  private static final class ParquetWriter extends BaseParquetWriter {
    ParquetWriter(OutputFile output, String codec) {
      super(output, codec, SCHEMA);
    }
  }

  private static final class ParquetWriterWithMumblingBitmap extends BaseParquetWriter {
    private byte[][] serializedMumblingBitmaps = new byte[1024][];

    ParquetWriterWithMumblingBitmap(OutputFile output, String codec) {
      super(output, codec, SCHEMA_WITH_MUMBLING_BITMAP);
    }

    @Override
    protected void ensureAdditionalCapacity(int newCap) {
      serializedMumblingBitmaps = java.util.Arrays.copyOf(serializedMumblingBitmaps, newCap);
    }

    @Override
    protected void addSerializedMumblingBitmap(int index, byte[] serializedMumblingBitmap) {
      serializedMumblingBitmaps[index] =
          serializedMumblingBitmap == null
              ? null
              : java.util.Arrays.copyOf(serializedMumblingBitmap, serializedMumblingBitmap.length);
    }

    @Override
    protected void setAdditionalFields(Record rec, int index) {
      rec.set(
          4,
          serializedMumblingBitmaps[index] == null
              ? null
              : ByteBuffer.wrap(serializedMumblingBitmaps[index]));
    }
  }
}

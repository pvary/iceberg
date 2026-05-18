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

import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
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
 * <p>Schema: {@code (file_path STRING, offset LONG, min_value LONG)}. The entire file is written
 * as a <em>single</em> Parquet row group so column-level dictionary / RLE / delta encodings see
 * every value at once. Codec is configurable; {@code zstd} by default.
 */
public class ParquetMetadataHandler implements MetadataHandler {

  private static final Schema SCHEMA_WITH_OFFSET =
      new Schema(
          required(1, "file_path", Types.StringType.get()),
          required(2, "offset", Types.LongType.get()),
          required(3, "min_value", Types.LongType.get()));

  private static final Schema SCHEMA_NO_OFFSET =
      new Schema(
          required(1, "file_path", Types.StringType.get()),
          required(2, "min_value", Types.LongType.get()));

  private final String codec;
  private final boolean storeOffsets;

  public ParquetMetadataHandler() {
    this("zstd", true);
  }

  public ParquetMetadataHandler(String codec) {
    this(codec, true);
  }

  /** Kept for source compatibility; {@code rowGroupRows} is ignored (always one row group). */
  public ParquetMetadataHandler(String codec, int rowGroupRows) {
    this(codec, true);
  }

  /** Kept for source compatibility; {@code rowGroupRows} is ignored (always one row group). */
  public ParquetMetadataHandler(String codec, int rowGroupRows, boolean storeOffsets) {
    this(codec, storeOffsets);
  }

  public ParquetMetadataHandler(String codec, boolean storeOffsets) {
    this.codec = codec;
    this.storeOffsets = storeOffsets;
  }

  @Override
  public Writer writer(OutputFile output) {
    return new ParquetWriter(output, codec, storeOffsets);
  }

  private static final class ParquetWriter implements Writer {
    private final OutputFile output;
    private final String codec;
    private final boolean storeOffsets;
    private final List<String> paths = Lists.newArrayList();
    private long[] offsets = new long[1024];
    private long[] mins = new long[1024];
    private int size;
    private boolean closed;

    ParquetWriter(OutputFile output, String codec, boolean storeOffsets) {
      this.output = output;
      this.codec = codec;
      this.storeOffsets = storeOffsets;
    }

    @Override
    public void add(String filePath, long offset, long minValue) {
      ensureCapacity(size + 1);
      paths.add(filePath);
      offsets[size] = offset;
      mins[size] = minValue;
      size++;
    }

    private void ensureCapacity(int cap) {
      if (cap <= offsets.length) {
        return;
      }
      int newCap = Math.max(cap, offsets.length * 2);
      offsets = java.util.Arrays.copyOf(offsets, newCap);
      mins = java.util.Arrays.copyOf(mins, newCap);
    }

    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }
      closed = true;

      Schema writeSchema = storeOffsets ? SCHEMA_WITH_OFFSET : SCHEMA_NO_OFFSET;
      // Extract the longest common prefix of all file paths and stash it once in the Parquet
      // file-level key/value metadata so the file_path column only holds the per-row suffix.
      String prefix = FilePathPrefix.longestCommonPrefix(paths);
      int prefixCharLen = prefix.length();
      // Force everything into a single Parquet row group:
      //   - size target = Integer.MAX_VALUE bytes (~2 GiB; the property is parsed as int, so
      //     Long.MAX_VALUE would overflow) so the byte-based check effectively never trips
      //   - check.min/max-record-count = Integer.MAX_VALUE so the record-count-based check
      //     between size-checks never trips either
      String never = Integer.toString(Integer.MAX_VALUE);
      try (FileAppender<Record> writer =
          Parquet.write(output)
              .schema(writeSchema)
              .createWriterFunc(GenericParquetWriter::create)
              .writerVersion(WriterVersion.PARQUET_2_0)
              .meta(FilePathPrefix.META_KEY, prefix)
              .set(TableProperties.PARQUET_COMPRESSION, codec)
              .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, never)
              .set(TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT, never)
              .set(TableProperties.PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT, never)
              .overwrite()
              .build()) {
        GenericRecord template = GenericRecord.create(writeSchema);
        for (int i = 0; i < size; i++) {
          Record rec = template.copy();
          rec.set(0, paths.get(i).substring(prefixCharLen));
          if (storeOffsets) {
            rec.set(1, offsets[i]);
            rec.set(2, mins[i]);
          } else {
            rec.set(1, mins[i]);
          }
          writer.add(rec);
        }
      }
    }
  }
}




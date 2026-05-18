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
import java.io.UncheckedIOException;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;

/**
 * Avro-backed metadata format. Schema: {@code (file_path STRING, offset LONG, min_value LONG)} (or
 * the same minus {@code offset} when {@code storeOffsets=false}).
 *
 * <p>All entries are buffered. At close time the longest common {@code file_path} prefix is
 * extracted and stored once as Avro file-level metadata under key {@link
 * FilePathPrefix#META_KEY}; the {@code file_path} column then holds only the per-row suffix.
 *
 * <p>Uses the {@code gzip} codec by default.
 */
public class AvroMetadataHandler implements MetadataHandler {

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

  public AvroMetadataHandler() {
    this("gzip", true);
  }

  public AvroMetadataHandler(String codec) {
    this(codec, true);
  }

  public AvroMetadataHandler(String codec, boolean storeOffsets) {
    this.codec = codec;
    this.storeOffsets = storeOffsets;
  }

  @Override
  public Writer writer(OutputFile output) {
    return new AvroWriter(output, codec, storeOffsets);
  }

  private static final class AvroWriter implements Writer {
    private final OutputFile output;
    private final String codec;
    private final boolean storeOffsets;
    private final java.util.ArrayList<String> paths = Lists.newArrayList();
    private long[] offsets = new long[1024];
    private long[] mins = new long[1024];
    private int size;
    private boolean closed;

    AvroWriter(OutputFile output, String codec, boolean storeOffsets) {
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

      Schema schema = storeOffsets ? SCHEMA_WITH_OFFSET : SCHEMA_NO_OFFSET;
      String prefix = FilePathPrefix.longestCommonPrefix(paths);
      int prefixCharLen = prefix.length();

      try (FileAppender<Record> appender =
          Avro.write(output)
              .schema(schema)
              .createWriterFunc(DataWriter::create)
              .set("write.avro.compression-codec", codec)
              .meta(FilePathPrefix.META_KEY, prefix)
              .named("metadata")
              .overwrite()
              .build()) {
        GenericRecord template = GenericRecord.create(schema);
        for (int i = 0; i < size; i++) {
          Record r = template.copy();
          r.set(0, paths.get(i).substring(prefixCharLen));
          if (storeOffsets) {
            r.set(1, offsets[i]);
            r.set(2, mins[i]);
          } else {
            r.set(1, mins[i]);
          }
          appender.add(r);
        }
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to write Avro metadata file", e);
      }
    }
  }
}







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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Buffering "no-frills" metadata format. All entries are buffered, then at close time the longest
 * common {@code file_path} prefix is extracted and written once as a header; each row stores only
 * the suffix:
 *
 * <pre>
 *   [uvarint prefixLen][prefix utf8 bytes]
 *   [uvarint rowCount]
 *   per row: [uvarint suffixLen][suffix utf8 bytes][uvarint offset]?[zigzag varint minValue]
 * </pre>
 *
 * <p>No dictionary, no compression. Establishes the upper bound on file size against which the
 * other {@link MetadataHandler} implementations are compared.
 */
public class PlainBinaryMetadataHandler implements MetadataHandler {

  private final boolean storeOffsets;

  public PlainBinaryMetadataHandler() {
    this(true);
  }

  public PlainBinaryMetadataHandler(boolean storeOffsets) {
    this.storeOffsets = storeOffsets;
  }

  @Override
  public Writer writer(OutputFile output) {
    return new BinaryWriter(output, storeOffsets);
  }

  private static final class BinaryWriter implements Writer {
    private final OutputFile output;
    private final boolean storeOffsets;
    private final java.util.ArrayList<String> paths = Lists.newArrayList();
    private long[] offsets = new long[1024];
    private long[] mins = new long[1024];
    private int size;
    private boolean closed;

    BinaryWriter(OutputFile output, boolean storeOffsets) {
      this.output = output;
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

      String prefix = FilePathPrefix.longestCommonPrefix(paths);
      byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
      int prefixCharLen = prefix.length();

      try (PositionOutputStream out = output.createOrOverwrite();
          BufferedOutputStream buf = new BufferedOutputStream(out, 64 * 1024)) {
        // Header: prefix length (in UTF-8 bytes) + the prefix bytes themselves.
        VarInt.writeUVarInt(buf, prefixBytes.length);
        buf.write(prefixBytes);

        VarInt.writeUVarInt(buf, size);
        for (int i = 0; i < size; i++) {
          String full = paths.get(i);
          byte[] suffixBytes = full.substring(prefixCharLen).getBytes(StandardCharsets.UTF_8);
          VarInt.writeUVarInt(buf, suffixBytes.length);
          buf.write(suffixBytes);
          if (storeOffsets) {
            VarInt.writeUVarLong(buf, offsets[i]);
          }
          VarInt.writeZigZagVarLong(buf, mins[i]);
        }
      }
    }
  }
}


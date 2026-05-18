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

import it.unimi.dsi.fastutil.ints.IntArrays;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Compact binary metadata format with file-path dictionary and delta encoding.
 *
 * <p>All triples are buffered, then sorted by {@code (pathId, offset)}. The file layout is:
 *
 * <pre>
 *   [dictionary: uvarint count][per entry: uvarint utf8Len][utf8 bytes]]
 *   [rows: uvarint count]
 *   per row: [uvarint pathId][zigzag varint offsetDelta][zigzag varint minValueDelta]
 *   [footer: uint32 LE dictionaryOffset]
 * </pre>
 *
 * <p>{@code offsetDelta} resets to the absolute value on every path change. {@code minValueDelta}
 * is delta-encoded against the previous row globally.
 */
public class DictionaryBinaryMetadataHandler implements MetadataHandler {

  private final boolean storeOffsets;

  public DictionaryBinaryMetadataHandler() {
    this(true);
  }

  public DictionaryBinaryMetadataHandler(boolean storeOffsets) {
    this.storeOffsets = storeOffsets;
  }

  @Override
  public Writer writer(OutputFile output) {
    return new DictWriter(output, storeOffsets);
  }

  private static final class DictWriter implements Writer {
    private final PositionOutputStream out;
    private final BufferedOutputStream buf;
    private final boolean storeOffsets;
    // Interned path id per entry, parallel arrays.
    private final java.util.ArrayList<String> dict = Lists.newArrayList();
    private final Map<String, Integer> dictIndex = Maps.newHashMap();
    private int[] pathIds = new int[1024];
    private long[] offsets = new long[1024];
    private long[] minValues = new long[1024];
    private int size;
    private boolean closed;

    DictWriter(OutputFile output, boolean storeOffsets) {
      this.out = output.createOrOverwrite();
      this.buf = new BufferedOutputStream(out, 64 * 1024);
      this.storeOffsets = storeOffsets;
    }

    @Override
    public void add(String filePath, long offset, long minValue) {
      Integer id = dictIndex.get(filePath);
      if (id == null) {
        id = dict.size();
        dict.add(filePath);
        dictIndex.put(filePath, id);
      }
      ensureCapacity(size + 1);
      pathIds[size] = id;
      offsets[size] = offset;
      minValues[size] = minValue;
      size++;
    }

    private void ensureCapacity(int cap) {
      if (cap <= pathIds.length) {
        return;
      }
      int newCap = Math.max(cap, pathIds.length * 2);
      pathIds = java.util.Arrays.copyOf(pathIds, newCap);
      offsets = java.util.Arrays.copyOf(offsets, newCap);
      minValues = java.util.Arrays.copyOf(minValues, newCap);
    }

    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }
      closed = true;
      try {
        // Sort by (pathId, offset).
        int[] order = new int[size];
        for (int i = 0; i < size; i++) {
          order[i] = i;
        }
        IntArrays.quickSort(
            order,
            (a, b) -> {
              int c = Integer.compare(pathIds[a], pathIds[b]);
              if (c != 0) {
                return c;
              }
              return Long.compare(offsets[a], offsets[b]);
            });

        // Serialize dictionary into a temporary buffer so we can record its byte offset.
        // Extract the longest common UTF-16 prefix once and emit only suffixes per entry:
        //   [uvarint prefixLen][prefix bytes]
        //   [uvarint dictCount]
        //   per entry: [uvarint suffixLen][suffix bytes]
        String prefix = FilePathPrefix.longestCommonPrefix(dict);
        byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
        int prefixCharLen = prefix.length();

        ByteArrayOutputStream dictBuf = new ByteArrayOutputStream();
        VarInt.writeUVarInt(dictBuf, prefixBytes.length);
        dictBuf.write(prefixBytes);
        VarInt.writeUVarInt(dictBuf, dict.size());
        for (String s : dict) {
          byte[] b = s.substring(prefixCharLen).getBytes(StandardCharsets.UTF_8);
          VarInt.writeUVarInt(dictBuf, b.length);
          dictBuf.write(b);
        }

        // Rows section first so a reader can stream rows and seek to the dictionary via the footer.
        VarInt.writeUVarInt(buf, size);
        int prevPathId = -1;
        long prevOffset = 0L;
        long prevMin = 0L;
        for (int i = 0; i < size; i++) {
          int row = order[i];
          int pid = pathIds[row];
          long off = offsets[row];
          long mv = minValues[row];
          VarInt.writeUVarInt(buf, pid);
          if (storeOffsets) {
            long offDelta = (pid == prevPathId) ? (off - prevOffset) : off;
            VarInt.writeZigZagVarLong(buf, offDelta);
          }
          VarInt.writeZigZagVarLong(buf, mv - prevMin);
          prevPathId = pid;
          prevOffset = off;
          prevMin = mv;
        }
        buf.flush();

        long dictOffset = out.getPos();
        dictBuf.writeTo(buf);
        buf.flush();

        // Footer: 4-byte little-endian absolute offset of the dictionary section.
        buf.write((int) (dictOffset & 0xFF));
        buf.write((int) ((dictOffset >>> 8) & 0xFF));
        buf.write((int) ((dictOffset >>> 16) & 0xFF));
        buf.write((int) ((dictOffset >>> 24) & 0xFF));
        buf.flush();
      } finally {
        out.close();
      }
    }
  }
}





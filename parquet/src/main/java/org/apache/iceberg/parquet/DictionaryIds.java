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
package org.apache.iceberg.parquet;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Locale;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.io.ParquetDecodingException;

/**
 * Optimization: Better DICTIONARY_IDS decoder for dictionary-encoded data pages.
 *
 * <p>Decodes the dictionary ids of a dictionary-encoded data page: a one-byte bit width, followed
 * by the ids in the RLE/bit-packing hybrid encoding.
 *
 * <p>parquet-java's {@code RunLengthBitPackingHybridDecoder} returns one id per call, and allocates
 * a buffer for every bit-packed run. This decoder reads runs of ids in bulk and looks them up in a
 * decoded dictionary: an RLE run fills the destination with a single value, and bit-packed runs are
 * unpacked in batches into a reused buffer, then looked up in a tight loop.
 *
 * <p>Bit-packed ids are unpacked from the page's backing array with one little-endian 8-byte read
 * per id. parquet-java's generated unpackers read every byte through {@link ByteBuffer#get(int)},
 * which is several times slower for some page buffers.
 */
final class DictionaryIds {
  private static final VarHandle LONGS =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
  private static final int MAX_BIT_WIDTH = 32;
  // an id starts at bit 0 to 7 of a byte, so 5 bytes always hold it
  private static final int MAX_ID_BYTES = 5;
  // bit-packed values are stored in groups of 8, which take bitWidth bytes
  private static final int GROUP_SIZE = 8;
  // groups unpacked at a time; parquet-java writes runs of at most 63 groups
  private static final int GROUPS_PER_BATCH = 64;

  private final byte[] bytes;
  // index in bytes after the last byte of the ids
  private final int limit;
  private final int bitWidth;
  private final int idMask;
  private final int rleIdBytes;
  private final int[] unpacked;
  // index in bytes of the next byte to decode
  private int position;

  // ids left in the current batch: repeated rleId values, or unpacked ids from unpackedPos
  private int remaining = 0;
  private boolean rle = false;
  private int rleId = 0;
  private int unpackedPos = 0;
  // groups of the current bit-packed run that are not unpacked yet
  private int packedGroups = 0;

  private DictionaryIds(int bitWidth, ByteBuffer data) {
    if (bitWidth < 0 || bitWidth > MAX_BIT_WIDTH) {
      throw new ParquetDecodingException("Invalid dictionary id bit width: " + bitWidth);
    }

    if (data.hasArray()) {
      // zero-copy for heap pages
      this.bytes = data.array();
      this.position = data.arrayOffset() + data.position();
      this.limit = data.arrayOffset() + data.limit();
    } else {
      this.bytes = new byte[data.remaining()];
      data.duplicate().get(bytes);
      this.position = 0;
      this.limit = bytes.length;
    }

    this.bitWidth = bitWidth;
    this.idMask = bitWidth == MAX_BIT_WIDTH ? -1 : (1 << bitWidth) - 1;
    this.rleIdBytes = (bitWidth + 7) / 8;
    this.unpacked = new int[GROUPS_PER_BATCH * GROUP_SIZE];
  }

  /**
   * Reads the bit width from the page and returns a decoder of the ids that follow it.
   *
   * @param in the values section of a dictionary-encoded data page; all its bytes are consumed
   */
  static DictionaryIds read(ByteBufferInputStream in) throws IOException {
    int length = in.available();
    if (length == 0) {
      // a page with only nulls has no ids, not even a bit width
      return new DictionaryIds(0, ByteBuffer.allocate(0));
    }

    int bitWidth = in.read();
    return new DictionaryIds(bitWidth, in.slice(length - 1));
  }

  /** Returns the next id. */
  int nextId() {
    ensureIds();
    this.remaining -= 1;
    if (rle) {
      return rleId;
    }

    int id = unpacked[unpackedPos];
    this.unpackedPos += 1;
    return id;
  }

  /** Skips the next {@code count} ids. */
  void skip(int count) {
    int left = count;
    while (left > 0) {
      int run = nextRun(left);
      consume(run);
      left -= run;
    }
  }

  /**
   * Reads the next {@code count} ids and stores the dictionary value of each in {@code dest}.
   *
   * @throws ArrayIndexOutOfBoundsException if an id is not in the dictionary
   */
  void readFloats(float[] dictionary, float[] dest, int offset, int count) {
    int pos = offset;
    int end = offset + count;
    while (pos < end) {
      int run = nextRun(end - pos);
      if (rle) {
        Arrays.fill(dest, pos, pos + run, dictionary[rleId]);
      } else {
        int[] ids = unpacked;
        int start = unpackedPos;
        for (int i = 0; i < run; i += 1) {
          dest[pos + i] = dictionary[ids[start + i]];
        }
      }

      consume(run);
      pos += run;
    }
  }

  /**
   * Reads the next {@code count} ids and stores the dictionary value of each in {@code dest}.
   *
   * @throws ArrayIndexOutOfBoundsException if an id is not in the dictionary
   */
  void readShorts(short[] dictionary, short[] dest, int offset, int count) {
    int pos = offset;
    int end = offset + count;
    while (pos < end) {
      int run = nextRun(end - pos);
      if (rle) {
        Arrays.fill(dest, pos, pos + run, dictionary[rleId]);
      } else {
        int[] ids = unpacked;
        int start = unpackedPos;
        for (int i = 0; i < run; i += 1) {
          dest[pos + i] = dictionary[ids[start + i]];
        }
      }

      consume(run);
      pos += run;
    }
  }

  /** Returns how many of the next ids, at most {@code max}, are in the current batch. */
  private int nextRun(int max) {
    ensureIds();
    return Math.min(remaining, max);
  }

  private void consume(int count) {
    this.remaining -= count;
    if (!rle) {
      this.unpackedPos += count;
    }
  }

  private void ensureIds() {
    while (remaining == 0) {
      if (packedGroups > 0) {
        unpackBatch();
      } else {
        readRunHeader();
      }
    }
  }

  private void readRunHeader() {
    if (position >= limit) {
      throw new ParquetDecodingException("Cannot read dictionary id: no more ids in page");
    }

    int header = readUnsignedVarInt();
    int count = header >>> 1;
    if ((header & 1) == 0) {
      // an RLE run: count repetitions of one id
      this.rle = true;
      this.rleId = readRleId();
      this.remaining = count;
    } else if (bitWidth == 0) {
      // a bit-packed run of count groups of ids that are all 0, which take no bytes
      this.rle = true;
      this.rleId = 0;
      this.remaining = (int) Math.min((long) count * GROUP_SIZE, Integer.MAX_VALUE);
    } else {
      this.packedGroups = count;
    }
  }

  /** Unpacks the next batch of groups of the current bit-packed run. */
  private void unpackBatch() {
    int groups = Math.min(packedGroups, GROUPS_PER_BATCH);
    int length = groups * bitWidth;
    int available = limit - position;
    byte[] source = bytes;
    int start = position;
    if (length > available) {
      // the last run of a page may be truncated: decode the groups that have bytes, zero-padded
      groups = (available + bitWidth - 1) / bitWidth;
      if (groups == 0) {
        throw new ParquetDecodingException(
            String.format(
                Locale.ROOT,
                "Cannot read dictionary id: bit-packed run of %d groups is missing",
                packedGroups));
      }

      source = new byte[groups * bitWidth];
      System.arraycopy(bytes, position, source, 0, available);
      start = 0;
      length = available;
      // the remaining groups of the run have no bytes
      this.packedGroups = groups;
    }

    for (int group = 0; group < groups; group += 1) {
      unpackGroup(source, start + group * bitWidth, group * GROUP_SIZE);
    }

    this.position += length;
    this.packedGroups -= groups;
    this.rle = false;
    this.unpackedPos = 0;
    this.remaining = groups * GROUP_SIZE;
  }

  /**
   * Unpacks the 8 ids of a group of bit-packed ids, which starts at {@code groupStart} in {@code
   * source}, into {@code unpacked} at {@code out}.
   */
  private void unpackGroup(byte[] source, int groupStart, int out) {
    int width = bitWidth;
    int mask = idMask;
    int[] ids = unpacked;
    // the last id of the group starts in its last byte, and is read with 8 bytes from there
    if (groupStart + width + Long.BYTES - 1 <= source.length) {
      for (int i = 0; i < GROUP_SIZE; i += 1) {
        int bit = i * width;
        long word = (long) LONGS.get(source, groupStart + (bit >>> 3));
        ids[out + i] = (int) (word >>> (bit & 7)) & mask;
      }
    } else {
      // near the end of the array, read only the bytes of each id
      for (int i = 0; i < GROUP_SIZE; i += 1) {
        int bit = i * width;
        int first = groupStart + (bit >>> 3);
        int end = Math.min(first + MAX_ID_BYTES, source.length);
        long word = 0L;
        for (int index = first; index < end; index += 1) {
          word |= (source[index] & 0xFFL) << (8 * (index - first));
        }

        ids[out + i] = (int) (word >>> (bit & 7)) & mask;
      }
    }
  }

  private int readRleId() {
    int id = 0;
    for (int i = 0; i < rleIdBytes; i += 1) {
      id |= readByte() << (8 * i);
    }

    return id;
  }

  private int readUnsignedVarInt() {
    int value = 0;
    for (int shift = 0; shift < 35; shift += 7) {
      int next = readByte();
      value |= (next & 0x7F) << shift;
      if ((next & 0x80) == 0) {
        return value;
      }
    }

    throw new ParquetDecodingException("Invalid dictionary id run header: varint is too long");
  }

  private int readByte() {
    if (position >= limit) {
      throw new ParquetDecodingException("Cannot read dictionary id: unexpected end of page");
    }

    int next = bytes[position] & 0xFF;
    this.position += 1;
    return next;
  }
}

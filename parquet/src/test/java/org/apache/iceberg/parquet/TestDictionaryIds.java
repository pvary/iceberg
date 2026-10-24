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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.io.ParquetDecodingException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestDictionaryIds {
  private static final int COUNT = 10_000;

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3, 5, 7, 8, 9, 11, 16, 17, 24, 31, 32})
  public void testNextId(int bitWidth) throws IOException {
    int[] ids = ids(bitWidth, bitWidth);
    DictionaryIds decoder = DictionaryIds.read(stream(encode(bitWidth, ids)));
    for (int i = 0; i < ids.length; i += 1) {
      assertThat(decoder.nextId()).as("id %d with bit width %d", i, bitWidth).isEqualTo(ids[i]);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3, 5, 8, 12})
  public void testBulkLookups(int bitWidth) throws IOException {
    int size = 1 << bitWidth;
    float[] floatDictionary = new float[size];
    short[] shortDictionary = new short[size];
    for (int id = 0; id < size; id += 1) {
      floatDictionary[id] = id * 0.5f - 7.25f;
      shortDictionary[id] = (short) (id * 3 + 1);
    }

    int[] ids = ids(bitWidth, 31L * bitWidth);
    DictionaryIds decoder = DictionaryIds.read(stream(encode(bitWidth, ids)));
    // vary the number of ids per call, so that calls start and end inside runs
    int[] counts = {1, 7, 8, 9, 100, 513, 1_000};
    float[] floats = new float[1_001];
    short[] shorts = new short[1_001];
    int index = 0;
    for (int call = 0; index < ids.length; call += 1) {
      int count = Math.min(counts[call % counts.length], ids.length - index);
      switch (call % 4) {
        case 0 -> {
          decoder.readFloats(floatDictionary, floats, 1, count);
          for (int i = 0; i < count; i += 1) {
            assertThat(floats[1 + i])
                .as("value %d", index + i)
                .isEqualTo(floatDictionary[ids[index + i]]);
          }
        }
        case 1 -> {
          decoder.readShorts(shortDictionary, shorts, 1, count);
          for (int i = 0; i < count; i += 1) {
            assertThat(shorts[1 + i])
                .as("value %d", index + i)
                .isEqualTo(shortDictionary[ids[index + i]]);
          }
        }
        case 2 -> {
          count = 1;
          assertThat(decoder.nextId()).as("id %d", index).isEqualTo(ids[index]);
        }
        default -> decoder.skip(count);
      }

      index += count;
    }
  }

  @Test
  public void testPageInsideLargerBuffer() throws IOException {
    int[] ids = ids(5, 7L);
    byte[] page = encode(5, ids);
    // the page starts after other bytes of the same buffer, and is followed by other bytes
    byte[] buffer = new byte[page.length + 20];
    Arrays.fill(buffer, (byte) 0xFF);
    System.arraycopy(page, 0, buffer, 13, page.length);
    ByteBuffer slice = ByteBuffer.wrap(buffer, 13, page.length);

    assertIds(DictionaryIds.read(ByteBufferInputStream.wrap(slice)), ids);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 7, 13, 24, 32})
  public void testDirectBuffer(int bitWidth) throws IOException {
    int[] ids = ids(bitWidth, 3L * bitWidth);
    byte[] page = encode(bitWidth, ids);
    // not backed by an array, and ending exactly after the ids
    ByteBuffer direct = ByteBuffer.allocateDirect(page.length);
    direct.put(page).flip();

    assertIds(DictionaryIds.read(ByteBufferInputStream.wrap(direct)), ids);
  }

  @Test
  public void testPageInMultipleBuffers() throws IOException {
    int[] ids = ids(9, 11L);
    byte[] page = encode(9, ids);
    int split = page.length / 3;
    ByteBufferInputStream in =
        ByteBufferInputStream.wrap(
            ByteBuffer.wrap(page, 0, split).slice(),
            ByteBuffer.wrap(page, split, page.length - split).slice());

    assertIds(DictionaryIds.read(in), ids);
  }

  @Test
  public void testRunsLongerThanABatch() throws IOException {
    // a bit-packed run of 100 groups, longer than parquet-java writes, with a 2-byte header
    int groups = 100;
    int[] ids = new int[groups * 8];
    ByteBuffer page = ByteBuffer.allocate(3 + ids.length + 2);
    page.put((byte) 8).put((byte) 0xC9).put((byte) 0x01);
    for (int i = 0; i < ids.length; i += 1) {
      // with 8 bits per id, every id is one byte
      ids[i] = (i * 7) & 0xFF;
      page.put((byte) ids[i]);
    }

    // followed by an RLE run of 300 ids with a 2-byte header
    page.put((byte) 0xD8).put((byte) 0x04);
    page.flip();
    ByteBuffer rle = ByteBuffer.allocate(page.remaining() + 1);
    rle.put(page).put((byte) 42).flip();
    int[] expected = Arrays.copyOf(ids, ids.length + 300);
    Arrays.fill(expected, ids.length, expected.length, 42);

    DictionaryIds decoder = DictionaryIds.read(ByteBufferInputStream.wrap(rle));
    assertIds(decoder, expected);
    assertNoMoreIds(decoder);
  }

  @Test
  public void testBitPackedRunWithZeroBitWidth() throws IOException {
    // a bit-packed run of 3 groups of ids that are all 0, which take no bytes
    byte[] page = {0, 0x07};
    DictionaryIds decoder = DictionaryIds.read(stream(page));
    assertIds(decoder, new int[24]);
    assertNoMoreIds(decoder);
  }

  @Test
  public void testTruncatedBitPackedRun() throws IOException {
    // 16 ids that are bit-packed in 2 groups of 3 bytes
    int[] ids = {0, 1, 2, 3, 4, 5, 6, 7, 7, 6, 5, 4, 3, 2, 1, 0};
    byte[] page = encode(3, ids);
    assertThat(page).hasSize(8);

    // the first byte of the second group holds its first 2 ids, and part of the third
    DictionaryIds decoder = DictionaryIds.read(stream(Arrays.copyOf(page, page.length - 2)));
    for (int i = 0; i < 10; i += 1) {
      assertThat(decoder.nextId()).as("id %d", i).isEqualTo(ids[i]);
    }

    // missing bits are zero, and ids after the truncated group are missing
    decoder.skip(6);
    assertNoMoreIds(decoder);
  }

  @Test
  public void testEmptyPage() throws IOException {
    // a page with only nulls has no ids, not even a bit width
    assertNoMoreIds(DictionaryIds.read(stream(new byte[0])));
  }

  @Test
  public void testInvalidPages() {
    assertThatThrownBy(() -> DictionaryIds.read(stream(new byte[] {33, 0x02, 0})))
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessage("Invalid dictionary id bit width: 33");

    // an RLE run of 5 ids, without the id
    assertThatThrownBy(() -> DictionaryIds.read(stream(new byte[] {8, 0x0A})).nextId())
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessage("Cannot read dictionary id: unexpected end of page");

    // a bit-packed run of 1 group, without the ids
    assertThatThrownBy(() -> DictionaryIds.read(stream(new byte[] {8, 0x03})).nextId())
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessage("Cannot read dictionary id: bit-packed run of 1 groups is missing");

    byte[] longVarInt = {8, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0};
    assertThatThrownBy(() -> DictionaryIds.read(stream(longVarInt)).nextId())
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessage("Invalid dictionary id run header: varint is too long");
  }

  /**
   * Checks the next ids. Encoded pages can have more ids, because the last group of a bit-packed
   * run is padded to 8 ids; readers stop after the number of values in the page.
   */
  private static void assertIds(DictionaryIds decoder, int[] ids) {
    for (int i = 0; i < ids.length; i += 1) {
      assertThat(decoder.nextId()).as("id %d", i).isEqualTo(ids[i]);
    }
  }

  private static void assertNoMoreIds(DictionaryIds decoder) {
    assertThatThrownBy(decoder::nextId)
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessage("Cannot read dictionary id: no more ids in page");
  }

  /**
   * Returns ids of the given bit width in runs of mixed lengths, so that the encoder writes both
   * RLE runs (8 or more repeated ids) and bit-packed runs.
   */
  private static int[] ids(int bitWidth, long seed) {
    Random random = new Random(seed);
    int[] ids = new int[COUNT];
    int index = 0;
    while (index < COUNT) {
      int runLength = 1 + random.nextInt(random.nextInt(4) == 0 ? 40 : 3);
      int id = bitWidth == 0 ? 0 : (int) (random.nextLong() >>> (Long.SIZE - bitWidth));
      for (int i = 0; i < runLength && index < COUNT; i += 1) {
        ids[index] = id;
        index += 1;
      }
    }

    return ids;
  }

  /** Encodes ids like the values of a dictionary-encoded page: the bit width, then the runs. */
  private static byte[] encode(int bitWidth, int[] ids) throws IOException {
    try (RunLengthBitPackingHybridEncoder encoder =
        new RunLengthBitPackingHybridEncoder(
            bitWidth, 64, 1024 * 1024, new HeapByteBufferAllocator())) {
      for (int id : ids) {
        encoder.writeInt(id);
      }

      ByteArrayOutputStream page = new ByteArrayOutputStream();
      page.write(bitWidth);
      encoder.toBytes().writeAllTo(page);
      return page.toByteArray();
    }
  }

  private static ByteBufferInputStream stream(byte[] page) {
    return ByteBufferInputStream.wrap(ByteBuffer.wrap(page));
  }
}

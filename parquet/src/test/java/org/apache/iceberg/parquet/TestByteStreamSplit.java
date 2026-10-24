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

import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestByteStreamSplit {
  // bytes before the streams, so that they do not start at index 0 of the array
  private static final int BASE = 3;
  private static final int FLOAT16_BYTES = 2;
  // values in the destination before and after the decoded run, which must not change
  private static final int MARGIN = 5;
  private static final float FLOAT_SENTINEL = -123.5f;
  private static final short SHORT_SENTINEL = (short) 0xABCD;

  /**
   * Decodes every run of a page with a number of values that is not a multiple of the 8 values that
   * the decoders transpose at a time. The streams end at the end of the array, so runs that end
   * with the page also check that no decoder reads past the end of its streams.
   */
  @ParameterizedTest
  @ValueSource(ints = {0, 1, 7, 8, 9, 16, 37})
  public void testEveryRun(int valueCount) {
    byte[] floatPage = page(valueCount, Float.BYTES, valueCount);
    byte[] float16Page = page(valueCount, FLOAT16_BYTES, valueCount + 1);
    for (int start = 0; start <= valueCount; start += 1) {
      for (int count = 0; count <= valueCount - start; count += 1) {
        assertFloats(floatPage, valueCount, start, count);
        assertFloat16s(float16Page, valueCount, start, count);
        assertFloat16sAsFloats(float16Page, valueCount, start, count);
      }
    }
  }

  @Test
  public void testLongRuns() {
    int valueCount = 10_003;
    byte[] floatPage = page(valueCount, Float.BYTES, 1);
    byte[] float16Page = page(valueCount, FLOAT16_BYTES, 2);
    int[][] runs = {{0, valueCount}, {1, 2560}, {5, 9_998}, {7_443, 2_560}, {9_000, 1_003}};
    for (int[] run : runs) {
      assertFloats(floatPage, valueCount, run[0], run[1]);
      assertFloat16s(float16Page, valueCount, run[0], run[1]);
      assertFloat16sAsFloats(float16Page, valueCount, run[0], run[1]);
    }
  }

  @Test
  public void testSpecialValues() {
    float[] floats = {
      0.0f,
      -0.0f,
      1.0f,
      -2.5f,
      Float.MIN_VALUE,
      -Float.MIN_NORMAL,
      Float.MAX_VALUE,
      Float.POSITIVE_INFINITY,
      Float.NEGATIVE_INFINITY,
      Float.NaN,
      Float.intBitsToFloat(0x7FC00001),
      Float.intBitsToFloat(0xFF800001),
      3.14159f,
      -1e-30f,
      65504.0f,
      -0.333f,
      42.0f
    };

    // split the values into streams
    int valueCount = floats.length;
    byte[] page = new byte[valueCount * Float.BYTES];
    for (int i = 0; i < valueCount; i += 1) {
      int bits = Float.floatToRawIntBits(floats[i]);
      for (int k = 0; k < Float.BYTES; k += 1) {
        page[k * valueCount + i] = (byte) (bits >>> (8 * k));
      }
    }

    float[] decoded = new float[valueCount];
    ByteStreamSplit.readFloats(page, 0, valueCount, decoded, 0, valueCount);
    assertThat(rawBits(decoded)).isEqualTo(rawBits(floats));
  }

  private static void assertFloats(byte[] page, int valueCount, int start, int count) {
    float[] expected = new float[count + 2 * MARGIN];
    float[] actual = new float[count + 2 * MARGIN];
    Arrays.fill(expected, FLOAT_SENTINEL);
    Arrays.fill(actual, FLOAT_SENTINEL);
    for (int i = 0; i < count; i += 1) {
      expected[MARGIN + i] = Float.intBitsToFloat(bits(page, valueCount, start + i, Float.BYTES));
    }

    ByteStreamSplit.readFloats(page, BASE + start, valueCount, actual, MARGIN, count);
    assertThat(rawBits(actual))
        .as("%d floats at %d of %d", count, start, valueCount)
        .isEqualTo(rawBits(expected));
  }

  private static void assertFloat16s(byte[] page, int valueCount, int start, int count) {
    short[] expected = new short[count + 2 * MARGIN];
    short[] actual = new short[count + 2 * MARGIN];
    Arrays.fill(expected, SHORT_SENTINEL);
    Arrays.fill(actual, SHORT_SENTINEL);
    for (int i = 0; i < count; i += 1) {
      expected[MARGIN + i] = (short) bits(page, valueCount, start + i, FLOAT16_BYTES);
    }

    ByteStreamSplit.readFloat16s(page, BASE + start, valueCount, actual, MARGIN, count);
    assertThat(actual).as("%d float16s at %d of %d", count, start, valueCount).isEqualTo(expected);
  }

  private static void assertFloat16sAsFloats(byte[] page, int valueCount, int start, int count) {
    float[] expected = new float[count + 2 * MARGIN];
    float[] actual = new float[count + 2 * MARGIN];
    Arrays.fill(expected, FLOAT_SENTINEL);
    Arrays.fill(actual, FLOAT_SENTINEL);
    for (int i = 0; i < count; i += 1) {
      expected[MARGIN + i] =
          ParquetConversions.float16ToFloat(
              (short) bits(page, valueCount, start + i, FLOAT16_BYTES));
    }

    ByteStreamSplit.readFloat16sAsFloats(page, BASE + start, valueCount, actual, MARGIN, count);
    assertThat(rawBits(actual))
        .as("%d float16s as floats at %d of %d", count, start, valueCount)
        .isEqualTo(rawBits(expected));
  }

  /** Random streams of {@code valueCount} values that start at {@link #BASE} and end the array. */
  private static byte[] page(int valueCount, int width, long seed) {
    byte[] page = new byte[BASE + valueCount * width];
    new Random(seed).nextBytes(page);
    return page;
  }

  /** Joins the bytes of a value one at a time, the reference for the decoders. */
  private static int bits(byte[] page, int valueCount, int value, int width) {
    int bits = 0;
    for (int k = 0; k < width; k += 1) {
      bits |= (page[BASE + k * valueCount + value] & 0xFF) << (8 * k);
    }

    return bits;
  }

  private static int[] rawBits(float[] values) {
    int[] bits = new int[values.length];
    for (int i = 0; i < values.length; i += 1) {
      bits[i] = Float.floatToRawIntBits(values[i]);
    }

    return bits;
  }
}

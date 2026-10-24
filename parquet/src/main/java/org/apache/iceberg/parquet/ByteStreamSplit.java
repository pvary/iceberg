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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * Optimization: Better BYTE_STREAM_SPLIT decoders for FLOAT16 and FLOAT32 values.
 *
 * <p>Decodes runs of values from the streams of a BYTE_STREAM_SPLIT page.
 *
 * <p>A BYTE_STREAM_SPLIT page stores byte 0 of all values, then byte 1 of all values, and so on.
 * Joining the bytes of a value one at a time takes a load, a mask and a shift per byte. The JIT
 * does not vectorize that loop, because it widens bytes to ints. Instead, these decoders read 8
 * bytes of every stream at once, the bytes of 8 consecutive values, and transpose them with masks
 * and shifts on longs, which takes less than half the operations per value.
 *
 * <p>The streams of a page have {@code valueCount} bytes each, and the decoders only read the bytes
 * of the values that they decode.
 */
final class ByteStreamSplit {
  private static final VarHandle LONGS =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
  // values transposed at a time: the bytes of a stream that one long holds
  private static final int BLOCK = Long.BYTES;
  // the low byte of every 16-bit lane of a long
  private static final long LOW_BYTES = 0x00FF00FF00FF00FFL;
  // the low 16 bits of every 32-bit lane of a long
  private static final long LOW_SHORTS = 0x0000FFFF0000FFFFL;

  private ByteStreamSplit() {}

  /**
   * Decodes 4-byte FLOAT values.
   *
   * @param streams the bytes of the page
   * @param byte0 index in {@code streams} of byte 0 of the first value to decode
   * @param valueCount the number of values in the page, which is the length of every stream
   * @param dest array to fill
   * @param offset index in {@code dest} of the first value
   * @param count number of values to decode
   */
  static void readFloats(
      byte[] streams, int byte0, int valueCount, float[] dest, int offset, int count) {
    int byte1 = byte0 + valueCount;
    int byte2 = byte1 + valueCount;
    int byte3 = byte2 + valueCount;
    int pos = 0;
    for (; pos + BLOCK <= count; pos += BLOCK) {
      // byte k of values pos to pos + 7, one per byte of wk
      long w0 = (long) LONGS.get(streams, byte0 + pos);
      long w1 = (long) LONGS.get(streams, byte1 + pos);
      long w2 = (long) LONGS.get(streams, byte2 + pos);
      long w3 = (long) LONGS.get(streams, byte3 + pos);
      // 16-bit lane j holds bytes 0 and 1 (low01, high01) or 2 and 3 (low23, high23) of value 2j
      // (low01, low23) or 2j + 1 (high01, high23)
      long low01 = (w0 & LOW_BYTES) | (w1 & LOW_BYTES) << 8;
      long high01 = ((w0 >>> 8) & LOW_BYTES) | (w1 & ~LOW_BYTES);
      long low23 = (w2 & LOW_BYTES) | (w3 & LOW_BYTES) << 8;
      long high23 = ((w2 >>> 8) & LOW_BYTES) | (w3 & ~LOW_BYTES);
      // 32-bit lanes: values 0 and 4, 2 and 6, 1 and 5, and 3 and 7
      long values04 = (low01 & LOW_SHORTS) | (low23 & LOW_SHORTS) << 16;
      long values26 = ((low01 >>> 16) & LOW_SHORTS) | (low23 & ~LOW_SHORTS);
      long values15 = (high01 & LOW_SHORTS) | (high23 & LOW_SHORTS) << 16;
      long values37 = ((high01 >>> 16) & LOW_SHORTS) | (high23 & ~LOW_SHORTS);
      int out = offset + pos;
      dest[out] = Float.intBitsToFloat((int) values04);
      dest[out + 1] = Float.intBitsToFloat((int) values15);
      dest[out + 2] = Float.intBitsToFloat((int) values26);
      dest[out + 3] = Float.intBitsToFloat((int) values37);
      dest[out + 4] = Float.intBitsToFloat((int) (values04 >>> 32));
      dest[out + 5] = Float.intBitsToFloat((int) (values15 >>> 32));
      dest[out + 6] = Float.intBitsToFloat((int) (values26 >>> 32));
      dest[out + 7] = Float.intBitsToFloat((int) (values37 >>> 32));
    }

    for (; pos < count; pos += 1) {
      dest[offset + pos] =
          Float.intBitsToFloat(
              (streams[byte0 + pos] & 0xFF)
                  | (streams[byte1 + pos] & 0xFF) << 8
                  | (streams[byte2 + pos] & 0xFF) << 16
                  | streams[byte3 + pos] << 24);
    }
  }

  /**
   * Decodes 2-byte FLOAT16 values as IEEE 754 half-precision bits.
   *
   * @param streams the bytes of the page
   * @param low index in {@code streams} of the low byte of the first value to decode
   * @param valueCount the number of values in the page, which is the length of every stream
   * @param dest array to fill
   * @param offset index in {@code dest} of the first value
   * @param count number of values to decode
   */
  static void readFloat16s(
      byte[] streams, int low, int valueCount, short[] dest, int offset, int count) {
    int high = low + valueCount;
    int pos = 0;
    for (; pos + BLOCK <= count; pos += BLOCK) {
      long lows = (long) LONGS.get(streams, low + pos);
      long highs = (long) LONGS.get(streams, high + pos);
      // 16-bit lane j holds value 2j (even) or 2j + 1 (odd)
      long even = (lows & LOW_BYTES) | (highs & LOW_BYTES) << 8;
      long odd = ((lows >>> 8) & LOW_BYTES) | (highs & ~LOW_BYTES);
      int out = offset + pos;
      dest[out] = (short) even;
      dest[out + 1] = (short) odd;
      dest[out + 2] = (short) (even >>> 16);
      dest[out + 3] = (short) (odd >>> 16);
      dest[out + 4] = (short) (even >>> 32);
      dest[out + 5] = (short) (odd >>> 32);
      dest[out + 6] = (short) (even >>> 48);
      dest[out + 7] = (short) (odd >>> 48);
    }

    for (; pos < count; pos += 1) {
      dest[offset + pos] = (short) ((streams[high + pos] << 8) | (streams[low + pos] & 0xFF));
    }
  }

  /**
   * Decodes 2-byte FLOAT16 values and converts them to single precision.
   *
   * @param streams the bytes of the page
   * @param low index in {@code streams} of the low byte of the first value to decode
   * @param valueCount the number of values in the page, which is the length of every stream
   * @param dest array to fill
   * @param offset index in {@code dest} of the first value
   * @param count number of values to decode
   */
  static void readFloat16sAsFloats(
      byte[] streams, int low, int valueCount, float[] dest, int offset, int count) {
    int high = low + valueCount;
    int pos = 0;
    for (; pos + BLOCK <= count; pos += BLOCK) {
      long lows = (long) LONGS.get(streams, low + pos);
      long highs = (long) LONGS.get(streams, high + pos);
      long even = (lows & LOW_BYTES) | (highs & LOW_BYTES) << 8;
      long odd = ((lows >>> 8) & LOW_BYTES) | (highs & ~LOW_BYTES);
      int out = offset + pos;
      dest[out] = ParquetConversions.float16ToFloat((short) even);
      dest[out + 1] = ParquetConversions.float16ToFloat((short) odd);
      dest[out + 2] = ParquetConversions.float16ToFloat((short) (even >>> 16));
      dest[out + 3] = ParquetConversions.float16ToFloat((short) (odd >>> 16));
      dest[out + 4] = ParquetConversions.float16ToFloat((short) (even >>> 32));
      dest[out + 5] = ParquetConversions.float16ToFloat((short) (odd >>> 32));
      dest[out + 6] = ParquetConversions.float16ToFloat((short) (even >>> 48));
      dest[out + 7] = ParquetConversions.float16ToFloat((short) (odd >>> 48));
    }

    for (; pos < count; pos += 1) {
      dest[offset + pos] =
          ParquetConversions.float16ToFloat(
              (short) ((streams[high + pos] << 8) | (streams[low + pos] & 0xFF)));
    }
  }
}

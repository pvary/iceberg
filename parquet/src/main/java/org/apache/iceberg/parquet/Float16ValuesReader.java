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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.util.Locale;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.values.RequiresPreviousReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation.Float16LogicalTypeAnnotation;

/**
 * Optimization: FLOAT16 decoders that do not allocate a Binary per value, and also read runs of
 * values in bulk.
 *
 * <p>A {@link ValuesReader} for Parquet FLOAT16 columns, FIXED_LEN_BYTE_ARRAY(2) values that hold a
 * little-endian IEEE 754 half-precision float.
 *
 * <p>parquet-java returns every FIXED_LEN_BYTE_ARRAY value as a {@link Binary}. For PLAIN pages it
 * also wraps a new {@link ByteBuffer} slice, so each 2-byte value costs two objects. These readers
 * decode the half-precision bits straight from the page buffer through {@link #readFloat16()} and
 * {@link #readFloat()}, without per-value allocation. {@link #readBytes()} still works for readers
 * that consume the raw bytes, such as the fixed[2] reader used for legacy schemas.
 */
abstract class Float16ValuesReader extends ValuesReader {
  private static final int BYTES = Float16LogicalTypeAnnotation.BYTES;

  /**
   * Creates a reader for a non-dictionary encoding.
   *
   * <p>PLAIN and BYTE_STREAM_SPLIT pages are decoded directly. Other encodings, such as the
   * DELTA_BYTE_ARRAY fallback of the v2 writer, are read through parquet-java and converted.
   */
  static Float16ValuesReader forEncoding(Encoding encoding, ColumnDescriptor desc) {
    return switch (encoding) {
      case PLAIN -> new PlainReader();
      case BYTE_STREAM_SPLIT -> new ByteStreamSplitReader();
      default -> new BinaryReader(encoding.getValuesReader(desc, ValuesType.VALUES));
    };
  }

  /** Creates a reader for a dictionary-encoded page, using a decoded dictionary. */
  static Float16ValuesReader forDictionary(DecodedDictionary dictionary) {
    return new DictionaryReader(dictionary);
  }

  /**
   * Returns the bits of the next half-precision value.
   *
   * @return IEEE 754 half-precision bits
   */
  abstract short readFloat16();

  @Override
  public float readFloat() {
    return ParquetConversions.float16ToFloat(readFloat16());
  }

  /**
   * Reads the bits of the next {@code count} half-precision values into an array.
   *
   * @param dest array to fill
   * @param offset index in {@code dest} of the first value
   * @param count number of values to read
   */
  void readFloat16s(short[] dest, int offset, int count) {
    for (int i = 0; i < count; i += 1) {
      dest[offset + i] = readFloat16();
    }
  }

  /**
   * Reads the next {@code count} values, converted to single precision, into an array.
   *
   * @param dest array to fill
   * @param offset index in {@code dest} of the first value
   * @param count number of values to read
   */
  void readFloats(float[] dest, int offset, int count) {
    for (int i = 0; i < count; i += 1) {
      dest[offset + i] = readFloat();
    }
  }

  private static Binary toBinary(short float16Bits) {
    return Binary.fromConstantByteArray(new byte[] {(byte) float16Bits, (byte) (float16Bits >> 8)});
  }

  /** Reads PLAIN pages: values are stored back to back in little-endian byte order. */
  private static final class PlainReader extends Float16ValuesReader {
    private ByteBuffer bytes = null;
    // a view of the page bytes as values, which is the only position that is updated
    private ShortBuffer values = null;

    @Override
    public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
      // zero-copy unless the page spans multiple buffers
      this.bytes = in.slice(in.available()).order(ByteOrder.LITTLE_ENDIAN);
      this.values = bytes.asShortBuffer();
    }

    @Override
    short readFloat16() {
      return values.get();
    }

    @Override
    void readFloat16s(short[] dest, int offset, int count) {
      // a bulk copy, which only swaps bytes on big-endian platforms
      values.get(dest, offset, count);
    }

    @Override
    void readFloats(float[] dest, int offset, int count) {
      int start = values.position();
      if (count > values.limit() - start) {
        throw new ParquetDecodingException(
            String.format(
                Locale.ROOT,
                "Cannot read %d values at index %d of %d in PLAIN float16 page",
                count,
                start,
                values.limit()));
      }

      for (int i = 0; i < count; i += 1) {
        dest[offset + i] = ParquetConversions.float16ToFloat(values.get(start + i));
      }

      values.position(start + count);
    }

    @Override
    public Binary readBytes() {
      int index = values.position();
      values.position(index + 1);
      return Binary.fromConstantByteBuffer(bytes, bytes.position() + index * BYTES, BYTES);
    }

    @Override
    public void skip() {
      skip(1);
    }

    @Override
    public void skip(int count) {
      values.position(values.position() + count);
    }
  }

  /**
   * Reads BYTE_STREAM_SPLIT pages: the low bytes of all values are followed by their high bytes.
   *
   * <p>Unlike parquet-java, this reader does not decode the page into a separate buffer. It reads
   * the streams from the page's backing array, and only copies pages that are not backed by one.
   */
  private static final class ByteStreamSplitReader extends Float16ValuesReader {
    private byte[] streams = null;
    // index in streams of the low byte of the first value
    private int base = 0;
    private int valueCount = 0;
    private int index = 0;

    @Override
    public void initFromPage(int valueCountWithNulls, ByteBufferInputStream in) throws IOException {
      int length = in.available();
      // the number of values is not stored, nulls are not encoded, and padding is not allowed
      if (length % BYTES != 0 || length / BYTES > valueCountWithNulls) {
        throw new ParquetDecodingException(
            String.format(
                Locale.ROOT,
                "Invalid BYTE_STREAM_SPLIT float16 stream: %d bytes for at most %d values",
                length,
                valueCountWithNulls));
      }

      ByteBuffer buffer = in.slice(length);
      if (buffer.hasArray()) {
        this.streams = buffer.array();
        this.base = buffer.arrayOffset() + buffer.position();
      } else {
        this.streams = new byte[length];
        this.base = 0;
        buffer.get(streams);
      }

      this.valueCount = length / BYTES;
      this.index = 0;
    }

    /** Checks that {@code count} values remain, and returns the index of the first one. */
    private int advance(int count) {
      int pos = index;
      if (count < 0 || count > valueCount - pos) {
        throw new ParquetDecodingException(
            String.format(
                Locale.ROOT,
                "Cannot read %d values at index %d of %d in BYTE_STREAM_SPLIT float16 stream",
                count,
                pos,
                valueCount));
      }

      this.index = pos + count;
      return pos;
    }

    @Override
    short readFloat16() {
      int low = base + advance(1);
      return (short) ((streams[low + valueCount] << 8) | (streams[low] & 0xFF));
    }

    @Override
    void readFloat16s(short[] dest, int offset, int count) {
      ByteStreamSplit.readFloat16s(streams, base + advance(count), valueCount, dest, offset, count);
    }

    @Override
    void readFloats(float[] dest, int offset, int count) {
      ByteStreamSplit.readFloat16sAsFloats(
          streams, base + advance(count), valueCount, dest, offset, count);
    }

    @Override
    public Binary readBytes() {
      return toBinary(readFloat16());
    }

    @Override
    public void skip() {
      skip(1);
    }

    @Override
    public void skip(int count) {
      advance(count);
    }
  }

  /**
   * Reads dictionary ids with the RLE/bit-packing hybrid encoding and looks them up, in bulk for
   * runs of values.
   */
  private static final class DictionaryReader extends Float16ValuesReader {
    private final DecodedDictionary dictionary;
    private DictionaryIds ids = null;

    private DictionaryReader(DecodedDictionary dictionary) {
      this.dictionary = dictionary;
    }

    @Override
    public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
      this.ids = DictionaryIds.read(in);
    }

    @Override
    public int readValueDictionaryId() {
      return ids.nextId();
    }

    @Override
    short readFloat16() {
      return dictionary.float16s[ids.nextId()];
    }

    @Override
    public float readFloat() {
      return dictionary.floats[ids.nextId()];
    }

    @Override
    void readFloat16s(short[] dest, int offset, int count) {
      ids.readShorts(dictionary.float16s, dest, offset, count);
    }

    @Override
    void readFloats(float[] dest, int offset, int count) {
      ids.readFloats(dictionary.floats, dest, offset, count);
    }

    @Override
    public Binary readBytes() {
      return dictionary.binaries.decodeToBinary(ids.nextId());
    }

    @Override
    public void skip() {
      ids.skip(1);
    }

    @Override
    public void skip(int count) {
      ids.skip(count);
    }
  }

  /** Reads values through a parquet-java reader for encodings that are not decoded directly. */
  private static final class BinaryReader extends Float16ValuesReader
      implements RequiresPreviousReader {
    private final ValuesReader delegate;

    private BinaryReader(ValuesReader delegate) {
      this.delegate = delegate;
    }

    @Override
    public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
      delegate.initFromPage(valueCount, in);
    }

    @Override
    short readFloat16() {
      return delegate.readBytes().get2BytesLittleEndian();
    }

    @Override
    public Binary readBytes() {
      return delegate.readBytes();
    }

    @Override
    public void skip() {
      delegate.skip();
    }

    @Override
    public void skip(int count) {
      delegate.skip(count);
    }

    @Override
    public void setPreviousReader(ValuesReader reader) {
      if (delegate instanceof RequiresPreviousReader requiresPrevious) {
        ValuesReader previous =
            reader instanceof BinaryReader binaryReader ? binaryReader.delegate : reader;
        requiresPrevious.setPreviousReader(previous);
      }
    }
  }

  /**
   * A FLOAT16 dictionary decoded once per row group, so that dictionary-encoded values are a single
   * array lookup.
   */
  static final class DecodedDictionary {
    private final Dictionary binaries;
    private final short[] float16s;
    private final float[] floats;

    private DecodedDictionary(Dictionary binaries) {
      int size = binaries.getMaxId() + 1;
      this.binaries = binaries;
      this.float16s = new short[size];
      this.floats = new float[size];
      for (int id = 0; id < size; id += 1) {
        short bits = binaries.decodeToBinary(id).get2BytesLittleEndian();
        float16s[id] = bits;
        floats[id] = ParquetConversions.float16ToFloat(bits);
      }
    }

    static DecodedDictionary decode(Dictionary dictionary) {
      return new DecodedDictionary(dictionary);
    }
  }
}

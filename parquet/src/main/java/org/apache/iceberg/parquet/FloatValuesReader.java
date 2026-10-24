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
import java.nio.FloatBuffer;
import java.util.Locale;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;

/**
 * Optimization: FLOAT decoders that read runs of values in bulk.
 *
 * <p>A {@link ValuesReader} for Parquet FLOAT columns that can also read runs of values in bulk.
 *
 * <p>parquet-java decodes PLAIN pages one value at a time through a stream, copies whole
 * BYTE_STREAM_SPLIT pages into a separate buffer before decoding them, and decodes dictionary ids
 * one at a time. These readers decode PLAIN pages with a bulk copy, BYTE_STREAM_SPLIT pages
 * straight from the page buffer, and dictionary-encoded pages with bulk id decoding and lookups in
 * a dictionary decoded once per row group.
 */
abstract class FloatValuesReader extends ValuesReader {
  /**
   * Creates a reader for a non-dictionary encoding.
   *
   * <p>PLAIN and BYTE_STREAM_SPLIT pages are decoded directly. Other encodings are read through
   * parquet-java.
   */
  static FloatValuesReader forEncoding(Encoding encoding, ColumnDescriptor desc) {
    return switch (encoding) {
      case PLAIN -> new PlainReader();
      case BYTE_STREAM_SPLIT -> new ByteStreamSplitReader();
      default -> new DelegatingReader(encoding.getValuesReader(desc, ValuesType.VALUES));
    };
  }

  /**
   * Creates a reader for a dictionary-encoded page.
   *
   * @param dictionary the dictionary values, decoded with {@link #decodeDictionary(Dictionary)}
   */
  static FloatValuesReader forDictionary(float[] dictionary) {
    return new DictionaryReader(dictionary);
  }

  /** Decodes a FLOAT dictionary, so that dictionary-encoded values are a single array lookup. */
  static float[] decodeDictionary(Dictionary dictionary) {
    float[] floats = new float[dictionary.getMaxId() + 1];
    for (int id = 0; id < floats.length; id += 1) {
      floats[id] = dictionary.decodeToFloat(id);
    }

    return floats;
  }

  @Override
  public abstract float readFloat();

  /**
   * Reads the next {@code count} values into an array.
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

  @Override
  public void skip() {
    skip(1);
  }

  /** Reads PLAIN pages: values are stored back to back in little-endian byte order. */
  private static final class PlainReader extends FloatValuesReader {
    // a view of the page bytes as values, which is the only position that is updated
    private FloatBuffer values = null;

    @Override
    public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
      // zero-copy unless the page spans multiple buffers
      this.values = in.slice(in.available()).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer();
    }

    @Override
    public float readFloat() {
      return values.get();
    }

    @Override
    void readFloats(float[] dest, int offset, int count) {
      // a bulk copy, which only swaps bytes on big-endian platforms
      values.get(dest, offset, count);
    }

    @Override
    public void skip(int count) {
      values.position(values.position() + count);
    }
  }

  /**
   * Reads BYTE_STREAM_SPLIT pages: byte 0 of all values is followed by byte 1 of all values, and so
   * on.
   *
   * <p>Unlike parquet-java, this reader does not decode the page into a separate buffer. It reads
   * the streams from the page's backing array, and only copies pages that are not backed by one.
   */
  private static final class ByteStreamSplitReader extends FloatValuesReader {
    private byte[] streams = null;
    // index in streams of byte 0 of the first value
    private int base = 0;
    private int valueCount = 0;
    private int index = 0;

    @Override
    public void initFromPage(int valueCountWithNulls, ByteBufferInputStream in) throws IOException {
      int length = in.available();
      // the number of values is not stored, nulls are not encoded, and padding is not allowed
      if (length % Float.BYTES != 0 || length / Float.BYTES > valueCountWithNulls) {
        throw new ParquetDecodingException(
            String.format(
                Locale.ROOT,
                "Invalid BYTE_STREAM_SPLIT float stream: %d bytes for at most %d values",
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

      this.valueCount = length / Float.BYTES;
      this.index = 0;
    }

    /** Checks that {@code count} values remain, and returns the index of the first one. */
    private int advance(int count) {
      int pos = index;
      if (count < 0 || count > valueCount - pos) {
        throw new ParquetDecodingException(
            String.format(
                Locale.ROOT,
                "Cannot read %d values at index %d of %d in BYTE_STREAM_SPLIT float stream",
                count,
                pos,
                valueCount));
      }

      this.index = pos + count;
      return pos;
    }

    @Override
    public float readFloat() {
      int byte0 = base + advance(1);
      int byte1 = byte0 + valueCount;
      int byte2 = byte1 + valueCount;
      int byte3 = byte2 + valueCount;
      return Float.intBitsToFloat(
          (streams[byte0] & 0xFF)
              | (streams[byte1] & 0xFF) << 8
              | (streams[byte2] & 0xFF) << 16
              | streams[byte3] << 24);
    }

    @Override
    void readFloats(float[] dest, int offset, int count) {
      ByteStreamSplit.readFloats(streams, base + advance(count), valueCount, dest, offset, count);
    }

    @Override
    public void skip(int count) {
      advance(count);
    }
  }

  /** Reads dictionary ids with the RLE/bit-packing hybrid encoding and looks them up. */
  private static final class DictionaryReader extends FloatValuesReader {
    private final float[] dictionary;
    private DictionaryIds ids = null;

    private DictionaryReader(float[] dictionary) {
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
    public float readFloat() {
      return dictionary[ids.nextId()];
    }

    @Override
    void readFloats(float[] dest, int offset, int count) {
      ids.readFloats(dictionary, dest, offset, count);
    }

    @Override
    public void skip(int count) {
      ids.skip(count);
    }
  }

  /** Reads values through a parquet-java reader for encodings that are not decoded directly. */
  private static final class DelegatingReader extends FloatValuesReader {
    private final ValuesReader delegate;

    private DelegatingReader(ValuesReader delegate) {
      this.delegate = delegate;
    }

    @Override
    public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
      delegate.initFromPage(valueCount, in);
    }

    @Override
    public float readFloat() {
      return delegate.readFloat();
    }

    @Override
    public void skip() {
      delegate.skip();
    }

    @Override
    public void skip(int count) {
      delegate.skip(count);
    }
  }
}

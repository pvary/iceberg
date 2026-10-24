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
import java.util.Locale;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.parquet.CorruptDeltaByteArrays;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.values.RequiresPreviousReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;

abstract class PageIterator<T> extends BasePageIterator implements TripleIterator<T> {
  @SuppressWarnings("unchecked")
  static <T> PageIterator<T> newIterator(ColumnDescriptor desc, String writerVersion) {
    switch (desc.getPrimitiveType().getPrimitiveTypeName()) {
      case BOOLEAN:
        return (PageIterator<T>)
            new PageIterator<Boolean>(desc, writerVersion) {
              @Override
              public Boolean next() {
                return nextBoolean();
              }
            };
      case INT32:
        return (PageIterator<T>)
            new PageIterator<Integer>(desc, writerVersion) {
              @Override
              public Integer next() {
                return nextInteger();
              }
            };
      case INT64:
        return (PageIterator<T>)
            new PageIterator<Long>(desc, writerVersion) {
              @Override
              public Long next() {
                return nextLong();
              }
            };
      case INT96:
        return (PageIterator<T>)
            new PageIterator<Binary>(desc, writerVersion) {
              @Override
              public Binary next() {
                return nextBinary();
              }
            };
      case FLOAT:
        return (PageIterator<T>) new FloatPageIterator(desc, writerVersion);
      case DOUBLE:
        return (PageIterator<T>)
            new PageIterator<Double>(desc, writerVersion) {
              @Override
              public Double next() {
                return nextDouble();
              }
            };
      case FIXED_LEN_BYTE_ARRAY:
        if (ParquetConversions.isFloat16(desc.getPrimitiveType())) {
          // Parquet only allows FLOAT16 to annotate FIXED_LEN_BYTE_ARRAY(2)
          return (PageIterator<T>) new Float16PageIterator(desc, writerVersion);
        }

        return (PageIterator<T>)
            new PageIterator<Binary>(desc, writerVersion) {
              @Override
              public Binary next() {
                return nextBinary();
              }
            };
      case BINARY:
        return (PageIterator<T>)
            new PageIterator<Binary>(desc, writerVersion) {
              @Override
              public Binary next() {
                return nextBinary();
              }
            };
      default:
        throw new UnsupportedOperationException(
            "Unsupported primitive type: " + desc.getPrimitiveType().getPrimitiveTypeName());
    }
  }

  // when positive, every list has this many non-null elements, so levels are derived, not decoded
  private int fixedListSize = 0;
  // the fixed list size in effect for the current page
  private int pageListSize = 0;

  private PageIterator(ColumnDescriptor desc, String writerVersion) {
    super(desc, writerVersion);
  }

  /**
   * Sets the number of elements of every list, or 0 to decode the levels of every value.
   *
   * <p>Takes effect from the next page.
   *
   * @see ColumnIterator#setFixedListSize(int)
   */
  void setFixedListSize(int size) {
    Preconditions.checkArgument(size >= 0, "Invalid fixed list size: %s", size);
    Preconditions.checkArgument(
        size == 0 || desc.getMaxRepetitionLevel() == 1,
        "Cannot read fixed-size lists from column %s with max repetition level %s",
        desc,
        desc.getMaxRepetitionLevel());
    this.fixedListSize = size;
  }

  @Override
  public void setPage(DataPage page) {
    super.setPage(page);
    this.pageListSize = fixedListSize;
    // parquet-java only ends pages at row boundaries, so a page holds whole lists
    if (pageListSize > 0 && triplesCount % pageListSize != 0) {
      throw new ParquetDecodingException(
          String.format(
              Locale.ROOT,
              "Cannot read lists of %d elements from column %s: page has %d values",
              pageListSize,
              desc,
              triplesCount));
    }

    advance();
  }

  @Override
  public int currentDefinitionLevel() {
    Preconditions.checkArgument(currentDL >= 0, "Should not read definition, past page end");
    return currentDL;
  }

  @Override
  public int currentRepetitionLevel() {
    //    Preconditions.checkArgument(currentDL >= 0, "Should not read repetition, past page end");
    return currentRL;
  }

  @Override
  public boolean nextBoolean() {
    advance();
    try {
      return values.readBoolean();
    } catch (RuntimeException e) {
      throw handleRuntimeException(e);
    }
  }

  @Override
  public int nextInteger() {
    advance();
    try {
      return values.readInteger();
    } catch (RuntimeException e) {
      throw handleRuntimeException(e);
    }
  }

  @Override
  public long nextLong() {
    advance();
    try {
      return values.readLong();
    } catch (RuntimeException e) {
      throw handleRuntimeException(e);
    }
  }

  @Override
  public float nextFloat() {
    advance();
    try {
      return values.readFloat();
    } catch (RuntimeException e) {
      throw handleRuntimeException(e);
    }
  }

  @Override
  public double nextDouble() {
    advance();
    try {
      return values.readDouble();
    } catch (RuntimeException e) {
      throw handleRuntimeException(e);
    }
  }

  @Override
  public Binary nextBinary() {
    advance();
    try {
      return values.readBytes();
    } catch (RuntimeException e) {
      throw handleRuntimeException(e);
    }
  }

  /**
   * Consumes the levels of a run of values in this page, so that the values can be read in bulk.
   *
   * <p>The run starts at the next value, and ends before a null value, before a value other than
   * the first that starts a new innermost list, at the end of the page, or after {@code maxCount}
   * values.
   *
   * @return the number of values in the run
   */
  int consumeRun(int maxCount) {
    if (pageListSize > 0) {
      return consumeFixedSizeRun(maxCount);
    }

    int maxDL = desc.getMaxDefinitionLevel();
    int maxRL = desc.getMaxRepetitionLevel();
    int count = 0;
    while (count < maxCount
        && hasNext
        && currentDL == maxDL
        && (count == 0 || currentRL == maxRL)) {
      advance();
      count += 1;
    }

    return count;
  }

  /**
   * Consumes a run of a page of fixed-size lists without decoding levels: the run ends at the end
   * of the current list, which is also never after the end of the page.
   */
  private int consumeFixedSizeRun(int maxCount) {
    if (!hasNext || maxCount <= 0) {
      return 0;
    }

    // the levels of the next value are current, so triplesRead already counts it
    int next = triplesRead - 1;
    int count = Math.min(maxCount, pageListSize - next % pageListSize);
    this.triplesRead = next + count;
    advance();
    return count;
  }

  @Override
  public <V> V nextNull() {
    advance();
    // values do not contain nulls
    return null;
  }

  void advance() {
    if (triplesRead < triplesCount) {
      if (pageListSize > 0) {
        // all elements are defined, and only the first element of a list starts a new record
        this.currentDL = desc.getMaxDefinitionLevel();
        this.currentRL = triplesRead % pageListSize == 0 ? 0 : 1;
      } else {
        this.currentDL = definitionLevels.nextInt();
        this.currentRL = repetitionLevels.nextInt();
      }

      this.triplesRead += 1;
      this.hasNext = true;
    } else {
      this.currentDL = -1;
      this.currentRL = -1;
      this.hasNext = false;
    }
  }

  RuntimeException handleRuntimeException(RuntimeException exception) {
    if (CorruptDeltaByteArrays.requiresSequentialReads(writerVersion, valueEncoding)
        && exception instanceof ArrayIndexOutOfBoundsException) {
      // this is probably PARQUET-246, which may happen if reading data with
      // MR because this can't be detected without reading all footers
      throw new ParquetDecodingException(
          "Read failure possibly due to " + "PARQUET-246: try setting parquet.split.files to false",
          new ParquetDecodingException(
              String.format(
                  Locale.ROOT,
                  "Can't read value in column %s at value %d out of %d in current page. "
                      + "repetition level: %d, definition level: %d",
                  desc,
                  triplesRead,
                  triplesCount,
                  currentRL,
                  currentDL),
              exception));
    }
    throw new ParquetDecodingException(
        String.format(
            Locale.ROOT,
            "Can't read value in column %s at value %d out of %d in current page. "
                + "repetition level: %d, definition level: %d",
            desc,
            triplesRead,
            triplesCount,
            currentRL,
            currentDL),
        exception);
  }

  @Override
  protected void initDataReader(Encoding dataEncoding, ByteBufferInputStream in, int valueCount) {
    ValuesReader previousReader = values;

    this.valueEncoding = dataEncoding;

    // TODO: May want to change this so that this class is not dictionary-aware.
    // For dictionary columns, this class could rely on wrappers to correctly handle dictionaries
    // This isn't currently possible because RLE must be read by getDictionaryBasedValuesReader
    if (dataEncoding.usesDictionary()) {
      if (dictionary == null) {
        throw new ParquetDecodingException(
            "could not read page in col "
                + desc
                + " as the dictionary was missing for encoding "
                + dataEncoding);
      }
    }

    this.values = newValuesReader(dataEncoding);

    try {
      values.initFromPage(valueCount, in);
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read page in col " + desc, e);
    }

    if (CorruptDeltaByteArrays.requiresSequentialReads(writerVersion, dataEncoding)
        && previousReader instanceof RequiresPreviousReader) {
      // previous reader can only be set if reading sequentially
      ((RequiresPreviousReader) values).setPreviousReader(previousReader);
    }
  }

  /**
   * Returns a reader for the values of a data page with the given encoding. For dictionary
   * encodings, {@link #dictionary} is the dictionary of the column chunk.
   */
  ValuesReader newValuesReader(Encoding dataEncoding) {
    if (dataEncoding.usesDictionary()) {
      return dataEncoding.getDictionaryBasedValuesReader(desc, ValuesType.VALUES, dictionary);
    }

    return dataEncoding.getValuesReader(desc, ValuesType.VALUES);
  }

  @Override
  protected void initDefinitionLevelsReader(
      DataPageV1 dataPageV1, ColumnDescriptor desc, ByteBufferInputStream in, int triplesCount)
      throws IOException {
    ValuesReader dlReader =
        dataPageV1.getDlEncoding().getValuesReader(desc, ValuesType.DEFINITION_LEVEL);
    this.definitionLevels = new ValuesReaderIntIterator(dlReader);
    dlReader.initFromPage(triplesCount, in);
  }

  @Override
  protected void initDefinitionLevelsReader(DataPageV2 dataPageV2, ColumnDescriptor desc) {
    this.definitionLevels =
        newRLEIterator(desc.getMaxDefinitionLevel(), dataPageV2.getDefinitionLevels());
  }

  /**
   * Optimization: Batch reading and better dictionary decoding.
   *
   * <p>See {@link FloatValuesReader} for how pages are decoded. This iterator creates the reader
   * for every page, decodes the dictionary of a column chunk once and reuses it for all of its
   * dictionary-encoded pages, and reads runs of values in bulk in {@link #nextFloats(float[], int,
   * int)}.
   */
  private static final class FloatPageIterator extends PageIterator<Float> {
    // decoded when the first dictionary-encoded page of the column chunk is read
    private float[] decodedDictionary = null;
    private FloatValuesReader floatValues = null;

    private FloatPageIterator(ColumnDescriptor desc, String writerVersion) {
      super(desc, writerVersion);
    }

    @Override
    public Float next() {
      return nextFloat();
    }

    @Override
    public void setDictionary(Dictionary dict) {
      super.setDictionary(dict);
      this.decodedDictionary = null;
    }

    @Override
    protected ValuesReader newValuesReader(Encoding dataEncoding) {
      if (dataEncoding.usesDictionary()) {
        if (decodedDictionary == null) {
          this.decodedDictionary = FloatValuesReader.decodeDictionary(dictionary);
        }

        this.floatValues = FloatValuesReader.forDictionary(decodedDictionary);
      } else {
        this.floatValues = FloatValuesReader.forEncoding(dataEncoding, desc);
      }

      return floatValues;
    }

    /** Reads a run of values from this page, which ends at the end of the page. */
    @Override
    public int nextFloats(float[] dest, int offset, int maxCount) {
      Preconditions.checkPositionIndexes(offset, offset + maxCount, dest.length);
      int count = consumeRun(maxCount);
      try {
        floatValues.readFloats(dest, offset, count);
      } catch (RuntimeException e) {
        throw handleRuntimeException(e);
      }

      return count;
    }
  }

  /**
   * Optimization: Batch reading and better dictionary decoding.
   *
   * <p>See {@link Float16ValuesReader} for how pages are decoded. This iterator creates the reader
   * for every page, decodes the dictionary of a column chunk once and reuses it for all of its
   * dictionary-encoded pages, and reads runs of values in bulk in {@link #nextFloat16s(short[],
   * int, int)} and {@link #nextFloats(float[], int, int)}.
   *
   * <p>{@link #next()} and {@link #nextBinary()} return the 2 bytes of a value, like for other
   * FIXED_LEN_BYTE_ARRAY columns; {@link #nextFloat()} returns the value as a float.
   */
  private static final class Float16PageIterator extends PageIterator<Binary> {
    // decoded when the first dictionary-encoded page of the column chunk is read
    private Float16ValuesReader.DecodedDictionary decodedDictionary = null;
    private Float16ValuesReader float16Values = null;

    private Float16PageIterator(ColumnDescriptor desc, String writerVersion) {
      super(desc, writerVersion);
    }

    @Override
    public Binary next() {
      return nextBinary();
    }

    @Override
    public void setDictionary(Dictionary dict) {
      super.setDictionary(dict);
      this.decodedDictionary = null;
    }

    @Override
    protected ValuesReader newValuesReader(Encoding dataEncoding) {
      if (dataEncoding.usesDictionary()) {
        if (decodedDictionary == null) {
          this.decodedDictionary = Float16ValuesReader.DecodedDictionary.decode(dictionary);
        }

        this.float16Values = Float16ValuesReader.forDictionary(decodedDictionary);
      } else {
        this.float16Values = Float16ValuesReader.forEncoding(dataEncoding, desc);
      }

      return float16Values;
    }

    @Override
    public short nextFloat16() {
      advance();
      try {
        return float16Values.readFloat16();
      } catch (RuntimeException e) {
        throw handleRuntimeException(e);
      }
    }

    /** Reads a run of values from this page, which ends at the end of the page. */
    @Override
    public int nextFloat16s(short[] dest, int offset, int maxCount) {
      Preconditions.checkPositionIndexes(offset, offset + maxCount, dest.length);
      int count = consumeRun(maxCount);
      try {
        float16Values.readFloat16s(dest, offset, count);
      } catch (RuntimeException e) {
        throw handleRuntimeException(e);
      }

      return count;
    }

    /** Reads a run of values from this page, which ends at the end of the page. */
    @Override
    public int nextFloats(float[] dest, int offset, int maxCount) {
      Preconditions.checkPositionIndexes(offset, offset + maxCount, dest.length);
      int count = consumeRun(maxCount);
      try {
        float16Values.readFloats(dest, offset, count);
      } catch (RuntimeException e) {
        throw handleRuntimeException(e);
      }

      return count;
    }
  }
}

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

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.api.Binary;

public abstract class ColumnIterator<T> extends BaseColumnIterator implements TripleIterator<T> {
  @SuppressWarnings("unchecked")
  static <T> ColumnIterator<T> newIterator(ColumnDescriptor desc, String writerVersion) {
    switch (desc.getPrimitiveType().getPrimitiveTypeName()) {
      case BOOLEAN:
        return (ColumnIterator<T>)
            new ColumnIterator<Boolean>(desc, writerVersion) {
              @Override
              public Boolean next() {
                return nextBoolean();
              }
            };
      case INT32:
        return (ColumnIterator<T>)
            new ColumnIterator<Integer>(desc, writerVersion) {
              @Override
              public Integer next() {
                return nextInteger();
              }
            };
      case INT64:
        return (ColumnIterator<T>)
            new ColumnIterator<Long>(desc, writerVersion) {
              @Override
              public Long next() {
                return nextLong();
              }
            };
      case INT96:
        return (ColumnIterator<T>)
            new ColumnIterator<Binary>(desc, writerVersion) {
              @Override
              public Binary next() {
                return nextBinary();
              }
            };
      case FLOAT:
        return (ColumnIterator<T>)
            new ColumnIterator<Float>(desc, writerVersion) {
              @Override
              public Float next() {
                return nextFloat();
              }
            };
      case DOUBLE:
        return (ColumnIterator<T>)
            new ColumnIterator<Double>(desc, writerVersion) {
              @Override
              public Double next() {
                return nextDouble();
              }
            };
      case FIXED_LEN_BYTE_ARRAY:
      case BINARY:
        return (ColumnIterator<T>)
            new ColumnIterator<Binary>(desc, writerVersion) {
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

  private final PageIterator<T> pageIterator;

  private ColumnIterator(ColumnDescriptor desc, String writerVersion) {
    super(desc);
    this.pageIterator = PageIterator.newIterator(desc, writerVersion);
  }

  /**
   * Optimization: Depend on the caller to know the size of every list in a column.
   *
   * <p>Declares that every list of this column has exactly {@code size} non-null elements, so that
   * repetition and definition levels are derived instead of decoded for every value.
   *
   * <p>Parquet has no fixed-size list type, so readers find where each list ends by decoding the
   * levels of all of its elements. When every list is known to have the same size, like the
   * embeddings of one model, the levels follow from the position of a value in its page, and bulk
   * reads such as {@link #nextFloat16s(short[], int, int)} read whole lists without touching the
   * levels.
   *
   * <p>This is only valid for a list column with no other repeated levels, where no list is null or
   * empty, every list has exactly {@code size} elements and no element is null. Pages that do not
   * hold whole lists are rejected, but other violations are not detected and lead to incorrect
   * results. The setting takes effect from the next page, so it should be set before reading.
   *
   * @param size the number of elements of every list, or 0 to decode the levels
   * @throws IllegalArgumentException if the size is negative, or the column is not a list of
   *     primitive values with a single repeated level
   */
  public void setFixedListSize(int size) {
    pageIterator.setFixedListSize(size);
  }

  @Override
  public int currentDefinitionLevel() {
    advance();
    return pageIterator.currentDefinitionLevel();
  }

  @Override
  public int currentRepetitionLevel() {
    advance();
    return pageIterator.currentRepetitionLevel();
  }

  @Override
  public boolean nextBoolean() {
    this.triplesRead += 1;
    advance();
    return pageIterator.nextBoolean();
  }

  @Override
  public int nextInteger() {
    this.triplesRead += 1;
    advance();
    return pageIterator.nextInteger();
  }

  @Override
  public long nextLong() {
    this.triplesRead += 1;
    advance();
    return pageIterator.nextLong();
  }

  @Override
  public float nextFloat() {
    this.triplesRead += 1;
    advance();
    return pageIterator.nextFloat();
  }

  @Override
  public double nextDouble() {
    this.triplesRead += 1;
    advance();
    return pageIterator.nextDouble();
  }

  @Override
  public Binary nextBinary() {
    this.triplesRead += 1;
    advance();
    return pageIterator.nextBinary();
  }

  @Override
  public short nextFloat16() {
    this.triplesRead += 1;
    advance();
    return pageIterator.nextFloat16();
  }

  @Override
  public int nextFloat16s(short[] values, int offset, int maxCount) {
    Preconditions.checkPositionIndexes(offset, offset + maxCount, values.length);
    int count = 0;
    int read;
    do {
      advance();
      // a run that reached the end of a page continues only with values of the same list
      if (count > 0 && !continuesList()) {
        break;
      }

      read = pageIterator.nextFloat16s(values, offset + count, maxCount - count);
      this.triplesRead += read;
      count += read;
    } while (read > 0 && count < maxCount);

    return count;
  }

  @Override
  public int nextFloats(float[] values, int offset, int maxCount) {
    Preconditions.checkPositionIndexes(offset, offset + maxCount, values.length);
    int count = 0;
    int read;
    do {
      advance();
      // a run that reached the end of a page continues only with values of the same list
      if (count > 0 && !continuesList()) {
        break;
      }

      read = pageIterator.nextFloats(values, offset + count, maxCount - count);
      this.triplesRead += read;
      count += read;
    } while (read > 0 && count < maxCount);

    return count;
  }

  /** Returns whether the next value continues the current innermost list. */
  private boolean continuesList() {
    // past the last value, the repetition level is -1
    return pageIterator.currentRepetitionLevel() == desc.getMaxRepetitionLevel();
  }

  @Override
  public <N> N nextNull() {
    this.triplesRead += 1;
    advance();
    return pageIterator.nextNull();
  }

  @Override
  protected BasePageIterator pageIterator() {
    return pageIterator;
  }
}

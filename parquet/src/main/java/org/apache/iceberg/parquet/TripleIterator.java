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

import java.util.Iterator;
import org.apache.parquet.io.api.Binary;

interface TripleIterator<T> extends Iterator<T> {
  /**
   * Returns the definition level from the current triple.
   *
   * <p>This method does not advance this iterator.
   *
   * @return the definition level of the current triple.
   * @throws java.util.NoSuchElementException if there are no more elements
   */
  int currentDefinitionLevel();

  /**
   * Returns the repetition level from the current triple or 0 if there are no more elements.
   *
   * <p>This method does not advance this iterator.
   *
   * @return the repetition level of the current triple, or 0 if there is no current triple.
   * @throws java.util.NoSuchElementException if there are no more elements
   */
  int currentRepetitionLevel();

  /**
   * Returns the next value as an un-boxed boolean.
   *
   * <p>This method has the same behavior as {@link #next()} and will advance this iterator.
   *
   * @return the next value as an un-boxed boolean
   * @throws java.util.NoSuchElementException if there are no more elements
   * @throws UnsupportedOperationException if the underlying data values are not booleans
   */
  default boolean nextBoolean() {
    throw new UnsupportedOperationException("Not a boolean column");
  }

  /**
   * Returns the next value as an un-boxed int.
   *
   * <p>This method has the same behavior as {@link #next()} and will advance this iterator.
   *
   * @return the next value as an un-boxed int
   * @throws java.util.NoSuchElementException if there are no more elements
   * @throws UnsupportedOperationException if the underlying data values are not ints
   */
  default int nextInteger() {
    throw new UnsupportedOperationException("Not an integer column");
  }

  /**
   * Returns the next value as an un-boxed long.
   *
   * <p>This method has the same behavior as {@link #next()} and will advance this iterator.
   *
   * @return the next value as an un-boxed long
   * @throws java.util.NoSuchElementException if there are no more elements
   * @throws UnsupportedOperationException if the underlying data values are not longs
   */
  default long nextLong() {
    throw new UnsupportedOperationException("Not a long column");
  }

  /**
   * Returns the next value as an un-boxed float.
   *
   * <p>This method has the same behavior as {@link #next()} and will advance this iterator.
   *
   * @return the next value as an un-boxed float
   * @throws java.util.NoSuchElementException if there are no more elements
   * @throws UnsupportedOperationException if the underlying data values are not floats
   */
  default float nextFloat() {
    throw new UnsupportedOperationException("Not a float column");
  }

  /**
   * Returns the next value as an un-boxed double.
   *
   * <p>This method has the same behavior as {@link #next()} and will advance this iterator.
   *
   * @return the next value as an un-boxed double
   * @throws java.util.NoSuchElementException if there are no more elements
   * @throws UnsupportedOperationException if the underlying data values are not doubles
   */
  default double nextDouble() {
    throw new UnsupportedOperationException("Not a double column");
  }

  /**
   * Returns the next value as a Binary.
   *
   * <p>This method has the same behavior as {@link #next()} and will advance this iterator.
   *
   * @return the next value as a Binary
   * @throws java.util.NoSuchElementException if there are no more elements
   * @throws UnsupportedOperationException if the underlying data values are not binary
   */
  default Binary nextBinary() {
    throw new UnsupportedOperationException("Not a binary column");
  }

  /**
   * Optimization: Read a FLOAT16 value without allocating a Binary.
   *
   * <p>Returns the next value of a Parquet FLOAT16 column as IEEE 754 half-precision bits.
   *
   * <p>This method has the same behavior as {@link #next()} and will advance this iterator. Unlike
   * {@link #nextBinary()}, this reads the value directly from the page and does not allocate. Use
   * {@link #nextFloat()} to read the value as a single-precision float.
   *
   * @return the bits of the next half-precision value
   * @throws java.util.NoSuchElementException if there are no more elements
   * @throws UnsupportedOperationException if the column is not a FLOAT16 column
   */
  default short nextFloat16() {
    throw new UnsupportedOperationException("Not a float16 column");
  }

  /**
   * Optimization: Read the values of a list in bulk, instead of one call per value.
   *
   * <p>Reads a run of non-null values of a Parquet FLOAT16 column as IEEE 754 half-precision bits.
   *
   * <p>The values are decoded in bulk, page by page, instead of one call per value. The run starts
   * at the next value and stops before a null value, or before a value that starts a new innermost
   * list, so that for a repeated column it reads the rest of the current list. Every value of a
   * non-repeated column continues the run. Reads stop after {@code maxCount} values, so a caller
   * can detect a list that is longer than expected by checking the next repetition level.
   *
   * @param values array to fill
   * @param offset index in {@code values} of the first value
   * @param maxCount maximum number of values to read
   * @return the number of values read, which is 0 if the next value is null or there is none
   * @throws UnsupportedOperationException if the column is not a FLOAT16 column
   */
  default int nextFloat16s(short[] values, int offset, int maxCount) {
    throw new UnsupportedOperationException("Not a float16 column");
  }

  /**
   * Optimization: Read the values of a list in bulk, instead of one call per value.
   *
   * <p>Reads a run of non-null values of a Parquet FLOAT or FLOAT16 column as floats.
   *
   * <p>The run is read like {@link #nextFloat16s(short[], int, int)}, and FLOAT16 values are
   * converted to single precision.
   *
   * @param values array to fill
   * @param offset index in {@code values} of the first value
   * @param maxCount maximum number of values to read
   * @return the number of values read, which is 0 if the next value is null or there is none
   * @throws UnsupportedOperationException if the column is not a FLOAT or FLOAT16 column
   */
  default int nextFloats(float[] values, int offset, int maxCount) {
    throw new UnsupportedOperationException("Not a float or float16 column");
  }

  /**
   * Returns null and advances the iterator.
   *
   * <p>This method has the same behavior as {@link #next()} and will advance this iterator.
   *
   * @return null
   * @throws java.util.NoSuchElementException if there are no more elements
   */
  <N> N nextNull();
}

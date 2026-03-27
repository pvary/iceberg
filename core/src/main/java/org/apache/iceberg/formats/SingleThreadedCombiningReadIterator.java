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
package org.apache.iceberg.formats;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.SkippingCloseableIterator;

class SingleThreadedCombiningReadIterator<E> implements CloseableIterator<E> {
  private final SkippingCloseableIterator<E>[] iterators;
  private final FormatModel.Combiner<E> combiner;
  private final E[] nextElements;
  private boolean closed = false;
  private boolean exhausted = false;
  private long position = 0L;

  /** Pre-fetched combined result, computed eagerly in {@link #advanceAligned()}. */
  private E pendingResult = null;

  @SuppressWarnings("unchecked")
  SingleThreadedCombiningReadIterator(
      List<SkippingCloseableIterator<E>> iteratorList, FormatModel.Combiner<E> combiner) {
    this.iterators = iteratorList.toArray(new SkippingCloseableIterator[0]);
    this.combiner = combiner;
    this.nextElements = combiner.newArray(iterators.length);
  }

  /**
   * Attempts to advance all iterators assuming they are already aligned at {@link #position}. This
   * is the hot path for Spark reads without gaps/deletes: one {@code hasNext()}, one {@code
   * next()}, and one {@code position()} per iterator. If any iterator jumps ahead, falls back to
   * the slower realignment path.
   */
  private boolean advanceAligned() {
    long expectedPosition = position + 1;

    for (int i = 0; i < iterators.length; ++i) {
      SkippingCloseableIterator<E> iterator = iterators[i];
      if (!iterator.hasNext()) {
        exhausted = true;
        return false;
      }

      E value = iterator.next();
      long newPosition = iterator.position();
      nextElements[i] = value;

      if (newPosition != expectedPosition) {
        return realignFrom(i, newPosition);
      }
    }

    position = expectedPosition;
    pendingResult = combiner.combine(nextElements);
    return true;
  }

  /**
   * Restarts alignment from the beginning after iterator {@code savedIndex} has already produced a
   * value for logical position {@code savedPosition - 1}. Only used when an iterator jumps ahead.
   */
  private boolean realignFrom(int savedIndex, long savedPosition) {
    position = savedPosition - 1;
    int currentIndex = savedIndex;

    while (true) {
      long targetPosition = position;

      for (int i = 0; i < iterators.length; ++i) {
        if (i == currentIndex) {
          currentIndex = -1;
          continue;
        }

        SkippingCloseableIterator<E> iterator = iterators[i];
        if (iterator.position() < targetPosition) {
          iterator.skipTo(targetPosition);
        }

        if (!iterator.hasNext()) {
          exhausted = true;
          return false;
        }

        E value = iterator.next();
        long newPosition = iterator.position();
        nextElements[i] = value;

        if (newPosition != targetPosition + 1) {
          position = newPosition - 1;
          currentIndex = i;
          break;
        }
      }

      if (currentIndex == -1) {
        position = targetPosition + 1;
        pendingResult = combiner.combine(nextElements);
        return true;
      }
    }
  }

  @Override
  public boolean hasNext() {
    if (closed) {
      return false;
    }

    if (pendingResult != null) {
      return true;
    }

    if (exhausted) {
      return false;
    }

    return advanceAligned();
  }

  @Override
  public E next() {
    if (pendingResult == null && !hasNext()) {
      throw new NoSuchElementException();
    }

    E result = pendingResult;
    pendingResult = null;
    return result;
  }

  @Override
  public void close() throws IOException {
    closed = true;
    for (SkippingCloseableIterator<E> iterator : iterators) {
      iterator.close();
    }
  }
}

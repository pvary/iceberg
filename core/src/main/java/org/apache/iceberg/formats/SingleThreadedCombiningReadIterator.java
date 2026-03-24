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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

class SingleThreadedCombiningReadIterator<E> implements CloseableIterator<E> {
  private final List<SkippingCloseableIterator<E>> iterators;
  private final FormatModel.Combiner<E> combiner;
  private final List<E> nextElements;
  private boolean closed = false;
  private boolean exhausted = false;
  private long position = 0L;

  /** Pre-fetched combined result, computed eagerly in {@link #tryAdvance()}. */
  private E pendingResult = null;

  SingleThreadedCombiningReadIterator(
      List<SkippingCloseableIterator<E>> iterators, FormatModel.Combiner<E> combiner) {
    this.iterators = iterators;
    this.combiner = combiner;
    this.nextElements = Lists.newArrayListWithExpectedSize(iterators.size());
    for (int i = 0; i < iterators.size(); ++i) {
      nextElements.add(null);
    }
  }

  /**
   * Attempts to compute the next aligned, combined result from all iterators. Returns {@code true}
   * if a result was produced and stored in {@link #pendingResult}, {@code false} if any iterator
   * has been exhausted.
   */
  private boolean tryAdvance() {
    if (pendingResult != null) {
      return true;
    }

    if (exhausted) {
      return false;
    }

    int savedIndex = -1;
    while (true) {
      boolean needsRealign = false;

      for (int i = 0; i < iterators.size(); ++i) {
        // If this iterator already provided its value and only doing realignment now, reuse the
        // old value kept in the nextElements list and skip to the next iterator
        if (i == savedIndex) {
          savedIndex = -1;
          continue;
        }

        SkippingCloseableIterator<E> iterator = iterators.get(i);
        // Ensure the iterator is at the pre-read position before calling next()
        if (iterator.position() < position) {
          iterator.skipTo(position);
        }

        if (!iterator.hasNext()) {
          exhausted = true;
          return false;
        }

        E value = iterator.next();
        long newPosition = iterator.position();

        if (newPosition > position + 1) {
          // This iterator jumped ahead — keep its value in nextElements and discard
          // results from previous iterators, then restart from the first iterator
          nextElements.set(i, value);
          position = newPosition - 1;
          savedIndex = i;
          needsRealign = true;
          break;
        }

        nextElements.set(i, value);
      }

      if (needsRealign) {
        continue;
      }

      position++;
      pendingResult = combiner.combine(nextElements);
      return true;
    }
  }

  @Override
  public boolean hasNext() {
    if (closed) {
      return false;
    }

    return tryAdvance();
  }

  @Override
  public E next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    E result = pendingResult;
    pendingResult = null;
    return result;
  }

  @Override
  public void close() throws IOException {
    closed = true;
  }
}

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

import static org.apache.iceberg.formats.FormatModel.DEFAULT_BATCH_SIZE;
import static org.apache.iceberg.formats.FormatModel.DEFAULT_QUEUE_CAPACITY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.SkippingCloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.FieldSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestColumnSplitReadBuilder {

  @SuppressWarnings("unused")
  private static final Arguments[] MULTI_THREADED_AND_REUSE =
      new Arguments[] {
        Arguments.of(false, false),
        Arguments.of(false, true),
        Arguments.of(true, false),
        Arguments.of(true, true)
      };

  /**
   * A test SkippingCloseableIterator backed by an array. Follows the same position convention as
   * SkippingCloseableIterator.wrap: position starts at 0 and increments on each next() call.
   * skipTo(N) advances position to N by consuming elements.
   */
  private static class TestSkippingIterator implements SkippingCloseableIterator<Integer> {
    private final Integer[] values;
    private long pos = 0;
    private boolean closed = false;

    TestSkippingIterator(Integer... values) {
      this.values = values;
    }

    @Override
    public void skipTo(long targetPosition) {
      if (targetPosition > pos) {
        pos = targetPosition;
      }
    }

    @Override
    public long position() {
      return pos;
    }

    @Override
    public boolean hasNext() {
      return pos < values.length;
    }

    @Override
    public Integer next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      Integer value = values[(int) pos];
      pos++;
      return value;
    }

    @Override
    public void close() {
      closed = true;
    }

    boolean isClosed() {
      return closed;
    }
  }

  /**
   * A test SkippingCloseableIterator that skips certain positions (simulating delete files). When
   * next() is called, it advances past any positions in the skip set. Position convention matches
   * SkippingCloseableIterator.wrap: starts at 0, increments on next().
   */
  private static class SkippingTestIterator implements SkippingCloseableIterator<Integer> {
    private final Integer[] values;
    private final boolean[] skipped;
    private long pos = 0;

    SkippingTestIterator(Integer[] values, long... skipPositions) {
      this.values = values;
      this.skipped = new boolean[values.length];
      for (long skipPos : skipPositions) {
        if (skipPos >= 0 && skipPos < values.length) {
          skipped[(int) skipPos] = true;
        }
      }
    }

    @Override
    public void skipTo(long targetPosition) {
      if (targetPosition > pos) {
        pos = targetPosition;
      }
    }

    @Override
    public long position() {
      return pos;
    }

    @Override
    public boolean hasNext() {
      long nextPos = pos;
      while (nextPos < values.length && skipped[(int) nextPos]) {
        nextPos++;
      }

      return nextPos < values.length;
    }

    @Override
    public Integer next() {
      // Skip over deleted positions
      while (pos < values.length && skipped[(int) pos]) {
        pos++;
      }

      if (pos >= values.length) {
        throw new NoSuchElementException();
      }

      Integer value = values[(int) pos];
      pos++;
      return value;
    }

    @Override
    public void close() {}
  }

  /** Wraps a SkippingCloseableIterator into a CloseableIterable for use with combiner. */
  private static <T> CloseableIterable<T> iterableOf(SkippingCloseableIterator<T> iter) {
    return new CloseableIterable<>() {
      @Override
      public CloseableIterator<T> iterator() {
        return iter;
      }

      @Override
      public void close() throws IOException {
        iter.close();
      }
    };
  }

  /** Simple combiner that sums all elements. */
  private static final FormatModel.Combiner<Integer> SUM_COMBINER =
      elements -> {
        int sum = 0;
        for (Integer e : elements) {
          sum += e;
        }
        return sum;
      };

  /** Combiner that concatenates elements as a list string for traceability. */
  private static final FormatModel.Combiner<Integer> FIRST_COMBINER = elements -> elements.get(0);

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testBasicCombiningTwoAlignedIterators(boolean multiThreaded) throws IOException {
    TestSkippingIterator iter1 = new TestSkippingIterator(10, 20, 30);
    TestSkippingIterator iter2 = new TestSkippingIterator(1, 2, 3);

    try (CloseableIterable<Integer> result =
        ColumnSplitReadBuilder.combiner(
            ImmutableList.of(iterableOf(iter1), iterableOf(iter2)),
            SUM_COMBINER,
            multiThreaded,
            false,
            DEFAULT_BATCH_SIZE,
            DEFAULT_QUEUE_CAPACITY)) {
      List<Integer> collected = Lists.newArrayList(result.iterator());
      assertThat(collected).containsExactly(11, 22, 33);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSingleIterator(boolean multiThreaded) throws IOException {
    TestSkippingIterator iter = new TestSkippingIterator(5, 10, 15);

    try (CloseableIterable<Integer> result =
        ColumnSplitReadBuilder.combiner(
            ImmutableList.of(iterableOf(iter)),
            FIRST_COMBINER,
            multiThreaded,
            false,
            DEFAULT_BATCH_SIZE,
            DEFAULT_QUEUE_CAPACITY)) {
      List<Integer> collected = Lists.newArrayList(result.iterator());
      assertThat(collected).containsExactly(5, 10, 15);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testEmptyIterators(boolean multiThreaded) throws IOException {
    TestSkippingIterator iter1 = new TestSkippingIterator();
    TestSkippingIterator iter2 = new TestSkippingIterator();

    try (CloseableIterable<Integer> result =
        ColumnSplitReadBuilder.combiner(
            ImmutableList.of(iterableOf(iter1), iterableOf(iter2)),
            SUM_COMBINER,
            multiThreaded,
            false,
            DEFAULT_BATCH_SIZE,
            DEFAULT_QUEUE_CAPACITY)) {
      CloseableIterator<Integer> iterator = result.iterator();
      assertThat(iterator.hasNext()).isFalse();
    }
  }

  @SuppressWarnings("AssertThatThrownByWithMessageCheck")
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testNoSuchElementOnExhaustedIterator(boolean multiThreaded) throws IOException {
    TestSkippingIterator iter = new TestSkippingIterator(1);

    try (CloseableIterable<Integer> result =
        ColumnSplitReadBuilder.combiner(
            ImmutableList.of(iterableOf(iter)),
            FIRST_COMBINER,
            multiThreaded,
            false,
            DEFAULT_BATCH_SIZE,
            DEFAULT_QUEUE_CAPACITY)) {
      CloseableIterator<Integer> iterator = result.iterator();
      assertThat(iterator.next()).isEqualTo(1);
      assertThat(iterator.hasNext()).isFalse();
      assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testRealignmentWhenFirstIteratorSkips(boolean multiThreaded) throws IOException {
    // iter1 skips position 0, so its first next() returns value at position 1
    SkippingTestIterator iter1 = new SkippingTestIterator(new Integer[] {10, 20, 30, 40}, 0);
    // iter2 has no skips
    TestSkippingIterator iter2 = new TestSkippingIterator(1, 2, 3, 4);

    try (CloseableIterable<Integer> result =
        ColumnSplitReadBuilder.combiner(
            ImmutableList.of(iterableOf(iter1), iterableOf(iter2)),
            SUM_COMBINER,
            multiThreaded,
            false,
            DEFAULT_BATCH_SIZE,
            DEFAULT_QUEUE_CAPACITY)) {
      List<Integer> collected = Lists.newArrayList(result.iterator());
      // Position 0 is skipped by iter1, so the first combined result should be at position 1
      // iter1 at pos 1 = 20, iter2 at pos 1 = 2 => 22
      // iter1 at pos 2 = 30, iter2 at pos 2 = 3 => 33
      // iter1 at pos 3 = 40, iter2 at pos 3 = 4 => 44
      assertThat(collected).containsExactly(22, 33, 44);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testRealignmentWhenSecondIteratorSkips(boolean multiThreaded) throws IOException {
    // iter1 has no skips
    TestSkippingIterator iter1 = new TestSkippingIterator(10, 20, 30, 40);
    // iter2 skips position 0
    SkippingTestIterator iter2 = new SkippingTestIterator(new Integer[] {1, 2, 3, 4}, 0);

    try (CloseableIterable<Integer> result =
        ColumnSplitReadBuilder.combiner(
            ImmutableList.of(iterableOf(iter1), iterableOf(iter2)),
            SUM_COMBINER,
            multiThreaded,
            false,
            DEFAULT_BATCH_SIZE,
            DEFAULT_QUEUE_CAPACITY)) {
      List<Integer> collected = Lists.newArrayList(result.iterator());
      // iter1 reads pos 0 = 10, iter2 skips to pos 1 = 2 => realign
      // After realign: iter1 skips to pos 1 and reads pos 1 = 20, iter2 reads pos 1...
      // but iter2 already consumed pos 1, so it reads pos 2 = 3
      // The realignment should cause iter1 to catch up
      // Expected: positions 1, 2, 3 => (20+2), (30+3), (40+4)
      assertThat(collected).containsExactly(22, 33, 44);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testRealignmentWithMultipleSkips(boolean multiThreaded) throws IOException {
    // iter1 skips positions 0 and 1
    SkippingTestIterator iter1 = new SkippingTestIterator(new Integer[] {10, 20, 30, 40, 50}, 0, 1);
    // iter2 skips position 0
    SkippingTestIterator iter2 = new SkippingTestIterator(new Integer[] {1, 2, 3, 4, 5}, 0);

    try (CloseableIterable<Integer> result =
        ColumnSplitReadBuilder.combiner(
            ImmutableList.of(iterableOf(iter1), iterableOf(iter2)),
            SUM_COMBINER,
            multiThreaded,
            false,
            DEFAULT_BATCH_SIZE,
            DEFAULT_QUEUE_CAPACITY)) {
      List<Integer> collected = Lists.newArrayList(result.iterator());
      // Both skip pos 0, iter1 also skips pos 1. Aligned at pos 2.
      // pos 2: 30+3=33, pos 3: 40+4=44, pos 4: 50+5=55
      assertThat(collected).containsExactly(33, 44, 55);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testThreeIteratorsAligned(boolean multiThreaded) throws IOException {
    TestSkippingIterator iter1 = new TestSkippingIterator(1, 2, 3);
    TestSkippingIterator iter2 = new TestSkippingIterator(10, 20, 30);
    TestSkippingIterator iter3 = new TestSkippingIterator(100, 200, 300);

    try (CloseableIterable<Integer> result =
        ColumnSplitReadBuilder.combiner(
            ImmutableList.of(iterableOf(iter1), iterableOf(iter2), iterableOf(iter3)),
            SUM_COMBINER,
            multiThreaded,
            false,
            DEFAULT_BATCH_SIZE,
            DEFAULT_QUEUE_CAPACITY)) {
      List<Integer> collected = Lists.newArrayList(result.iterator());
      assertThat(collected).containsExactly(111, 222, 333);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testClosePropagation(boolean multiThreaded) throws IOException {
    TestSkippingIterator iter1 = new TestSkippingIterator(1, 2);
    TestSkippingIterator iter2 = new TestSkippingIterator(3, 4);

    CloseableIterable<Integer> result =
        ColumnSplitReadBuilder.combiner(
            ImmutableList.of(iterableOf(iter1), iterableOf(iter2)),
            SUM_COMBINER,
            multiThreaded,
            false,
            DEFAULT_BATCH_SIZE,
            DEFAULT_QUEUE_CAPACITY);

    CloseableIterator<Integer> iterator = result.iterator();
    assertThat(iterator.hasNext()).isTrue();

    result.close();
    assertThat(iter1.isClosed()).isTrue();
    assertThat(iter2.isClosed()).isTrue();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testDifferentLengthIteratorsStopsAtShortest(boolean multiThreaded) throws IOException {
    TestSkippingIterator iter1 = new TestSkippingIterator(1, 2, 3);
    TestSkippingIterator iter2 = new TestSkippingIterator(10, 20);

    try (CloseableIterable<Integer> result =
        ColumnSplitReadBuilder.combiner(
            ImmutableList.of(iterableOf(iter1), iterableOf(iter2)),
            SUM_COMBINER,
            multiThreaded,
            false,
            DEFAULT_BATCH_SIZE,
            DEFAULT_QUEUE_CAPACITY)) {
      List<Integer> collected = Lists.newArrayList(result.iterator());
      // Should stop when the shorter iterator is exhausted
      assertThat(collected).containsExactly(11, 22);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testMiddlePositionSkippedByOneIterator(boolean multiThreaded) throws IOException {
    // iter1 skips position 1 (middle element)
    SkippingTestIterator iter1 = new SkippingTestIterator(new Integer[] {10, 20, 30, 40}, 1);
    TestSkippingIterator iter2 = new TestSkippingIterator(1, 2, 3, 4);

    try (CloseableIterable<Integer> result =
        ColumnSplitReadBuilder.combiner(
            ImmutableList.of(iterableOf(iter1), iterableOf(iter2)),
            SUM_COMBINER,
            multiThreaded,
            false,
            DEFAULT_BATCH_SIZE,
            DEFAULT_QUEUE_CAPACITY)) {
      List<Integer> collected = Lists.newArrayList(result.iterator());
      // pos 0: iter1=10, iter2=1 => 11
      // pos 1: iter1 skips to pos 2 (=30), iter2 needs to realign to pos 2 (=3) => 33
      // pos 3: iter1=40, iter2=4 => 44
      assertThat(collected).containsExactly(11, 33, 44);
    }
  }

  /**
   * A SkippingTestIterator that counts how many times next() is called. Useful for verifying that
   * values from jumped iterators are kept and not re-read.
   */
  private static class CountingSkippingTestIterator implements SkippingCloseableIterator<Integer> {
    private final SkippingTestIterator delegate;
    private int nextCallCount = 0;

    CountingSkippingTestIterator(Integer[] values, long... skipPositions) {
      this.delegate = new SkippingTestIterator(values, skipPositions);
    }

    @Override
    public void skipTo(long targetPosition) {
      delegate.skipTo(targetPosition);
    }

    @Override
    public long position() {
      return delegate.position();
    }

    @Override
    public boolean hasNext() {
      return delegate.hasNext();
    }

    @Override
    public Integer next() {
      nextCallCount++;
      return delegate.next();
    }

    @Override
    public void close() {
      delegate.close();
    }

    int getNextCallCount() {
      return nextCallCount;
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testUnalignedIteratorValueIsKeptNotReRead(boolean multiThreaded) throws IOException {
    // iter1 has no skips — normal iterator
    // iter2 skips position 0 — will jump ahead on first next()
    // iter3 has no skips — normal iterator
    // When iter2 jumps, the combiner should discard iter1's result, keep iter2's value,
    // restart from iter1, and continue through iter3 — without re-reading iter2.
    TestSkippingIterator iter1 = new TestSkippingIterator(10, 20, 30, 40);
    CountingSkippingTestIterator iter2 =
        new CountingSkippingTestIterator(new Integer[] {1, 2, 3, 4}, 0);
    TestSkippingIterator iter3 = new TestSkippingIterator(100, 200, 300, 400);

    try (CloseableIterable<Integer> result =
        ColumnSplitReadBuilder.combiner(
            ImmutableList.of(iterableOf(iter1), iterableOf(iter2), iterableOf(iter3)),
            SUM_COMBINER,
            multiThreaded,
            false,
            DEFAULT_BATCH_SIZE,
            DEFAULT_QUEUE_CAPACITY)) {
      List<Integer> collected = Lists.newArrayList(result.iterator());
      // Position 0 is skipped by iter2, so alignment starts at position 1.
      // pos 1: iter1=20, iter2=2, iter3=200 => 222
      // pos 2: iter1=30, iter2=3, iter3=300 => 333
      // pos 3: iter1=40, iter2=4, iter3=400 => 444
      assertThat(collected).containsExactly(222, 333, 444);

      // iter2.next() should have been called exactly 3 times (once per output row) in
      // single-threaded mode. The first call caused the jump (returned value 2 at position 1→2),
      // and that value was kept — not re-read. Subsequent calls are for positions 2 and 3.
      // In multi-threaded mode, the producer may eagerly read ahead before realignment kicks in,
      // so we only verify the output values.
      if (!multiThreaded) {
        assertThat(iter2.getNextCallCount()).isEqualTo(3);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testHasNextIsIdempotentWithSkips(boolean multiThreaded) throws IOException {
    // iter1 skips position 0, so realignment is needed on the very first access
    SkippingTestIterator iter1 = new SkippingTestIterator(new Integer[] {10, 20, 30}, 0);
    TestSkippingIterator iter2 = new TestSkippingIterator(1, 2, 3);

    try (CloseableIterable<Integer> result =
        ColumnSplitReadBuilder.combiner(
            ImmutableList.of(iterableOf(iter1), iterableOf(iter2)),
            SUM_COMBINER,
            multiThreaded,
            false,
            DEFAULT_BATCH_SIZE,
            DEFAULT_QUEUE_CAPACITY)) {
      CloseableIterator<Integer> iterator = result.iterator();

      // Call hasNext() multiple times — it must be idempotent and not advance state
      assertThat(iterator.hasNext()).isTrue();
      assertThat(iterator.hasNext()).isTrue();
      assertThat(iterator.hasNext()).isTrue();
      assertThat(iterator.next()).isEqualTo(22); // pos 1: 20+2

      assertThat(iterator.hasNext()).isTrue();
      assertThat(iterator.hasNext()).isTrue();
      assertThat(iterator.next()).isEqualTo(33); // pos 2: 30+3

      assertThat(iterator.hasNext()).isFalse();
      assertThat(iterator.hasNext()).isFalse();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testHasNextReturnsFalseWhenSkipExhaustsIterator(boolean multiThreaded) throws IOException {
    // iter1 skips everything except position 3, iter2 only has 2 elements.
    // After realignment to position 3, iter2 is exhausted, so hasNext() must return false.
    SkippingTestIterator iter1 = new SkippingTestIterator(new Integer[] {10, 20, 30, 40}, 0, 1, 2);
    TestSkippingIterator iter2 = new TestSkippingIterator(1, 2);

    try (CloseableIterable<Integer> result =
        ColumnSplitReadBuilder.combiner(
            ImmutableList.of(iterableOf(iter1), iterableOf(iter2)),
            SUM_COMBINER,
            multiThreaded,
            false,
            DEFAULT_BATCH_SIZE,
            DEFAULT_QUEUE_CAPACITY)) {
      CloseableIterator<Integer> iterator = result.iterator();

      // iter1's first element is at position 3, but iter2 only has positions 0-1.
      // After realignment, iter2 cannot advance to position 3, so the result is empty.
      assertThat(iterator.hasNext()).isFalse();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testHasNextWithInterleavedCallsAndMiddleSkip(boolean multiThreaded) throws IOException {
    // iter1 is normal, iter2 skips position 2 (a middle position)
    TestSkippingIterator iter1 = new TestSkippingIterator(10, 20, 30, 40, 50);
    SkippingTestIterator iter2 = new SkippingTestIterator(new Integer[] {1, 2, 3, 4, 5}, 2);

    try (CloseableIterable<Integer> result =
        ColumnSplitReadBuilder.combiner(
            ImmutableList.of(iterableOf(iter1), iterableOf(iter2)),
            SUM_COMBINER,
            multiThreaded,
            false,
            DEFAULT_BATCH_SIZE,
            DEFAULT_QUEUE_CAPACITY)) {
      CloseableIterator<Integer> iterator = result.iterator();

      // pos 0: 10+1=11, pos 1: 20+2=22, then iter2 skips pos 2, realign to pos 3: 40+4=44,
      // pos 4: 50+5=55
      assertThat(iterator.hasNext()).isTrue();
      assertThat(iterator.next()).isEqualTo(11);

      assertThat(iterator.hasNext()).isTrue();
      assertThat(iterator.next()).isEqualTo(22);

      // This is where the skip at position 2 causes realignment.
      // The old hasNext() would have naively returned true without considering alignment,
      // potentially causing issues in next().
      assertThat(iterator.hasNext()).isTrue();
      assertThat(iterator.next()).isEqualTo(44);

      assertThat(iterator.hasNext()).isTrue();
      assertThat(iterator.next()).isEqualTo(55);

      assertThat(iterator.hasNext()).isFalse();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testHasNextWithoutCallingNext(boolean multiThreaded) throws IOException {
    // Verify that calling only hasNext() (never next()) does not throw or loop forever
    SkippingTestIterator iter1 = new SkippingTestIterator(new Integer[] {10, 20, 30}, 0);
    TestSkippingIterator iter2 = new TestSkippingIterator(1, 2, 3);

    try (CloseableIterable<Integer> result =
        ColumnSplitReadBuilder.combiner(
            ImmutableList.of(iterableOf(iter1), iterableOf(iter2)),
            SUM_COMBINER,
            multiThreaded,
            false,
            DEFAULT_BATCH_SIZE,
            DEFAULT_QUEUE_CAPACITY)) {
      CloseableIterator<Integer> iterator = result.iterator();

      // Just check hasNext — the tryAdvance pattern should compute the result and cache it,
      // but repeated hasNext calls must not advance further
      assertThat(iterator.hasNext()).isTrue();
      assertThat(iterator.hasNext()).isTrue();

      // Now consume the cached value
      assertThat(iterator.next()).isEqualTo(22); // pos 1: 20+2
    }
  }

  /**
   * A container that holds a single mutable value, simulating a reusable record returned by a
   * Parquet reader with reuseContainers=true.
   */
  private static class MutableContainer {
    int value;

    MutableContainer(int value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return "MutableContainer{" + value + "}";
    }
  }

  /**
   * A SkippingCloseableIterator that optionally reuses the same MutableContainer instance,
   * simulating how a Parquet reader behaves with reuseContainers=true.
   */
  private static class ReusableContainerIterator
      implements SkippingCloseableIterator<MutableContainer> {
    private final int[] values;
    private final boolean reuseContainers;
    private final MutableContainer reusedContainer;
    private long pos = 0;

    ReusableContainerIterator(boolean reuseContainers, int... values) {
      this.values = values;
      this.reuseContainers = reuseContainers;
      this.reusedContainer = reuseContainers ? new MutableContainer(0) : null;
    }

    @Override
    public void skipTo(long targetPosition) {
      if (targetPosition > pos) {
        pos = targetPosition;
      }
    }

    @Override
    public long position() {
      return pos;
    }

    @Override
    public boolean hasNext() {
      return pos < values.length;
    }

    @Override
    public MutableContainer next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      if (reuseContainers) {
        reusedContainer.value = values[(int) pos];
        pos++;
        return reusedContainer;
      } else {
        MutableContainer container = new MutableContainer(values[(int) pos]);
        pos++;
        return container;
      }
    }

    @Override
    public void close() {}
  }

  @ParameterizedTest
  @FieldSource("MULTI_THREADED_AND_REUSE")
  void testReuseContainers(boolean reuseContainers, boolean multiThreaded) throws IOException {
    // Underlying iterators reuse their container when reuseContainers=true.
    // In multi-threaded mode, combiner.copyInto() snapshots reused containers
    // into pre-allocated batch slots on the producer thread before the source
    // is overwritten.
    ReusableContainerIterator iter1 = new ReusableContainerIterator(reuseContainers, 10, 20, 30);
    ReusableContainerIterator iter2 = new ReusableContainerIterator(reuseContainers, 1, 2, 3);

    // Mimics the real combiner: reuses the combined wrapper in single-threaded mode,
    // clones it in multi-threaded mode. copyInto does shallow copy when reuseContainers=true.
    MutableContainer reusedResult = new MutableContainer(0);
    boolean reuseWrapper = reuseContainers && !multiThreaded;
    FormatModel.Combiner<MutableContainer> combiner =
        new FormatModel.Combiner<>() {
          @Override
          public MutableContainer combine(List<MutableContainer> elements) {
            MutableContainer target = reuseWrapper ? reusedResult : new MutableContainer(0);
            target.value = 0;
            for (MutableContainer e : elements) {
              target.value += e.value;
            }

            return target;
          }

          @Override
          public MutableContainer copyInto(MutableContainer source, MutableContainer target) {
            if (!reuseContainers) {
              return source;
            }

            MutableContainer dest = target != null ? target : new MutableContainer(0);
            dest.value = source.value;
            return dest;
          }
        };

    try (CloseableIterable<MutableContainer> result =
        ColumnSplitReadBuilder.combiner(
            ImmutableList.of(iterableOf(iter1), iterableOf(iter2)),
            combiner,
            multiThreaded,
            reuseContainers,
            DEFAULT_BATCH_SIZE,
            DEFAULT_QUEUE_CAPACITY)) {
      CloseableIterator<MutableContainer> iterator = result.iterator();

      MutableContainer first = iterator.next();
      assertThat(first.value).isEqualTo(11); // 10 + 1

      MutableContainer second = iterator.next();
      assertThat(second.value).isEqualTo(22); // 20 + 2

      MutableContainer third = iterator.next();
      assertThat(third.value).isEqualTo(33); // 30 + 3

      if (reuseWrapper) {
        // Single-threaded + reuse: same wrapper instance
        assertThat(first).isSameAs(reusedResult);
        assertThat(second).isSameAs(reusedResult);
        assertThat(third).isSameAs(reusedResult);
      } else {
        // All other cases: distinct instances with stable values
        assertThat(second).isNotSameAs(first);
        assertThat(third).isNotSameAs(second);
        assertThat(first.value).isEqualTo(11);
        assertThat(second.value).isEqualTo(22);
      }

      assertThat(iterator.hasNext()).isFalse();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testNoReuseContainersReturnsDifferentInstances(boolean multiThreaded) throws IOException {
    // Simulates reuseContainers=false: both the underlying iterators and the combiner create
    // new containers. Each call to next() should return a different object, and previously
    // returned objects should remain unchanged.
    ReusableContainerIterator iter1 = new ReusableContainerIterator(false, 10, 20, 30);
    ReusableContainerIterator iter2 = new ReusableContainerIterator(false, 1, 2, 3);

    // A combiner that creates a new container each time (like MyCombiner with
    // reuseContainers=false)
    FormatModel.Combiner<MutableContainer> cloneCombiner =
        elements -> {
          int sum = 0;
          for (MutableContainer e : elements) {
            sum += e.value;
          }
          return new MutableContainer(sum);
        };

    try (CloseableIterable<MutableContainer> result =
        ColumnSplitReadBuilder.combiner(
            ImmutableList.of(iterableOf(iter1), iterableOf(iter2)),
            cloneCombiner,
            multiThreaded,
            false,
            DEFAULT_BATCH_SIZE,
            DEFAULT_QUEUE_CAPACITY)) {
      CloseableIterator<MutableContainer> iterator = result.iterator();

      MutableContainer first = iterator.next();
      assertThat(first.value).isEqualTo(11);

      MutableContainer second = iterator.next();
      assertThat(second.value).isEqualTo(22);
      // Without reuse, each result is a distinct object
      assertThat(second).isNotSameAs(first);
      // Previously returned instance should still hold its original value
      assertThat(first.value).isEqualTo(11);

      MutableContainer third = iterator.next();
      assertThat(third.value).isEqualTo(33);
      assertThat(third).isNotSameAs(first);
      assertThat(third).isNotSameAs(second);
      // All previous instances retain their values
      assertThat(first.value).isEqualTo(11);
      assertThat(second.value).isEqualTo(22);

      assertThat(iterator.hasNext()).isFalse();
    }
  }
}

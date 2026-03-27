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
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.SkippingCloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Queues;

/**
 * A multi-threaded combining iterator that uses batched {@link
 * java.util.concurrent.ArrayBlockingQueue}s for producer-consumer coordination.
 *
 * <p>Each producer reads elements into fixed-size {@link Batch} objects and puts each full batch
 * into its queue as a single operation. The consumer takes one batch per queue at a time and
 * iterates through elements locally without any queue interaction.
 *
 * <p>This amortizes per-record synchronization: instead of 2N queue operations per record (N {@code
 * put()} + N {@code take()}), the cost is 2N queue operations per <em>batch</em>, reducing lock
 * acquisitions by a factor of the batch size.
 *
 * <p>Producers can read ahead into the queue (up to the configured queue capacity batches),
 * preserving the I/O pipelining that makes multi-threading beneficial. Position realignment on
 * skips is handled through a shared {@code targetPosition} that producers check before each element
 * read.
 */
class MultiThreadedCombiningReadIterator<E> implements CloseableIterator<E> {
  private final int batchSize;
  private final List<SkippingCloseableIterator<E>> iterators;
  private final FormatModel.Combiner<E> combiner;
  private final ExecutorService executorService;
  private final List<BlockingQueue<Batch<E>>> buffers;

  /**
   * Per-producer queues for returning consumed batches to producers for array reuse. Only populated
   * when {@code reuseContainers} is true — batch recycling is only beneficial when copyInto
   * snapshots reused containers into pre-allocated batch slots.
   */
  private final List<BlockingQueue<Batch<E>>> returnQueues;

  private final List<E> elements;

  /** Shared target position: producers skip elements below this position. */
  private final AtomicLong targetPosition = new AtomicLong(0);

  /** Captures the first exception thrown by any producer thread. */
  private final AtomicReference<Throwable> producerError = new AtomicReference<>(null);

  private volatile boolean closed = false;
  private boolean fetching = false;

  /** Pre-fetched combined result, computed eagerly in {@link #tryAdvance()}. */
  private E pendingResult = null;

  /** Set once any producer's sentinel has been consumed; no more results will be produced. */
  private boolean exhausted = false;

  /** Current batch from each producer. */
  private final Batch<E>[] currentBatches;

  @SuppressWarnings("unchecked")
  MultiThreadedCombiningReadIterator(
      Collection<SkippingCloseableIterator<E>> iterators,
      FormatModel.Combiner<E> combiner,
      int batchSize,
      int queueCapacity,
      boolean reuseContainers) {
    this.batchSize = batchSize;
    this.iterators = Lists.newArrayList(iterators);
    this.combiner = combiner;
    int size = this.iterators.size();
    this.executorService = Executors.newFixedThreadPool(size);
    this.buffers = Lists.newArrayListWithExpectedSize(size);
    this.returnQueues = reuseContainers ? Lists.newArrayListWithExpectedSize(size) : null;
    this.elements = Lists.newArrayListWithExpectedSize(size);
    this.currentBatches = new Batch[size];

    for (int i = 0; i < size; ++i) {
      elements.add(null);
      buffers.add(Queues.newArrayBlockingQueue(queueCapacity));
      if (returnQueues != null) {
        returnQueues.add(Queues.newLinkedBlockingQueue());
      }
    }
  }

  /**
   * Starts one producer thread per iterator. Each producer reads elements into {@link Batch}
   * objects and puts them into its dedicated queue. Producers check the shared {@code
   * targetPosition} before each read to skip ahead when the consumer detects a position gap.
   */
  @SuppressWarnings("CyclomaticComplexity")
  private void startFetching() {
    fetching = true;

    for (int i = 0; i < iterators.size(); i++) {
      final SkippingCloseableIterator<E> iterator = iterators.get(i);
      final BlockingQueue<Batch<E>> buffer = buffers.get(i);
      final BlockingQueue<Batch<E>> returnQueue = returnQueues != null ? returnQueues.get(i) : null;
      executorService.execute(
          () -> {
            try {
              Batch<E> batch = newOrRecycledBatch(returnQueue);

              while (!closed && iterator.hasNext()) {
                long currentTarget = targetPosition.get();

                // Skip ahead if the iterator is behind the shared target position
                if (iterator.position() < currentTarget) {
                  // Flush any partial batch before skipping — the consumer needs
                  // these elements to detect the position gap
                  if (!batch.isEmpty()) {
                    buffer.put(batch);
                    batch = newOrRecycledBatch(returnQueue);
                  }

                  iterator.skipTo(currentTarget);
                }

                if (!iterator.hasNext()) {
                  break;
                }

                E value = iterator.next();
                long pos = iterator.position();

                // Only include if this element is still at or ahead of the target
                if (pos > targetPosition.get()) {
                  if (batch.addCopy(value, pos, combiner)) {
                    buffer.put(batch);
                    batch = newOrRecycledBatch(returnQueue);
                  }
                }
                // Otherwise discard and loop — the target moved while we were reading
              }

              // Flush remaining partial batch
              if (!batch.isEmpty()) {
                buffer.put(batch);
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              producerError.compareAndSet(null, e);
            } catch (Exception e) {
              producerError.compareAndSet(null, e);
            } finally {
              try {
                // Signal completion with a sentinel batch
                buffer.put(Batch.sentinel());
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            }
          });
    }
  }

  /**
   * Returns a recycled batch from the return queue if available, otherwise creates a new one. When
   * recycled, the batch's pre-allocated element slots are preserved for reuse by {@link
   * Batch#addCopy}.
   */
  private Batch<E> newOrRecycledBatch(BlockingQueue<Batch<E>> returnQueue) {
    if (returnQueue != null) {
      Batch<E> recycled = returnQueue.poll();
      if (recycled != null) {
        recycled.reset();
        return recycled;
      }
    }

    return new Batch<>(batchSize);
  }

  private void checkProducerError() {
    Throwable error = producerError.get();
    if (error != null) {
      throw new RuntimeIOException("Error in producer thread: %s", error.getMessage());
    }
  }

  /**
   * Returns the position of the next non-stale element from producer {@code producerIndex},
   * fetching a new batch from the queue if the current one is exhausted. As a side effect, the
   * element value is stored in {@code elements.get(producerIndex)} for later use by the combiner.
   *
   * @param producerIndex the producer index
   * @return the element's position, or {@code -1} if the producer has finished
   * @throws InterruptedException if interrupted while waiting for a batch
   */
  private long nextFromProducer(int producerIndex) throws InterruptedException {
    while (true) {
      Batch<E> batch = currentBatches[producerIndex];
      // If we have a current batch with remaining elements, use it
      if (batch != null && batch.hasRemaining()) {
        long pos = batch.position();

        // Skip stale elements
        if (pos > targetPosition.get()) {
          elements.set(producerIndex, batch.value());
          batch.advance();
          return pos;
        }

        batch.advance();
        continue;
      }

      // Return the exhausted batch for recycling before blocking on the next one,
      // so the producer can reuse it sooner
      if (currentBatches[producerIndex] != null && returnQueues != null) {
        returnQueues.get(producerIndex).offer(currentBatches[producerIndex]);
      }

      // Need a new batch
      Batch<E> newBatch = buffers.get(producerIndex).take();
      if (newBatch.isSentinel()) {
        return -1;
      }

      currentBatches[producerIndex] = newBatch;
    }
  }

  /**
   * Attempts to compute the next aligned, combined result from all producers. Returns {@code true}
   * if a result was produced and stored in {@link #pendingResult}, {@code false} if any producer
   * has finished (sentinel).
   */
  private boolean tryAdvance() {
    if (pendingResult != null) {
      return true;
    }

    if (exhausted) {
      return false;
    }

    checkProducerError();

    if (!fetching) {
      startFetching();
    }

    try {
      int savedIndex = -1;
      while (true) {
        long currentTarget = targetPosition.get();
        boolean needsRealign = false;

        for (int i = 0; i < buffers.size(); i++) {
          // If this buffer already provided its value at the current target, reuse it
          if (i == savedIndex) {
            savedIndex = -1;
            continue;
          }

          long position = nextFromProducer(i);
          if (position < 0) {
            // Producer finished — no more combined results
            exhausted = true;
            return false;
          }

          if (position > currentTarget + 1) {
            // This buffer skipped ahead — keep its value and update the shared target
            // position so producers and subsequent nextFromProducer calls discard stale elements
            targetPosition.set(position - 1);
            savedIndex = i;
            needsRealign = true;
            break;
          }
        }

        if (needsRealign) {
          // Re-fetch elements for buffers before the one that jumped.
          // The saved buffer's element is already correct at the new position.
          continue;
        }

        // All elements are aligned — advance the target position and combine
        targetPosition.incrementAndGet();
        pendingResult = combiner.combine(elements);
        return true;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeIOException("Interrupted while reading: %s", e.getMessage());
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
    executorService.shutdownNow();
  }

  /**
   * A batch of elements with their corresponding positions. Stores data in parallel arrays
   * (struct-of-arrays layout) for better cache locality and to avoid per-element object allocation.
   *
   * <p>The producer fills the batch by calling {@link #addCopy} repeatedly. The consumer reads
   * elements sequentially using {@link #hasRemaining()}, {@link #value()}, {@link #position()}, and
   * {@link #advance()}.
   *
   * <p>A sentinel batch (where {@link #isSentinel()} returns {@code true}) signals that the
   * producer has finished.
   *
   * @param <T> the element type
   */
  private static class Batch<T> {
    private static final Batch<?> SENTINEL = new Batch<>();

    private final Object[] values;
    private final long[] positions;
    private int size;
    private int offset;

    /**
     * Creates an empty batch that can hold up to {@code capacity} elements.
     *
     * @param capacity maximum number of elements this batch can hold
     */
    private Batch(int capacity) {
      this.values = new Object[capacity];
      this.positions = new long[capacity];
      this.size = 0;
      this.offset = 0;
    }

    /** Creates a sentinel batch that signals end-of-stream. */
    private Batch() {
      this.values = null;
      this.positions = null;
      this.size = 0;
      this.offset = 0;
    }

    /**
     * Appends an element to this batch, using {@link FormatModel.Combiner#copyInto} to shallow-copy
     * reused containers into pre-allocated batch slots. When {@code reuseContainers} is false,
     * {@code copyInto} returns the source as-is (no copy). When true, the existing slot value
     * serves as the pre-allocated target for zero-allocation snapshots.
     *
     * @param value the element value (may be a reused container)
     * @param position the iterator position associated with this element
     * @param combiner the combiner providing the copyInto operation
     * @return {@code true} if the batch is full after this add
     */
    @SuppressWarnings("unchecked")
    private boolean addCopy(T value, long position, FormatModel.Combiner<T> combiner) {
      values[size] = combiner.copyInto(value, (T) values[size]);
      positions[size] = position;
      size++;
      return size == values.length;
    }

    /** Resets this batch for reuse, preserving the pre-allocated element slots. */
    private void reset() {
      this.size = 0;
      this.offset = 0;
    }

    /** Returns {@code true} if no elements have been added to this batch. */
    private boolean isEmpty() {
      return size == 0;
    }

    /** Returns a shared sentinel instance that signals end-of-stream. */
    @SuppressWarnings("unchecked")
    private static <T> Batch<T> sentinel() {
      return (Batch<T>) SENTINEL;
    }

    /** Returns {@code true} if this is a sentinel batch signaling end-of-stream. */
    private boolean isSentinel() {
      return values == null;
    }

    /** Returns {@code true} if there are unread elements remaining in this batch. */
    private boolean hasRemaining() {
      return offset < size;
    }

    /** Returns the value of the current element at the read offset. */
    @SuppressWarnings("unchecked")
    private T value() {
      return (T) values[offset];
    }

    /** Returns the position of the current element at the read offset. */
    private long position() {
      return positions[offset];
    }

    /** Advances the read offset to the next element. */
    private void advance() {
      offset++;
    }
  }
}

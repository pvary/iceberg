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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SkippingCloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Interface that provides a unified abstraction for converting between data file formats and
 * input/output data representations.
 *
 * <p>{@link FormatModel} serves as a bridge between storage formats ({@link FileFormat}) and
 * expected input/output data structures, optimizing performance through direct conversion without
 * intermediate representations. File format implementations handle the low-level parsing details
 * while the object model determines the in-memory representation used for the parsed data.
 * Together, these provide a consistent API for consuming data files while optimizing for specific
 * processing engines.
 *
 * <p>Iceberg provides some built-in object models and processing engines can implement custom
 * object models to integrate with Iceberg's file reading and writing capabilities.
 *
 * @param <D> output type used for reading data, and input type for writing data and deletes
 * @param <S> the type of the schema for the input/output data
 */
public interface FormatModel<D, S> {
  /** The file format which is read/written by the object model. */
  FileFormat format();

  /**
   * Return the row type class for the object model implementation processed by this factory.
   *
   * <p>The model types act as a contract specifying the expected data structures for both reading
   * (converting file formats into output objects) and writing (converting input objects into file
   * formats). This ensures proper integration between Iceberg's storage layer and processing
   * engines.
   *
   * <p>Processing engines can define their own object models by implementing this interface and
   * using their own model name. They can register these models with Iceberg by using the {@link
   * FormatModelRegistry}. This allows custom data representations to be seamlessly integrated with
   * Iceberg's file format handlers.
   *
   * @return the type of the data structures handled by this model implementation
   */
  Class<? extends D> type();

  /**
   * Return the schema type class for the object model implementation processed by this factory.
   *
   * @return the type of the schema for the data structures handled by this model implementation
   */
  Class<S> schemaType();

  /**
   * Creates a writer builder for data files.
   *
   * <p>The returned {@link ModelWriteBuilder} configures and creates a writer that converts input
   * objects into the file format supported by this factory.
   *
   * @param outputFile destination for the written data
   * @return configured writer builder
   */
  ModelWriteBuilder<D, S> writeBuilder(EncryptedOutputFile outputFile);

  /**
   * Creates a file reader builder for the specified input file.
   *
   * <p>The returned {@link ReadBuilder} configures and creates a reader that converts data from the
   * file format into output objects supported by this factory.
   *
   * @param inputFile source file to read from
   * @return configured reader builder for the specified input
   */
  ReadBuilder<D, S> readBuilder(InputFile inputFile);

  interface Combiner<E> {
    E combine(List<E> elements);
  }

  default BiFunction<Schema, Integer[][], Combiner<D>> combiner() {
    throw new UnsupportedOperationException("Not implemented");
  }

  static <E> CloseableIterable<E> combiner(
      Collection<CloseableIterable<E>> iterable, Combiner<E> combiner, boolean multiThreaded) {
    List<SkippingCloseableIterator<E>> iterators =
        iterable.stream()
            .map(
                ci -> {
                  CloseableIterator<E> iterator = ci.iterator();
                  if (iterator instanceof SkippingCloseableIterator<E>) {
                    return (SkippingCloseableIterator<E>) iterator;
                  } else {
                    return SkippingCloseableIterator.wrap(iterator);
                  }
                })
            .toList();
    CloseableIterator<E> combined =
        multiThreaded
            ? new MultiThreadedCombiningReadIterator<>(iterators, combiner)
            : new SingleThreadedCombiningReadIterator<>(iterators, combiner);
    return CloseableIterable.combine(
        () -> combined,
        () -> {
          combined.close();
          for (CloseableIterable<E> inner : iterable) {
            inner.close();
          }
        });
  }

  class SingleThreadedCombiningReadIterator<E> implements CloseableIterator<E> {
    private final List<SkippingCloseableIterator<E>> iterators;
    private final Combiner<E> combiner;
    private final List<E> nextElements;
    private boolean closed = false;
    private boolean exhausted = false;
    private long position = 0L;

    /** Pre-fetched combined result, computed eagerly in {@link #tryAdvance()}. */
    private E pendingResult = null;

    private SingleThreadedCombiningReadIterator(
        List<SkippingCloseableIterator<E>> iterators, Combiner<E> combiner) {
      this.iterators = iterators;
      this.combiner = combiner;
      this.nextElements = Lists.newArrayListWithExpectedSize(iterators.size());
      for (int i = 0; i < iterators.size(); ++i) {
        nextElements.add(null);
      }
    }

    /**
     * Attempts to compute the next aligned, combined result from all iterators. Returns {@code
     * true} if a result was produced and stored in {@link #pendingResult}, {@code false} if any
     * iterator has been exhausted.
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

  /**
   * An immutable pair of a value and the position of the iterator at the time it was produced.
   * Position represents the iterator's position <em>after</em> calling {@code next()}, so element
   * at position {@code p} was the {@code p}-th element returned (1-based).
   *
   * <p>A sentinel instance with position {@code -1} signals that the producer has finished.
   */
  record PositionedElement<E>(long position, E value) {
    private static final long SENTINEL_POSITION = -1L;
    private static final PositionedElement<?> SENTINEL =
        new PositionedElement<>(SENTINEL_POSITION, null);

    @SuppressWarnings("unchecked")
    static <E> PositionedElement<E> sentinel() {
      return (PositionedElement<E>) SENTINEL;
    }

    boolean isSentinel() {
      return position == SENTINEL_POSITION;
    }
  }

  class MultiThreadedCombiningReadIterator<E> implements CloseableIterator<E> {
    private static final int BUFFER_CAPACITY = 128;

    private final List<SkippingCloseableIterator<E>> iterators;
    private final Combiner<E> combiner;
    private final ExecutorService executorService;
    private final List<BlockingQueue<PositionedElement<E>>> buffers;
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

    private MultiThreadedCombiningReadIterator(
        Collection<SkippingCloseableIterator<E>> iterators, Combiner<E> combiner) {
      this.iterators = Lists.newArrayList(iterators);
      this.combiner = combiner;
      this.executorService = Executors.newFixedThreadPool(iterators.size());
      this.buffers = Lists.newArrayListWithExpectedSize(this.iterators.size());
      this.elements = Lists.newArrayListWithExpectedSize(this.iterators.size());

      for (int i = 0; i < this.iterators.size(); ++i) {
        elements.add(null);
      }

      for (int idx = 0; idx < this.iterators.size(); idx++) {
        this.buffers.add(new ArrayBlockingQueue<>(BUFFER_CAPACITY));
      }
    }

    private void startFetching() {
      fetching = true;

      for (int i = 0; i < iterators.size(); i++) {
        final SkippingCloseableIterator<E> iterator = iterators.get(i);
        final BlockingQueue<PositionedElement<E>> buffer = buffers.get(i);
        executorService.execute(
            () -> {
              try {
                while (!closed && iterator.hasNext()) {
                  long currentTarget = targetPosition.get();

                  // Skip ahead if the iterator is behind the shared target position
                  if (iterator.position() < currentTarget) {
                    iterator.skipTo(currentTarget);
                  }

                  if (!iterator.hasNext()) {
                    break;
                  }

                  E value = iterator.next();
                  long pos = iterator.position();

                  // Only enqueue if this element is still at or ahead of the target
                  if (pos > targetPosition.get()) {
                    buffer.put(new PositionedElement<>(pos, value));
                  }
                  // Otherwise discard and loop — the target moved while we were reading
                }

                // Signal completion with a sentinel
                buffer.put(PositionedElement.sentinel());
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                producerError.compareAndSet(null, e);
              } catch (Exception e) {
                producerError.compareAndSet(null, e);
              }
            });
      }
    }

    private void checkProducerError() {
      Throwable error = producerError.get();
      if (error != null) {
        throw new RuntimeIOException("Error in producer thread: %s", error.getMessage());
      }
    }

    /**
     * Takes the next aligned element from the given blocking queue, consuming and discarding any
     * elements whose position is below the current target position. Returns {@code null} if the
     * producer has finished (sentinel encountered).
     */
    private PositionedElement<E> takeAligned(BlockingQueue<PositionedElement<E>> buffer)
        throws InterruptedException {
      while (true) {
        PositionedElement<E> element = buffer.take();
        if (element.isSentinel()) {
          return null;
        }

        if (element.position() > targetPosition.get()) {
          return element;
        }

        // Discard stale element and try the next one
      }
    }

    /**
     * Attempts to compute the next aligned, combined result from all buffers. Returns {@code true}
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

            PositionedElement<E> pe = takeAligned(buffers.get(i));
            if (pe == null) {
              // Producer finished — no more combined results
              exhausted = true;
              return false;
            }

            elements.set(i, pe.value());
            long pePosition = pe.position();

            if (pePosition > currentTarget + 1) {
              // This buffer skipped ahead — keep its value and update the shared target
              // position so producers and subsequent takeAligned calls discard stale elements
              targetPosition.set(pePosition - 1);
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
  }
}

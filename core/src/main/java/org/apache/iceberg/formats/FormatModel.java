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
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SkippingCloseableIterator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Queues;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  Logger LOG = LoggerFactory.getLogger(FormatModel.class);

  /** Property key to enable multi-threaded reading and writing for column splits. */
  String MULTI_THREADED = "multi-threaded";

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

  interface Narrower<E> {
    E narrow(E elements);
  }

  default BiFunction<Schema, Integer[][], Combiner<D>> combiner() {
    throw new UnsupportedOperationException("Not implemented");
  }

  default BiFunction<Schema, Integer[], Narrower<D>> narrower() {
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
   * A batch of elements with their corresponding positions. Stores data in parallel arrays
   * (struct-of-arrays layout) for better cache locality and to avoid per-element object allocation.
   *
   * <p>The producer fills the batch by calling {@link #add(Object, long)} repeatedly, then calls
   * {@link #seal()} to prepare it for reading. The consumer reads elements sequentially using
   * {@link #hasRemaining()}, {@link #value()}, {@link #position()}, and {@link #advance()}.
   *
   * <p>A sentinel batch (where {@link #isSentinel()} returns {@code true}) signals that the
   * producer has finished.
   *
   * @param <E> the element type
   */
  class Batch<E> {
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
    Batch(int capacity) {
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
     * Appends an element to this batch.
     *
     * @param value the element value
     * @param position the iterator position associated with this element
     * @return {@code true} if the batch is full after this add
     */
    boolean add(E value, long position) {
      values[size] = value;
      positions[size] = position;
      size++;
      return size == values.length;
    }

    /** Returns {@code true} if no elements have been added to this batch. */
    boolean isEmpty() {
      return size == 0;
    }

    /**
     * Prepares this batch for reading by resetting the read offset to the beginning. Must be called
     * after the producer has finished adding elements and before the batch is handed to the
     * consumer.
     */
    void seal() {
      this.offset = 0;
    }

    /** Returns a shared sentinel instance that signals end-of-stream. */
    @SuppressWarnings("unchecked")
    static <E> Batch<E> sentinel() {
      return (Batch<E>) SENTINEL;
    }

    /** Returns {@code true} if this is a sentinel batch signaling end-of-stream. */
    boolean isSentinel() {
      return values == null;
    }

    /** Returns {@code true} if there are unread elements remaining in this batch. */
    boolean hasRemaining() {
      return offset < size;
    }

    /** Returns the value of the current element at the read offset. */
    @SuppressWarnings("unchecked")
    E value() {
      return (E) values[offset];
    }

    /** Returns the position of the current element at the read offset. */
    long position() {
      return positions[offset];
    }

    /** Advances the read offset to the next element. */
    void advance() {
      offset++;
    }
  }

  /**
   * A multi-threaded combining iterator that uses batched {@link ArrayBlockingQueue}s for
   * producer-consumer coordination.
   *
   * <p>Each producer reads elements into fixed-size {@link Batch} objects and puts each full batch
   * into its queue as a single operation. The consumer takes one batch per queue at a time and
   * iterates through elements locally without any queue interaction.
   *
   * <p>This amortizes per-record synchronization: instead of 2N queue operations per record (N
   * {@code put()} + N {@code take()}), the cost is 2N queue operations per <em>batch</em>, reducing
   * lock acquisitions by a factor of {@link #BATCH_SIZE}.
   *
   * <p>Producers can read ahead into the queue (up to {@link #QUEUE_CAPACITY} batches), preserving
   * the I/O pipelining that makes multi-threading beneficial. Position realignment on skips is
   * handled through a shared {@code targetPosition} that producers check before each element read.
   */
  class MultiThreadedCombiningReadIterator<E> implements CloseableIterator<E> {
    /**
     * Number of elements per batch. Each queue {@code put()}/{@code take()} transfers this many
     * elements, reducing lock acquisitions by this factor compared to per-element queuing.
     */
    private static final int BATCH_SIZE = 1024;

    /**
     * Number of batches each queue can hold. Total buffered elements = BATCH_SIZE * QUEUE_CAPACITY.
     */
    private static final int QUEUE_CAPACITY = 16;

    private final List<SkippingCloseableIterator<E>> iterators;
    private final Combiner<E> combiner;
    private final ExecutorService executorService;
    private final List<BlockingQueue<Batch<E>>> buffers;
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
    private MultiThreadedCombiningReadIterator(
        Collection<SkippingCloseableIterator<E>> iterators, Combiner<E> combiner) {
      this.iterators = Lists.newArrayList(iterators);
      this.combiner = combiner;
      int size = this.iterators.size();
      this.executorService = Executors.newFixedThreadPool(size);
      this.buffers = Lists.newArrayListWithExpectedSize(size);
      this.elements = Lists.newArrayListWithExpectedSize(size);
      this.currentBatches = new Batch[size];

      for (int i = 0; i < size; ++i) {
        elements.add(null);
        buffers.add(Queues.newArrayBlockingQueue(QUEUE_CAPACITY));
      }
    }

    /**
     * Starts one producer thread per iterator. Each producer reads elements into {@link Batch}
     * objects and puts them into its dedicated queue. Producers check the shared {@code
     * targetPosition} before each read to skip ahead when the consumer detects a position gap.
     */
    private void startFetching() {
      fetching = true;

      for (int i = 0; i < iterators.size(); i++) {
        final SkippingCloseableIterator<E> iterator = iterators.get(i);
        final BlockingQueue<Batch<E>> buffer = buffers.get(i);
        executorService.execute(
            () -> {
              try {
                Batch<E> batch = new Batch<>(BATCH_SIZE);

                while (!closed && iterator.hasNext()) {
                  long currentTarget = targetPosition.get();

                  // Skip ahead if the iterator is behind the shared target position
                  if (iterator.position() < currentTarget) {
                    // Flush any partial batch before skipping — the consumer needs
                    // these elements to detect the position gap
                    if (!batch.isEmpty()) {
                      batch.seal();
                      buffer.put(batch);
                      batch = new Batch<>(BATCH_SIZE);
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
                    if (batch.add(value, pos)) {
                      batch.seal();
                      buffer.put(batch);
                      batch = new Batch<>(BATCH_SIZE);
                    }
                  }
                  // Otherwise discard and loop — the target moved while we were reading
                }

                // Flush remaining partial batch
                if (!batch.isEmpty()) {
                  batch.seal();
                  buffer.put(batch);
                }

                // Signal completion with a sentinel batch
                buffer.put(Batch.sentinel());
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
     * Returns the position of the next non-stale element from producer {@code i}, fetching a new
     * batch from the queue if the current one is exhausted. As a side effect, the element value is
     * stored in {@code elements.get(i)} for later use by the combiner.
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

        // Need a new batch
        Batch<E> newBatch = buffers.get(producerIndex).take();
        if (newBatch.isSentinel()) {
          return -1;
        }

        currentBatches[producerIndex] = newBatch;
      }
    }

    /**
     * Attempts to compute the next aligned, combined result from all producers. Returns {@code
     * true} if a result was produced and stored in {@link #pendingResult}, {@code false} if any
     * producer has finished (sentinel).
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

            long pePosition = nextFromProducer(i);
            if (pePosition < 0) {
              // Producer finished — no more combined results
              exhausted = true;
              return false;
            }

            if (pePosition > currentTarget + 1) {
              // This buffer skipped ahead — keep its value and update the shared target
              // position so producers and subsequent nextFromProducer calls discard stale elements
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

  static <E> FileAppender<E> narrower(
      List<Pair<FileAppender<E>, Narrower<E>>> appenders, boolean multiThreaded) {
    return multiThreaded
        ? new MultiThreadedFileAppender<>(appenders)
        : new SingleThreadedFileAppender<>(appenders);
  }

  /**
   * Base class for narrowing {@link FileAppender} implementations that holds the shared appender
   * list and provides common {@link #metrics()}, {@link #length()}, and {@link #close()} logic.
   *
   * <p>Subclasses only need to implement {@link #add(Object)} and {@link #toString()}. If a
   * subclass needs additional close logic, it should override {@link #close()} and call {@code
   * super.close()} to close the underlying appenders.
   */
  abstract class AbstractNarrowingFileAppender<X> implements FileAppender<X> {
    private final List<Pair<FileAppender<X>, Narrower<X>>> appenders;

    protected AbstractNarrowingFileAppender(List<Pair<FileAppender<X>, Narrower<X>>> appenders) {
      this.appenders = appenders;
    }

    protected List<Pair<FileAppender<X>, Narrower<X>>> appenders() {
      return appenders;
    }

    @Override
    public Metrics metrics() {
      Long rowCount = null;
      Map<Integer, Long> columnSizes = Maps.newHashMap();
      Map<Integer, Long> valueCounts = Maps.newHashMap();
      Map<Integer, Long> nullValueCounts = Maps.newHashMap();
      Map<Integer, Long> nanValueCounts = Maps.newHashMap();
      Map<Integer, ByteBuffer> lowerBounds = Maps.newHashMap();
      Map<Integer, ByteBuffer> upperBounds = Maps.newHashMap();

      for (Pair<FileAppender<X>, Narrower<X>> pair : appenders) {
        Metrics metrics = pair.first().metrics();
        if (metrics.recordCount() != null) {
          if (rowCount == null) {
            rowCount = metrics.recordCount();
          } else {
            Preconditions.checkState(
                rowCount.equals(metrics.recordCount()),
                "Record count mismatch across column split appenders: expected %s but got %s",
                rowCount,
                metrics.recordCount());
          }
        }

        if (metrics.columnSizes() != null) {
          columnSizes.putAll(metrics.columnSizes());
        }

        if (metrics.valueCounts() != null) {
          valueCounts.putAll(metrics.valueCounts());
        }

        if (metrics.nullValueCounts() != null) {
          nullValueCounts.putAll(metrics.nullValueCounts());
        }

        if (metrics.nanValueCounts() != null) {
          nanValueCounts.putAll(metrics.nanValueCounts());
        }

        if (metrics.lowerBounds() != null) {
          lowerBounds.putAll(metrics.lowerBounds());
        }

        if (metrics.upperBounds() != null) {
          upperBounds.putAll(metrics.upperBounds());
        }
      }

      return new Metrics(
          rowCount,
          columnSizes,
          valueCounts,
          nullValueCounts,
          nanValueCounts,
          lowerBounds,
          upperBounds);
    }

    @Override
    public long length() {
      return appenders.stream().mapToLong(pair -> pair.first().length()).sum();
    }

    @Override
    public void close() throws IOException {
      for (Pair<FileAppender<X>, Narrower<X>> pair : appenders) {
        pair.first().close();
      }
    }
  }

  /**
   * A single-threaded {@link FileAppender} that narrows each record using per-writer {@link
   * Narrower}s and delegates the narrowed record to the corresponding underlying appender.
   */
  class SingleThreadedFileAppender<X> extends AbstractNarrowingFileAppender<X> {

    private SingleThreadedFileAppender(List<Pair<FileAppender<X>, Narrower<X>>> appenders) {
      super(appenders);
    }

    @Override
    public void add(X record) {
      appenders().forEach(pair -> pair.first().add(pair.second().narrow(record)));
    }

    @Override
    public String toString() {
      return "SingleThreadedFileAppender{" + "appenders=" + appenders() + '}';
    }
  }

  /**
   * A multi-threaded {@link FileAppender} that narrows each record using per-writer {@link
   * Narrower}s and delegates the narrowed record to the corresponding underlying appender.
   *
   * <p>Each underlying appender runs in its own thread. The producer ({@link #add}) collects
   * records into fixed-size batches and puts each full batch into every consumer's queue as a
   * single operation. Consumers take one batch at a time, apply the narrower, and write every
   * element to the appender without touching the queue.
   *
   * <p>This amortizes per-record synchronization: instead of N queue operations per record (one
   * {@code put()} per consumer), the cost is N queue operations per <em>batch</em>, reducing lock
   * acquisitions by a factor of {@link #BATCH_SIZE}.
   */
  class MultiThreadedFileAppender<X> extends AbstractNarrowingFileAppender<X> {
    /**
     * Number of elements per batch. Each queue {@code put()}/{@code take()} transfers this many
     * elements, reducing lock acquisitions by this factor compared to per-element queuing.
     */
    private static final int BATCH_SIZE = 1024;

    /**
     * Number of batches each queue can hold. Total buffered elements = BATCH_SIZE * QUEUE_CAPACITY.
     */
    private static final int QUEUE_CAPACITY = 16;

    private final ExecutorService executorService;
    private final List<BlockingQueue<X[]>> queues;
    private final CountDownLatch finished;
    private final AtomicReference<Throwable> consumerError = new AtomicReference<>(null);
    private boolean started = false;
    private X[] currentBatch = newBatch();
    private int batchOffset = 0;

    private MultiThreadedFileAppender(List<Pair<FileAppender<X>, Narrower<X>>> appenders) {
      super(appenders);
      this.executorService = Executors.newFixedThreadPool(appenders().size());
      this.queues =
          appenders().stream()
              .map(i -> Queues.<X[]>newArrayBlockingQueue(QUEUE_CAPACITY))
              .collect(Collectors.toList());
      this.finished = new CountDownLatch(appenders().size());
    }

    private void startConsumers() {
      started = true;
      for (int i = 0; i < appenders().size(); i++) {
        final BlockingQueue<X[]> queue = queues.get(i);
        final FileAppender<X> appender = appenders().get(i).first();
        final Narrower<X> narrower = appenders().get(i).second();
        final int index = i;
        executorService.execute(
            () -> {
              try {
                while (true) {
                  for (X element : queue.take()) {
                    if (element == null) {
                      // end of data in this batch
                      return;
                    }

                    appender.add(narrower.narrow(element));
                  }
                }
              } catch (Exception e) {
                consumerError.compareAndSet(null, e);
                LOG.error("Error processing records in appender {}", index, e);
              } finally {
                finished.countDown();
              }
            });
      }
    }

    @Override
    public void add(X record) {
      if (!started) {
        startConsumers();
      }

      checkConsumerError();

      currentBatch[batchOffset++] = record;
      if (batchOffset == BATCH_SIZE) {
        flushBatch();
      }
    }

    private void flushBatch() {
      // Place a null sentinel at the end of a partial batch so consumers know where data ends
      if (batchOffset < BATCH_SIZE) {
        currentBatch[batchOffset] = null;
      }

      queues.forEach(
          q -> {
            try {
              q.put(currentBatch);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new RuntimeException(e);
            }
          });
      currentBatch = newBatch();
      batchOffset = 0;
    }

    @SuppressWarnings("unchecked")
    private X[] newBatch() {
      return (X[]) new Object[BATCH_SIZE];
    }

    private void checkConsumerError() {
      Throwable error = consumerError.get();
      if (error != null) {
        throw new RuntimeException("Consumer thread failed", error);
      }
    }

    @Override
    public void close() throws IOException {
      if (started) {
        flushBatch();

        try {
          finished.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted while waiting for appender to finish", e);
        }

        checkConsumerError();
      }

      super.close();
      executorService.shutdown();
    }

    @Override
    public String toString() {
      return "MultiThreadedFileAppender{" + "appenders=" + appenders() + '}';
    }
  }
}

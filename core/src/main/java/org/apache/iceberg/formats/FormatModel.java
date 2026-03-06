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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SkippingCloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Queues;

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
            .collect(Collectors.toList());
    return CloseableIterable.combine(
        () ->
            multiThreaded
                ? new MultiThreadedCombiningReadIterator<>(iterators, combiner)
                : new SingleThreadedCombiningReadIterator<>(iterators, combiner),
        () -> {
          for (CloseableIterable<E> inner : iterable) {
            inner.close();
          }
        });
  }

  class SingleThreadedCombiningReadIterator<E> implements Iterator<E>, Closeable {
    private final List<SkippingCloseableIterator<E>> iterators;
    private final Combiner<E> combiner;
    private final List<E> nextElements;
    private boolean closed = false;
    private long position = 0L;

    private SingleThreadedCombiningReadIterator(
        List<SkippingCloseableIterator<E>> iterators, Combiner<E> combiner) {
      this.iterators = iterators;
      this.combiner = combiner;
      this.nextElements = Lists.newArrayListWithExpectedSize(iterators.size());
      for (int i = 0; i < iterators.size(); ++i) {
        nextElements.add(null);
      }
    }

    @Override
    public boolean hasNext() {
      if (closed) {
        return false;
      }

      for (Iterator<E> iterator : iterators) {
        if (!iterator.hasNext()) {
          return false;
        }
      }

      return true;
    }

    @Override
    public E next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
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
            throw new NoSuchElementException();
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
        return combiner.combine(nextElements);
      }
    }

    @Override
    public void close() throws IOException {
      closed = true;
    }
  }

  class MultiThreadedCombiningReadIterator<E> implements Iterator<E>, Closeable {
    private final List<Iterator<E>> iterators;
    private final Combiner<E> combiner;
    private final ExecutorService executorService;
    private final List<BlockingQueue<E>> queues;
    private boolean closed = false;
    private boolean fetching = false;

    private MultiThreadedCombiningReadIterator(
        Collection<SkippingCloseableIterator<E>> iterators, Combiner<E> combiner) {
      this.iterators = Lists.newArrayList(iterators);
      this.combiner = combiner;
      this.executorService = Executors.newFixedThreadPool(iterators.size());
      this.queues =
          iterators.stream()
              .map(i -> Queues.<E>newLinkedBlockingDeque(100))
              .collect(Collectors.toList());
    }

    @Override
    public boolean hasNext() {
      if (closed) {
        return false;
      }

      if (!fetching) {
        fetching = true;

        // Start fetching elements from each iterator in parallel
        for (int i = 0; i < iterators.size(); i++) {
          final Iterator<E> iterator = iterators.get(i);
          final BlockingQueue<E> queue = queues.get(i);
          executorService.execute(
              () -> {
                try {
                  while (iterator.hasNext()) {
                    synchronized (iterator) {
                      queue.put(iterator.next());
                    }
                  }
                } catch (Exception e) {
                  throw new RuntimeIOException("Alma %s", e.getMessage());
                }
              });
        }
      }

      // If any iterator doesn't have next, return false
      for (int i = 0; i < iterators.size(); i++) {
        final Iterator<E> iterator = iterators.get(i);
        final BlockingQueue<E> queue = queues.get(i);
        if (!iterator.hasNext() && queue.isEmpty()) {
          synchronized (iterator) {
            if (!iterator.hasNext() && queue.isEmpty()) {
              return false;
            }
          }
        }
      }

      return true;
    }

    @Override
    public E next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      return combiner.combine(
          queues.stream()
              .map(
                  q -> {
                    try {
                      return q.take();
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                      throw new RuntimeIOException("Alma %s", e.getMessage());
                    }
                  })
              .collect(Collectors.toList()));
    }

    @Override
    public void close() throws IOException {
      closed = true;
      executorService.shutdown();
    }
  }
}

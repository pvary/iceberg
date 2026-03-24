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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Queues;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A multi-threaded {@link FileAppender} that narrows each record using per-writer {@link
 * FormatModel.Narrower}s and delegates the narrowed record to the corresponding underlying
 * appender.
 *
 * <p>Each underlying appender runs in its own thread. The producer ({@link #add}) collects records
 * into fixed-size batches and puts each full batch into every consumer's queue as a single
 * operation. Consumers take one batch at a time, apply the narrower, and write every element to the
 * appender without touching the queue.
 *
 * <p>This amortizes per-record synchronization: instead of N queue operations per record (one
 * {@code put()} per consumer), the cost is N queue operations per <em>batch</em>, reducing lock
 * acquisitions by a factor of the batch size.
 */
class MultiThreadedFileAppender<X> extends AbstractNarrowingFileAppender<X> {
  private static final Logger LOG = LoggerFactory.getLogger(MultiThreadedFileAppender.class);

  private final int batchSize;
  private final ExecutorService executorService;
  private final List<BlockingQueue<X[]>> queues;
  private final CountDownLatch finished;
  private final AtomicReference<Throwable> consumerError = new AtomicReference<>(null);
  private boolean started = false;
  private X[] currentBatch;
  private int batchOffset = 0;

  MultiThreadedFileAppender(
      List<Pair<FileAppender<X>, FormatModel.Narrower<X>>> appenders,
      int batchSize,
      int queueCapacity) {
    super(appenders);
    this.batchSize = batchSize;
    this.currentBatch = newBatch();
    this.executorService = Executors.newFixedThreadPool(appenders().size());
    this.queues =
        appenders().stream()
            .map(i -> Queues.<X[]>newArrayBlockingQueue(queueCapacity))
            .collect(Collectors.toList());
    this.finished = new CountDownLatch(appenders().size());
  }

  private void startConsumers() {
    started = true;
    for (int i = 0; i < appenders().size(); i++) {
      final BlockingQueue<X[]> queue = queues.get(i);
      final FileAppender<X> appender = appenders().get(i).first();
      final FormatModel.Narrower<X> narrower = appenders().get(i).second();
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
    if (batchOffset == batchSize) {
      flushBatch();
    }
  }

  private void flushBatch() {
    // Place a null sentinel at the end of a partial batch so consumers know where data ends
    if (batchOffset < batchSize) {
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
    return (X[]) new Object[batchSize];
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

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
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Pair;

/**
 * Base class for narrowing {@link FileAppender} implementations that holds the shared appender list
 * and provides common {@link #metrics()}, {@link #length()}, and {@link #close()} logic.
 *
 * <p>Subclasses only need to implement {@link #add(Object)} and {@link #toString()}. If a subclass
 * needs additional close logic, it should override {@link #close()} and call {@code super.close()}
 * to close the underlying appenders.
 */
abstract class AbstractNarrowingFileAppender<X> implements FileAppender<X> {
  private final List<Pair<FileAppender<X>, FormatModel.Narrower<X>>> appenders;

  protected AbstractNarrowingFileAppender(
      List<Pair<FileAppender<X>, FormatModel.Narrower<X>>> appenders) {
    this.appenders = appenders;
  }

  protected List<Pair<FileAppender<X>, FormatModel.Narrower<X>>> appenders() {
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

    for (Pair<FileAppender<X>, FormatModel.Narrower<X>> pair : appenders) {
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
    for (Pair<FileAppender<X>, FormatModel.Narrower<X>> pair : appenders) {
      pair.first().close();
    }
  }
}

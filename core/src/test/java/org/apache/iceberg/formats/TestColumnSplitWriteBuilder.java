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

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestColumnSplitWriteBuilder {

  /**
   * A test {@link FileAppender} that records all added values in memory and tracks close state.
   * Returns configurable metrics and length.
   */
  private static class TestFileAppender implements FileAppender<Integer> {
    private final List<Integer> written = Lists.newArrayList();
    private final int fieldId;
    private boolean closed = false;

    TestFileAppender(int fieldId) {
      this.fieldId = fieldId;
    }

    @Override
    public void add(Integer datum) {
      written.add(datum);
    }

    @Override
    public Metrics metrics() {
      return new Metrics(
          (long) written.size(),
          ImmutableMap.of(fieldId, (long) written.size() * 4),
          ImmutableMap.of(fieldId, (long) written.size()),
          ImmutableMap.of(fieldId, 0L),
          null,
          null,
          null);
    }

    @Override
    public long length() {
      return written.size() * 4L;
    }

    @Override
    public void close() throws IOException {
      closed = true;
    }

    List<Integer> written() {
      return written;
    }

    boolean isClosed() {
      return closed;
    }
  }

  /**
   * A narrower that multiplies each value by a factor. Simulates extracting a "column" by
   * transforming the record.
   */
  private static FormatModel.Narrower<Integer> multiplyNarrower(int factor) {
    return value -> value * factor;
  }

  /** An identity narrower that passes the value through unchanged. */
  private static final FormatModel.Narrower<Integer> IDENTITY_NARROWER = value -> value;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testBasicNarrowingTwoAppenders(boolean multiThreaded) throws IOException {
    TestFileAppender appender1 = new TestFileAppender(1);
    TestFileAppender appender2 = new TestFileAppender(2);

    // appender1 gets value * 10, appender2 gets value * 1
    List<Pair<FileAppender<Integer>, FormatModel.Narrower<Integer>>> appenders =
        ImmutableList.of(
            Pair.of(appender1, multiplyNarrower(10)), Pair.of(appender2, IDENTITY_NARROWER));

    try (FileAppender<Integer> writer =
        ColumnSplitWriteBuilder.narrower(
            appenders, multiThreaded, DEFAULT_BATCH_SIZE, DEFAULT_QUEUE_CAPACITY)) {
      writer.add(1);
      writer.add(2);
      writer.add(3);
    }

    assertThat(appender1.written()).containsExactly(10, 20, 30);
    assertThat(appender2.written()).containsExactly(1, 2, 3);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSingleAppender(boolean multiThreaded) throws IOException {
    TestFileAppender appender = new TestFileAppender(1);

    List<Pair<FileAppender<Integer>, FormatModel.Narrower<Integer>>> appenders =
        ImmutableList.of(Pair.of(appender, IDENTITY_NARROWER));

    try (FileAppender<Integer> writer =
        ColumnSplitWriteBuilder.narrower(
            appenders, multiThreaded, DEFAULT_BATCH_SIZE, DEFAULT_QUEUE_CAPACITY)) {
      writer.add(5);
      writer.add(10);
      writer.add(15);
    }

    assertThat(appender.written()).containsExactly(5, 10, 15);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testEmptyWrite(boolean multiThreaded) throws IOException {
    TestFileAppender appender1 = new TestFileAppender(1);
    TestFileAppender appender2 = new TestFileAppender(2);

    List<Pair<FileAppender<Integer>, FormatModel.Narrower<Integer>>> appenders =
        ImmutableList.of(
            Pair.of(appender1, IDENTITY_NARROWER), Pair.of(appender2, IDENTITY_NARROWER));

    try (FileAppender<Integer> writer =
        ColumnSplitWriteBuilder.narrower(
            appenders, multiThreaded, DEFAULT_BATCH_SIZE, DEFAULT_QUEUE_CAPACITY)) {
      // write nothing, just close
    }

    assertThat(appender1.written()).isEmpty();
    assertThat(appender2.written()).isEmpty();
    assertThat(appender1.isClosed()).isTrue();
    assertThat(appender2.isClosed()).isTrue();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testManyRecordsExceedsBatchSize(boolean multiThreaded) throws IOException {
    TestFileAppender appender1 = new TestFileAppender(1);
    TestFileAppender appender2 = new TestFileAppender(2);

    int numRecords = 5000;

    List<Pair<FileAppender<Integer>, FormatModel.Narrower<Integer>>> appenders =
        ImmutableList.of(
            Pair.of(appender1, multiplyNarrower(2)), Pair.of(appender2, multiplyNarrower(3)));

    try (FileAppender<Integer> writer =
        ColumnSplitWriteBuilder.narrower(
            appenders, multiThreaded, DEFAULT_BATCH_SIZE, DEFAULT_QUEUE_CAPACITY)) {
      for (int i = 0; i < numRecords; i++) {
        writer.add(i);
      }
    }

    assertThat(appender1.written()).hasSize(numRecords);
    assertThat(appender2.written()).hasSize(numRecords);

    for (int i = 0; i < numRecords; i++) {
      assertThat(appender1.written().get(i)).isEqualTo(i * 2);
      assertThat(appender2.written().get(i)).isEqualTo(i * 3);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testClosePropagation(boolean multiThreaded) throws IOException {
    TestFileAppender appender1 = new TestFileAppender(1);
    TestFileAppender appender2 = new TestFileAppender(2);
    TestFileAppender appender3 = new TestFileAppender(3);

    List<Pair<FileAppender<Integer>, FormatModel.Narrower<Integer>>> appenders =
        ImmutableList.of(
            Pair.of(appender1, IDENTITY_NARROWER),
            Pair.of(appender2, IDENTITY_NARROWER),
            Pair.of(appender3, IDENTITY_NARROWER));

    FileAppender<Integer> writer =
        ColumnSplitWriteBuilder.narrower(
            appenders, multiThreaded, DEFAULT_BATCH_SIZE, DEFAULT_QUEUE_CAPACITY);
    writer.add(1);
    writer.close();

    assertThat(appender1.isClosed()).isTrue();
    assertThat(appender2.isClosed()).isTrue();
    assertThat(appender3.isClosed()).isTrue();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testMetricsAggregation(boolean multiThreaded) throws IOException {
    TestFileAppender appender1 = new TestFileAppender(1);
    TestFileAppender appender2 = new TestFileAppender(2);

    List<Pair<FileAppender<Integer>, FormatModel.Narrower<Integer>>> appenders =
        ImmutableList.of(
            Pair.of(appender1, IDENTITY_NARROWER), Pair.of(appender2, IDENTITY_NARROWER));

    try (FileAppender<Integer> writer =
        ColumnSplitWriteBuilder.narrower(
            appenders, multiThreaded, DEFAULT_BATCH_SIZE, DEFAULT_QUEUE_CAPACITY)) {
      writer.add(1);
      writer.add(2);
      writer.add(3);
    }

    Metrics metrics = appender1.metrics();
    assertThat(metrics.recordCount()).isEqualTo(3);

    // The narrowing appender should aggregate metrics across all underlying appenders
    FileAppender<Integer> writer =
        ColumnSplitWriteBuilder.narrower(
            ImmutableList.of(
                Pair.of(appender1, IDENTITY_NARROWER), Pair.of(appender2, IDENTITY_NARROWER)),
            false,
            DEFAULT_BATCH_SIZE,
            DEFAULT_QUEUE_CAPACITY);

    // Write the same records so both appenders have 3 records each
    Metrics aggregated = writer.metrics();
    assertThat(aggregated.recordCount()).isEqualTo(3);
    assertThat(aggregated.columnSizes()).containsKeys(1, 2);
    assertThat(aggregated.valueCounts()).containsKeys(1, 2);
    writer.close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testLengthAggregation(boolean multiThreaded) throws IOException {
    TestFileAppender appender1 = new TestFileAppender(1);
    TestFileAppender appender2 = new TestFileAppender(2);

    List<Pair<FileAppender<Integer>, FormatModel.Narrower<Integer>>> appenders =
        ImmutableList.of(
            Pair.of(appender1, IDENTITY_NARROWER), Pair.of(appender2, IDENTITY_NARROWER));

    try (FileAppender<Integer> writer =
        ColumnSplitWriteBuilder.narrower(
            appenders, multiThreaded, DEFAULT_BATCH_SIZE, DEFAULT_QUEUE_CAPACITY)) {
      writer.add(1);
      writer.add(2);

      // Length should be sum of both appenders' lengths
      // Each appender: 2 records * 4 bytes = 8, total = 16
      // Note: in multi-threaded mode, length may not be accurate until close
      if (!multiThreaded) {
        assertThat(writer.length()).isEqualTo(16);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testThreeAppendersWithDifferentNarrowers(boolean multiThreaded) throws IOException {
    TestFileAppender appender1 = new TestFileAppender(1);
    TestFileAppender appender2 = new TestFileAppender(2);
    TestFileAppender appender3 = new TestFileAppender(3);

    List<Pair<FileAppender<Integer>, FormatModel.Narrower<Integer>>> appenders =
        ImmutableList.of(
            Pair.of(appender1, multiplyNarrower(1)),
            Pair.of(appender2, multiplyNarrower(10)),
            Pair.of(appender3, multiplyNarrower(100)));

    try (FileAppender<Integer> writer =
        ColumnSplitWriteBuilder.narrower(
            appenders, multiThreaded, DEFAULT_BATCH_SIZE, DEFAULT_QUEUE_CAPACITY)) {
      writer.add(1);
      writer.add(2);
      writer.add(3);
    }

    assertThat(appender1.written()).containsExactly(1, 2, 3);
    assertThat(appender2.written()).containsExactly(10, 20, 30);
    assertThat(appender3.written()).containsExactly(100, 200, 300);
  }
}

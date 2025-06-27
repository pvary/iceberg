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
package org.apache.iceberg.spark.data.parquet;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.deletes.DeleteCounter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.spark.data.SparkParquetReaders;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkParquetReaders;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * A benchmark that evaluates the performance of reading Parquet data with a flat schema using
 * direct and vectorized Parquet readers.
 *
 * <p>To run this benchmark for spark-3.5: <code>
 *   ./gradlew -DsparkVersions=3.5 :iceberg-spark:iceberg-spark-3.5_2.12:jmh
 *       -PjmhIncludeRegex=SparkVectorizedParquetReadersBenchmark
 *       -PjmhOutputPath=benchmark/spark-parquet-vectorized-readers-flat-data-benchmark-result.txt
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 20)
@BenchmarkMode(Mode.SingleShotTime)
public class SparkVectorizedParquetReadersBenchmark {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "longCol", Types.LongType.get()),
          required(2, "intCol", Types.IntegerType.get()),
          required(3, "floatCol", Types.FloatType.get()),
          optional(4, "doubleCol", Types.DoubleType.get()),
          optional(5, "decimalCol", Types.DecimalType.of(20, 5)),
          optional(6, "dateCol", Types.DateType.get()),
          optional(7, "timestampCol", Types.TimestampType.withZone()),
          optional(8, "stringCol", Types.StringType.get()));
  private static final int NUM_RECORDS = 10_000_000;
  private File dataFile;

  @Setup
  public void setupBenchmark() throws IOException {
    dataFile = File.createTempFile("parquet-flat-data-benchmark", ".parquet");
    dataFile.delete();
    List<GenericData.Record> records = RandomData.generateList(SCHEMA, NUM_RECORDS, 0L);
    try (FileAppender<GenericData.Record> writer =
        Parquet.write(Files.localOutput(dataFile)).schema(SCHEMA).named("benchmark").build()) {
      writer.addAll(records);
    }
  }

  @TearDown
  public void tearDownBenchmark() {
    if (dataFile != null) {
      dataFile.delete();
    }
  }

  @Benchmark
  @Threads(1)
  public void readCurrentReader(Blackhole blackhole) throws IOException {
    try (CloseableIterable<InternalRow> rows =
        Parquet.read(Files.localInput(dataFile))
            .project(SCHEMA)
            .createReaderFunc(type -> SparkParquetReaders.buildReader(SCHEMA, type))
            .build()) {

      for (InternalRow internalRow : rows) {
        blackhole.consume(internalRow.isNullAt(0) ? 0 : internalRow.getLong(0));
        blackhole.consume(internalRow.isNullAt(1) ? 0 : internalRow.getInt(1));
        blackhole.consume(internalRow.isNullAt(2) ? 0 : ((long) internalRow.getFloat(2)));
        blackhole.consume(internalRow.isNullAt(3) ? 0 : ((long) internalRow.getDouble(3)));
        blackhole.consume(internalRow.isNullAt(4) ? 0 : internalRow.getDecimal(4, 20, 5));
        blackhole.consume(internalRow.isNullAt(5) ? 0 : internalRow.getInt(5));
        blackhole.consume(internalRow.isNullAt(6) ? 0 : internalRow.getLong(6));
        blackhole.consume(internalRow.isNullAt(7) ? 0 : internalRow.getString(7));
      }
    }
  }

  @Benchmark
  @Threads(1)
  public void readVectorizedToInternalRow(Blackhole blackhole) throws IOException {
    try (CloseableIterable<ColumnarBatch> rows =
        Parquet.read(Files.localInput(dataFile))
            .project(SCHEMA)
            .createBatchedReaderFunc(
                fileSchema ->
                    VectorizedSparkParquetReaders.buildReader(
                        SCHEMA,
                        fileSchema,
                        Map.of(),
                        new SparkDeleteFilter(dataFile.getPath()),
                        Map.of()))
            .build()) {

      for (ColumnarBatch row : rows) {
        Iterator<InternalRow> rowIterator = row.rowIterator();
        while (rowIterator.hasNext()) {
          InternalRow internalRow = rowIterator.next();
          blackhole.consume(internalRow.isNullAt(0) ? 0 : internalRow.getLong(0));
          blackhole.consume(internalRow.isNullAt(1) ? 0 : internalRow.getInt(1));
          blackhole.consume(internalRow.isNullAt(2) ? 0 : ((long) internalRow.getFloat(2)));
          blackhole.consume(internalRow.isNullAt(3) ? 0 : ((long) internalRow.getDouble(3)));
          blackhole.consume(internalRow.isNullAt(4) ? 0 : internalRow.getDecimal(4, 20, 5));
          blackhole.consume(internalRow.isNullAt(5) ? 0 : internalRow.getInt(5));
          blackhole.consume(internalRow.isNullAt(6) ? 0 : internalRow.getLong(6));
          blackhole.consume(internalRow.isNullAt(7) ? 0 : internalRow.getString(7));
        }
      }
    }
  }

  protected static class SparkDeleteFilter extends DeleteFilter<InternalRow> {

    SparkDeleteFilter(String filePath) {
      super(filePath, List.of(), SCHEMA, SCHEMA, new DeleteCounter(), false);
    }

    @Override
    protected StructLike asStructLike(InternalRow row) {
      return null;
    }

    @Override
    protected InputFile getInputFile(String location) {
      return null;
    }

    @Override
    protected void markRowDeleted(InternalRow row) {}

    @Override
    protected DeleteLoader newDeleteLoader() {
      return null;
    }
  }
}

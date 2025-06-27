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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.orc.OrcBatchReader;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkOrcReaders;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.DateColumnVector;
import org.apache.orc.storage.ql.exec.vector.DecimalColumnVector;
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.TimestampColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
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
 * Iceberg and Spark Parquet readers.
 *
 * <p>To run this benchmark for spark-3.5: <code>
 *   ./gradlew -DsparkVersions=3.5 :iceberg-spark:iceberg-spark-3.5_2.12:jmh
 *       -PjmhIncludeRegex=SparkORCReadersFlatDataBenchmark
 *       -PjmhOutputPath=benchmark/spark-orc-readers-flat-data-benchmark-result.txt
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 20)
@BenchmarkMode(Mode.SingleShotTime)
public class SparkORCReadersFlatDataBenchmark {
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
  private FileIO fileIO;
  private static final String FILE_NAME = "data.orc";

  @Setup
  public void setupBenchmark() throws IOException {
    fileIO = new InMemoryFileIO();

    List<Record> records = RandomGenericData.generate(SCHEMA, NUM_RECORDS, 0L);
    try (FileAppender<Record> writer =
        ORC.write(fileIO.newOutputFile(FILE_NAME))
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .schema(SCHEMA)
            .build()) {
      writer.addAll(records);
    }
  }

  @TearDown
  public void tearDownBenchmark() {
    fileIO.deleteFile(FILE_NAME);
  }

  @Benchmark
  @Threads(1)
  public void readVectorized(Blackhole blackHole) throws IOException {
    try (CloseableIterable<ColumnarBatch> rows =
        ORC.read(fileIO.newInputFile(FILE_NAME))
            .project(SCHEMA)
            .createBatchedReaderFunc(
                fileSchema -> VectorizedSparkOrcReaders.buildReader(SCHEMA, fileSchema, Map.of()))
            .build()) {

      for (ColumnarBatch row : rows) {
        for (int i = 0; i < row.numRows(); ++i) {
          blackHole.consume(row.column(0).isNullAt(i) ? 0 : row.column(0).getLong(i));
          blackHole.consume(row.column(1).isNullAt(i) ? 0 : row.column(1).getInt(i));
          blackHole.consume(row.column(2).isNullAt(i) ? 0 : row.column(2).getFloat(i));
          blackHole.consume(row.column(3).isNullAt(i) ? 0 : row.column(3).getDouble(i));
          blackHole.consume(
              row.column(4).isNullAt(i)
                  ? 0
                  : row.column(4).getDecimal(i, 20, 5).toBigDecimal().longValue());
          blackHole.consume(row.column(5).isNullAt(i) ? 0 : row.column(5).getInt(i));
          blackHole.consume(row.column(6).isNullAt(i) ? 0 : row.column(6).getLong(i));
          blackHole.consume(row.column(7).isNullAt(i) ? 0 : row.column(7).getUTF8String(i));
        }
      }
    }
  }

  @Benchmark
  @Threads(1)
  public void readVectorizedWithNoTransform(Blackhole blackHole) throws IOException {
    try (CloseableIterable<VectorizedRowBatch> rows =
        ORC.read(fileIO.newInputFile(FILE_NAME))
            .project(SCHEMA)
            .createBatchedReaderFunc(NoopBatchReader::new)
            .build()) {

      for (VectorizedRowBatch row : rows) {
        for (int i = 0; i < row.count(); ++i) {
          BytesColumnVector bytesVector = (BytesColumnVector) row.cols[7];
          blackHole.consume(row.cols[0].isNull[i] ? 0 : ((LongColumnVector) row.cols[0]).vector[i]);
          blackHole.consume(row.cols[1].isNull[i] ? 0 : ((LongColumnVector) row.cols[1]).vector[i]);
          blackHole.consume(
              row.cols[2].isNull[i] ? 0 : ((DoubleColumnVector) row.cols[2]).vector[i]);
          blackHole.consume(
              row.cols[3].isNull[i] ? 0 : ((DoubleColumnVector) row.cols[3]).vector[i]);
          blackHole.consume(
              row.cols[4].isNull[i] ? 0 : ((DecimalColumnVector) row.cols[4]).vector[i]);
          blackHole.consume(row.cols[5].isNull[i] ? 0 : ((DateColumnVector) row.cols[5]).vector[i]);
          blackHole.consume(
              row.cols[6].isNull[i] ? 0 : ((TimestampColumnVector) row.cols[6]).time[i]);
          blackHole.consume(row.cols[7].isNull[i] ? 0 : bytesVector.vector[i]);
          blackHole.consume(row.cols[7].isNull[i] ? 0 : bytesVector.start[i]);
          blackHole.consume(row.cols[7].isNull[i] ? 0 : bytesVector.length[i]);
        }
      }
    }
  }

  private static class NoopBatchReader implements OrcBatchReader<VectorizedRowBatch> {
    public NoopBatchReader(TypeDescription notUsed) {
      // The TypeDescription is not used in this implementation.
    }

    @Override
    public VectorizedRowBatch read(VectorizedRowBatch batch) {
      return batch;
    }

    @Override
    public void setBatchContext(long batchOffsetInFile) {
      // No-op
    }
  }
}

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
package org.apache.iceberg.flink.data.parquet;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.arrow.vectorized.ColumnarBatch;
import org.apache.iceberg.data.FormatModelRegistry;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.flink.data.FlinkParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.ReadBuilder;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
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
 * <p>To run this benchmark: <code>
 *   ./gradlew -DflinkVersions=2.0 :iceberg-flink:iceberg-flink-2.0:jmh
 *       -PjmhIncludeRegex=FlinkParquetReadersFlatDataBenchmark
 *       -PjmhOutputPath=benchmark/flink-parquet-vectorized-readers-flat-data-benchmark-result.txt
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 20)
@BenchmarkMode(Mode.SingleShotTime)
public class FlinkParquetReadersFlatDataBenchmark {
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
  private static final String ARROW_OBJECT_MODEL = "arrow";
  private File dataFile;

  @Setup
  public void setupBenchmark() throws IOException {
    dataFile = File.createTempFile("parquet-flat-data-benchmark", ".parquet");
    dataFile.delete();

    List<Record> records = RandomGenericData.generate(SCHEMA, NUM_RECORDS, 0L);
    try (FileAppender<Record> writer =
        Parquet.write(Files.localOutput(dataFile))
            .createWriterFunc(GenericParquetWriter::create)
            .schema(SCHEMA)
            .build()) {
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
  public void readCurrent(Blackhole blackHole) throws IOException {
    try (CloseableIterable<RowData> rows =
        Parquet.read(Files.localInput(dataFile))
            .project(SCHEMA)
            .createReaderFunc(FlinkParquetReaders::buildReader)
            .reuseContainers()
            .build()) {

      for (RowData row : rows) {
        blackHole.consume(row.isNullAt(0) ? 0 : row.getLong(0));
        blackHole.consume(row.isNullAt(1) ? 0 : row.getInt(1));
        blackHole.consume(row.isNullAt(2) ? 0 : ((long) row.getFloat(2)));
        blackHole.consume(row.isNullAt(3) ? 0 : ((long) row.getDouble(3)));
        blackHole.consume(row.isNullAt(4) ? 0 : row.getDecimal(4, 20, 5));
        blackHole.consume(row.isNullAt(5) ? 0 : row.getInt(5));
        blackHole.consume(row.isNullAt(6) ? 0 : row.getTimestamp(6, 6));
        blackHole.consume(row.isNullAt(7) ? 0 : row.getString(7));
      }
    }
  }

  @Benchmark
  @Threads(1)
  public void readVectorizedRowData(Blackhole blackHole) throws IOException {
    ReadBuilder<?, ColumnarBatch> builder =
        FormatModelRegistry.readBuilder(
            FileFormat.PARQUET, ARROW_OBJECT_MODEL, Files.localInput(dataFile));
    GenericRowData data = GenericRowData.of(SCHEMA.columns().toArray());
    try (CloseableIterable<ColumnarBatch> rows =
        builder.project(SCHEMA).reuseContainers().build()) {

      for (ColumnarBatch row : rows) {
        VectorSchemaRoot vsr = row.createVectorSchemaRootFromVectors();
        for (int i = 0; i < vsr.getRowCount(); i++) {
          data.setField(0, vsr.getVector(0).isNull(i) ? null : vsr.getVector(0).getObject(i));
          data.setField(1, vsr.getVector(1).isNull(i) ? null : vsr.getVector(1).getObject(i));
          data.setField(2, vsr.getVector(2).isNull(i) ? null : vsr.getVector(2).getObject(i));
          data.setField(3, vsr.getVector(3).isNull(i) ? null : vsr.getVector(3).getObject(i));
          data.setField(
              4,
              vsr.getVector(4).isNull(i)
                  ? null
                  : DecimalData.fromBigDecimal(
                      new BigDecimal(new BigInteger((byte[]) vsr.getVector(4).getObject(i)), 5),
                      20,
                      5));
          data.setField(5, vsr.getVector(5).isNull(i) ? null : vsr.getVector(5).getObject(i));
          Long micros = vsr.getVector(6).isNull(i) ? null : (Long) vsr.getVector(6).getObject(i);
          data.setField(
              6,
              micros == null
                  ? null
                  : TimestampData.fromEpochMillis(
                      Math.floorDiv(micros, 1000), Math.floorMod(micros, 1000) * 1000));
          data.setField(
              7,
              vsr.getVector(7).isNull(i)
                  ? null
                  : StringData.fromString(vsr.getVector(7).getObject(i).toString()));
          blackHole.consume(data);
        }
      }
    }
  }
}

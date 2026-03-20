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
package org.apache.iceberg;

import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.data.CombinedRecord;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.formats.FormatModel;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JMH benchmark comparing the performance of {@link
 * FormatModel.SingleThreadedCombiningReadIterator} and {@link
 * FormatModel.MultiThreadedCombiningReadIterator} with actual Parquet file readers.
 *
 * <p>A wide table with 10,000 integer columns is split across 10 Parquet files (1,000 columns
 * each). Each benchmark iteration opens real Parquet readers for all files and combines them using
 * {@link CombinedRecord} via the respective iterator implementation.
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Timeout(time = 1000, timeUnit = TimeUnit.HOURS)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class MultiThreadedParquetBenchmark {
  private static final Logger LOG = LoggerFactory.getLogger(MultiThreadedParquetBenchmark.class);

  /** Number of Parquet files (column families). */
  private static final int NUM_FILES = 10;

  /** Total number of integer columns across all files. */
  private static final int NUM_COLUMNS = 10_000;

  /** Number of columns per file. */
  private static final int COLUMNS_PER_FILE = NUM_COLUMNS / NUM_FILES;

  /** Number of rows per file. */
  private static final int NUM_ROWS = 5_000;

  private static final int SEED = 42;

  /** The full schema spanning all 10,000 columns. */
  private Schema fullSchema;

  /** Per-file sub-schemas, each containing its family's columns. */
  private List<Schema> fileSchemas;

  /** Per-file family arrays (field IDs belonging to each file). */
  private Integer[][] families;

  private List<File> testFiles;

  @Setup
  public void setupBenchmark() throws IOException {
    // Build the full schema with NUM_COLUMNS required integer columns (field IDs 1..NUM_COLUMNS)
    List<Types.NestedField> allFields = Lists.newArrayListWithCapacity(NUM_COLUMNS);
    for (int i = 0; i < NUM_COLUMNS; i++) {
      allFields.add(required(i + 1, "col_" + i, Types.IntegerType.get()));
    }
    fullSchema = new Schema(allFields);

    // Split into NUM_FILES families, each with COLUMNS_PER_FILE columns
    families = new Integer[NUM_FILES][];
    fileSchemas = Lists.newArrayListWithCapacity(NUM_FILES);
    for (int f = 0; f < NUM_FILES; f++) {
      int startCol = f * COLUMNS_PER_FILE;
      Integer[] familyIds = new Integer[COLUMNS_PER_FILE];
      List<Types.NestedField> familyFields = Lists.newArrayListWithCapacity(COLUMNS_PER_FILE);
      for (int c = 0; c < COLUMNS_PER_FILE; c++) {
        int fieldId = startCol + c + 1;
        familyIds[c] = fieldId;
        familyFields.add(required(fieldId, "col_" + (startCol + c), Types.IntegerType.get()));
      }
      families[f] = familyIds;
      fileSchemas.add(new Schema(familyFields));
    }

    // Write NUM_FILES Parquet files, each containing its family's columns
    testFiles = Lists.newArrayListWithCapacity(NUM_FILES);
    for (int f = 0; f < NUM_FILES; f++) {
      File file =
          java.nio.file.Files.createTempFile("combining-bench-" + f + "-", ".parquet").toFile();
      file.delete();
      testFiles.add(file);

      Schema fileSchema = fileSchemas.get(f);
      List<Record> records = RandomGenericData.generate(fileSchema, NUM_ROWS, SEED + f);

      try (FileAppender<Record> writer =
          Parquet.write(Files.localOutput(file))
              .schema(fileSchema)
              .createWriterFunc(GenericParquetWriter::create)
              .build()) {
        writer.addAll(records);
      }

      LOG.info("Wrote file {} ({} bytes)", file.getName(), file.length());
    }

    LOG.info(
        "Setup complete: {} files, {} columns total ({} per file), {} rows per file",
        NUM_FILES,
        NUM_COLUMNS,
        COLUMNS_PER_FILE,
        NUM_ROWS);
  }

  @TearDown
  public void tearDownBenchmark() {
    if (testFiles != null) {
      for (File file : testFiles) {
        file.delete();
      }
    }
  }

  /** Opens a Parquet reader for the given file projected to its family sub-schema. */
  private CloseableIterable<Record> openReader(int fileIndex) {
    File file = testFiles.get(fileIndex);
    Schema fileSchema = fileSchemas.get(fileIndex);
    return Parquet.read(Files.localInput(file))
        .project(fileSchema)
        .createReaderFunc(
            parquetSchema -> GenericParquetReaders.buildReader(fileSchema, parquetSchema))
        .build();
  }

  /** Creates a collection of readers, one per file. */
  private Collection<CloseableIterable<Record>> createReaders() {
    List<CloseableIterable<Record>> readers = Lists.newArrayListWithCapacity(NUM_FILES);
    for (int f = 0; f < NUM_FILES; f++) {
      readers.add(openReader(f));
    }
    return readers;
  }

  /**
   * Combiner that assembles a {@link CombinedRecord} from per-file records. Each element in the
   * input list corresponds to one file's record; the combiner maps it into the appropriate family
   * slot of the output CombinedRecord.
   */
  private FormatModel.Combiner<Record> createCombiner() {
    // Pre-create a template CombinedRecord to clone per row for efficiency
    CombinedRecord template = CombinedRecord.create(fullSchema, families);
    return elements -> {
      CombinedRecord combined = CombinedRecord.clone(template);
      for (int f = 0; f < elements.size(); f++) {
        combined.setFamily(f, elements.get(f));
      }
      return combined;
    };
  }

  @Benchmark
  @Threads(1)
  public void singleThreadedCombiner(Blackhole blackhole) throws IOException {
    FormatModel.Combiner<Record> combiner = createCombiner();
    try (CloseableIterable<Record> result =
        FormatModel.combiner(createReaders(), combiner, false)) {
      for (Record row : result) {
        blackhole.consume(row);
      }
    }
  }

  @Benchmark
  @Threads(1)
  public void multiThreadedCombiner(Blackhole blackhole) throws IOException {
    FormatModel.Combiner<Record> combiner = createCombiner();
    try (CloseableIterable<Record> result = FormatModel.combiner(createReaders(), combiner, true)) {
      for (Record row : result) {
        blackhole.consume(row);
      }
    }
  }
}

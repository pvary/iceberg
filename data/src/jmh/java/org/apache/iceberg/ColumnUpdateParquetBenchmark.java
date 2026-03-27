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

import static org.apache.iceberg.types.Types.NestedField.optional;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.formats.FormatModel;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Benchmark that simulates reading from a base data file with column-level updates. The base file
 * contains 80 int columns and 20 string columns. A configurable number of int columns are
 * "updated", where each update is stored in a separate file containing a single int column. The
 * reader merges the base file (non-updated columns) with the individual update files using column
 * splits.
 */
@Fork(value = 1)
@State(Scope.Benchmark)
@Warmup(iterations = 5)
@Measurement(iterations = 20)
@BenchmarkMode(Mode.SingleShotTime)
public class ColumnUpdateParquetBenchmark {
  private static final Logger LOG = LoggerFactory.getLogger(ColumnUpdateParquetBenchmark.class);

  private static final int SEED = -2;
  private static final int GEN_BATCH_SIZE = 10000;
  private static final int NUM_INT_COLUMNS = 80;
  private static final int NUM_STRING_COLUMNS = 20;
  private static final int TOTAL_COLUMNS = NUM_INT_COLUMNS + NUM_STRING_COLUMNS;
  private static final int NUM_ROWS = 1_000_000;

  private static final String TEST_DIR =
      "/Users/petervary/iceberg-column-update-parquet-benchmark/";
  private static final String BASE_DIR = "base/";
  private static final String UPDATE_DIR = "updates/";

  private Schema fullSchema;
  private String baseFilePath;
  private List<String> updateFilePaths;

  /** Number of int columns that have been updated (each in a separate single-column file). */
  @Param({"0", "1", "2", "3", "4", "5", "10", "40", "80"})
  private int updatedColumns;

  @Param({"true", "false"})
  private boolean multiThreaded;

  @Param("128")
  private int batchSize;

  @Param("4")
  private int queueCapacity;

  {
    Path baseDirPath = Path.of(TEST_DIR, BASE_DIR);
    Path updateDirPath = Path.of(TEST_DIR, UPDATE_DIR);
    try {
      if (!java.nio.file.Files.exists(baseDirPath)) {
        java.nio.file.Files.createDirectories(baseDirPath);
      }
      if (!java.nio.file.Files.exists(updateDirPath)) {
        java.nio.file.Files.createDirectories(updateDirPath);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create directories", e);
    }
  }

  /**
   * When updatedColumns is 0 there is only a single base file, so multiThreaded/batchSize/
   * queueCapacity are irrelevant. We skip all but the first multiThreaded @Param value to avoid
   * redundant runs. Static state cannot be used here because JMH forks a new JVM for each parameter
   * combination.
   */
  private boolean shouldSkip() {
    return updatedColumns == 0 && !multiThreaded;
  }

  @Setup(Level.Trial)
  public void setupBenchmark() throws IOException {
    /* System.err.println(
    "Setup: updatedColumns="
        + updatedColumns
        + ", MT="
        + multiThreaded
        + ", batch="
        + batchSize
        + ", queue="
        + queueCapacity);*/

    if (shouldSkip()) {
      // System.err.println("Skipping benchmark - params irrelevant for updatedColumns=0");
      return;
    }

    // Build the full schema: 80 int columns (ids 0..79) + 20 string columns (ids 80..99)
    List<Types.NestedField> fields = Lists.newArrayListWithCapacity(TOTAL_COLUMNS);
    for (int i = 0; i < NUM_INT_COLUMNS; i++) {
      fields.add(optional(i, "int_col" + i, Types.IntegerType.get()));
    }
    for (int i = 0; i < NUM_STRING_COLUMNS; i++) {
      fields.add(optional(NUM_INT_COLUMNS + i, "str_col" + i, Types.StringType.get()));
    }
    fullSchema = new Schema(fields);

    // Generate the base data file containing all columns
    baseFilePath = TEST_DIR + BASE_DIR + "base_data.parquet";
    initBaseFile();

    // Generate individual update files, one per updated int column
    updateFilePaths = Lists.newArrayListWithCapacity(updatedColumns);
    for (int i = 0; i < updatedColumns; i++) {
      String updateFile = TEST_DIR + UPDATE_DIR + "update_col_" + i + ".parquet";
      updateFilePaths.add(updateFile);
      initUpdateFile(updateFile, i);
    }
  }

  @TearDown
  public void tearDownBenchmark() {
    // Keep generated files to speed up repeated runs
  }

  @Benchmark
  @Threads(1)
  public void readWithColumnUpdates() throws IOException {
    if (shouldSkip()) {
      return;
    }

    long val = 0;

    try (CloseableIterable<Record> reader = buildColumnSplitReader()) {
      for (Record record : reader) {
        // Access something to prevent dead code elimination
        Object col = record.get(0);
        if (col != null) {
          val ^= ((Integer) col).longValue();
        }
      }
    }

    LOG.info("XOR val: {}", val);
  }

  /**
   * Builds a column-split reader that reads non-updated columns from the base file and each updated
   * int column from its own update file.
   */
  private CloseableIterable<Record> buildColumnSplitReader() {
    // The first `updatedColumns` int columns (ids 0..updatedColumns-1) are updated.
    // Base file provides: remaining int columns + all string columns
    List<Integer> baseColumnIds = Lists.newArrayList();
    for (int i = updatedColumns; i < NUM_INT_COLUMNS; i++) {
      baseColumnIds.add(i);
    }
    for (int i = 0; i < NUM_STRING_COLUMNS; i++) {
      baseColumnIds.add(NUM_INT_COLUMNS + i);
    }

    Map<InputFile, List<Integer>> columnSplits = new LinkedHashMap<>();
    columnSplits.put(Files.localInput(baseFilePath), baseColumnIds);

    // Each update file provides a single int column
    for (int i = 0; i < updatedColumns; i++) {
      columnSplits.put(Files.localInput(updateFilePaths.get(i)), Lists.newArrayList(i));
    }

    ReadBuilder<Record, ?> builder =
        FormatModelRegistry.readBuilder(FileFormat.PARQUET, Record.class, columnSplits);
    return builder
        .project(fullSchema)
        .set(FormatModel.MULTI_THREADED, Boolean.toString(multiThreaded))
        .set(FormatModel.QUEUE_CAPACITY, String.valueOf(queueCapacity))
        .set(FormatModel.BATCH_SIZE, String.valueOf(batchSize))
        .build();
  }

  /** Creates the base data file with all 80 int + 20 string columns if it doesn't exist. */
  private void initBaseFile() throws IOException {
    if (!Files.localInput(baseFilePath).exists()) {
      // System.err.println("Generating base data file: " + baseFilePath);
      try (FileAppender<Record> writer =
          Parquet.write(Files.localOutput(baseFilePath))
              .schema(fullSchema)
              .createWriterFunc(GenericParquetWriter::create)
              .build()) {
        for (int i = 0; i < NUM_ROWS; i += GEN_BATCH_SIZE) {
          int batchRows = Math.min(GEN_BATCH_SIZE, NUM_ROWS - i);
          writer.addAll(RandomGenericData.generate(fullSchema, batchRows, SEED + i));
          // System.err.println("Base file progress: " + (i + batchRows) + "/" + NUM_ROWS);
        }
      }
      // System.err.println("Base data file created: " + baseFilePath);
    } /* else {
        System.err.println("Base data file already exists: " + baseFilePath);
      }*/
  }

  /**
   * Creates an update file for a single int column if it doesn't exist. The file contains only one
   * column with the same field id as in the full schema.
   */
  private void initUpdateFile(String filePath, int columnIndex) throws IOException {
    if (!Files.localInput(filePath).exists()) {
      // System.err.println("Generating update file for int_col" + columnIndex + ": " + filePath);
      Schema singleColSchema =
          new Schema(optional(columnIndex, "int_col" + columnIndex, Types.IntegerType.get()));
      try (FileAppender<Record> writer =
          Parquet.write(Files.localOutput(filePath))
              .schema(singleColSchema)
              .createWriterFunc(GenericParquetWriter::create)
              .build()) {
        for (int i = 0; i < NUM_ROWS; i += GEN_BATCH_SIZE) {
          int batchRows = Math.min(GEN_BATCH_SIZE, NUM_ROWS - i);
          writer.addAll(RandomGenericData.generate(singleColSchema, batchRows, SEED + i + 1000));
          /* System.err.println(
          "Update file int_col"
              + columnIndex
              + " progress: "
              + (i + batchRows)
              + "/"
              + NUM_ROWS);*/
        }
      }
      // System.err.println("Update file created: " + filePath);
    } /* else {
        System.err.println("Update file already exists: " + filePath);
      }*/
  }
}

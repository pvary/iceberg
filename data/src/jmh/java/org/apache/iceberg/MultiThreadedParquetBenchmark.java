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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.formats.FileWriterBuilder;
import org.apache.iceberg.formats.FormatModel;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.DataWriter;
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

@Fork(value = 1)
@State(Scope.Benchmark)
@Warmup(iterations = 10)
@Measurement(iterations = 20)
@BenchmarkMode(Mode.SingleShotTime)
public class MultiThreadedParquetBenchmark {
  private static final Logger LOG = LoggerFactory.getLogger(MultiThreadedParquetBenchmark.class);

  private static final int SEED = -2;
  private static final int TEST_BATCH_SIZE = 10000;
  private static final int DATA_SIZE = 100_000_000;
  private static final String TEST_DIR =
      "/Users/petervary/iceberg-generic-parquet-reader-benchmark/";
  private static final String READ_DIR = "read/";
  private static final String WRITE_DIR = "write/";
  private static final String SOURCE = "source/data.parquet";
  private Schema testSchema;
  private List<List<Integer>> familyIds;
  private int testDataSize;

  @Param({"10000"})
  private int columns;

  @Param({"1", "2", "5", "10"})
  private int families;

  @Param("false")
  private boolean multiThreaded;

  @Param("128")
  private int batchSize;

  @Param("4")
  private int queueCapacity;

  @Param("false")
  private boolean fullFileRead;

  @Param("true")
  private boolean reuseContainers;

  {
    // Only delete the write directory to avoid deleting the read/source directory and losing the
    // pregenerated test records.
    delete(WRITE_DIR);
    Path readDirPath = Path.of(TEST_DIR, READ_DIR);
    Path writeDirPath = Path.of(TEST_DIR, WRITE_DIR);
    try {
      if (!java.nio.file.Files.exists(readDirPath)) {
        java.nio.file.Files.createDirectories(readDirPath);
      }
      if (!java.nio.file.Files.exists(writeDirPath)) {
        java.nio.file.Files.createDirectories(writeDirPath);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create directories", e);
    }
  }

  @Setup(Level.Trial)
  public void setupBenchmark() throws IOException {
    LOG.info(
        "Run: {}, F: {}, MT: {}, BS: {}, QC: {}, FFR: {}",
        columns,
        families,
        multiThreaded,
        batchSize,
        queueCapacity,
        fullFileRead);

    List<Types.NestedField> fieldList = Lists.newArrayListWithCapacity(columns);
    familyIds = Lists.newArrayListWithCapacity(families);
    for (int i = 0; i < families; ++i) {
      familyIds.add(Lists.newArrayList());
    }

    // Generate the column families and the schema.
    int family = 0;
    for (int i = 0; i < columns; ++i) {
      fieldList.add(optional(i, "col" + i, Types.DoubleType.get()));

      List<Integer> familyIdsForColumn = familyIds.get(family);
      if (familyIdsForColumn == null) {
        familyIdsForColumn = Lists.newArrayList();
        familyIds.add(familyIdsForColumn);
      }

      familyIdsForColumn.add(i);
      family = (family + 1) % families;
    }

    testSchema = new Schema(fieldList);
    testDataSize = DATA_SIZE / columns;

    initSourceRecords();
    initReaderRecords();
  }

  @TearDown
  public void tearDownBenchmark() throws IOException {
    // To keep the generated files to speed up the tests, we do not delete the files here.
    //   delete(WRITE_DIR);
    //   delete(READ_DIR);
  }

  private static int counter = 0;

  @Benchmark
  @Threads(1)
  public void write() throws IOException {
    long val = 0;
    String prefix = WRITE_DIR + counter++ + "_write_" + multiThreaded + "_";
    try (DataWriter<Record> writer = writer(prefix);
        CloseableIterable<Record> data = testData()) {
      for (Record record : data) {
        // access something to ensure the compiler doesn't optimize this away
        writer.write(record);
        if (record.get(0) != null) {
          val ^= ((Double) record.get(0)).longValue();
        }
      }
    }

    LOG.info("XOR val: {}", val);
  }

  @Benchmark
  @Threads(1)
  public void writeBaseline() throws IOException {
    if (families != 1 || multiThreaded) {
      return;
    }

    long val = 0;

    String file = TEST_DIR + WRITE_DIR + counter++ + "_write_base_" + columns;
    try (FileAppender<Record> writer =
            Parquet.write(Files.localOutput(file))
                .schema(testSchema)
                .createWriterFunc(GenericParquetWriter::create)
                .build();
        CloseableIterable<Record> data = testData()) {
      for (Record record : data) {
        // access something to ensure the compiler doesn't optimize this away
        writer.add(record);
        if (record.get(0) != null) {
          val ^= ((Double) record.get(0)).longValue();
        }
      }
    }

    LOG.info("XOR val: {}", val);
  }

  @Benchmark
  @Threads(1)
  public void read() throws IOException {

    long val = 0;
    try (CloseableIterable<Record> reader = reader()) {
      for (Record record : reader) {
        // access something to ensure the compiler doesn't optimize this away
        if (record.get(0) != null) {
          val ^= ((Double) record.get(0)).longValue();
        }
      }
    }

    LOG.info("XOR val: {}", val);
  }

  @Benchmark
  @Threads(1)
  public void readBaseline() throws IOException {
    if (families != 1 || multiThreaded) {
      return;
    }
    long val = 0;

    String file = readFileName(0);
    Parquet.ReadBuilder readerBuilder =
        Parquet.read(Files.localInput(file))
            .project(testSchema)
            .createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(testSchema, fileSchema));
    if (reuseContainers) {
      readerBuilder.reuseContainers();
    }

    try (CloseableIterable<Record> reader = readerBuilder.build()) {
      for (Record record : reader) {
        // access something to ensure the compiler doesn't optimize this away
        if (record.get(0) != null) {
          val ^= ((Double) record.get(0)).longValue();
        }
      }
    }

    LOG.info("XOR val: {}", val);
  }

  private void write(String prefix) throws IOException {
    long val = 0;

    try (DataWriter<Record> writer = writer(prefix);
        CloseableIterable<Record> data = testData();
        CloseableIterator<Record> iterator = data.iterator()) {
      while (iterator.hasNext()) {
        Record record = iterator.next();
        // access something to ensure the compiler doesn't optimize this away
        writer.write(record);
        if (record.get(0) != null) {
          val ^= ((Double) record.get(0)).longValue();
        }
      }
    }

    LOG.info("XOR val: {}", val);
  }

  private String readFileName(int family) {
    return fullFileRead
        ? TEST_DIR + READ_DIR + columns + "_0"
        : TEST_DIR + READ_DIR + columns + "_" + (families < 2 ? "0" : families + "_" + family);
  }

  private CloseableIterable<Record> testData() {
    String file = TEST_DIR + SOURCE + "_" + columns + "_" + testDataSize;
    CloseableIterable<Record> iterator =
        Parquet.read(Files.localInput(file))
            .project(testSchema)
            .createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(testSchema, fileSchema))
            .build();
    return CloseableIterable.combine(
        () -> new LimitedIterator(iterator.iterator(), testDataSize), iterator);
  }

  private CloseableIterable<Record> reader() {
    Map<InputFile, List<Integer>> columnSplits = new LinkedHashMap<>();
    for (int i = 0; i < familyIds.size(); ++i) {
      String file = readFileName(i);
      columnSplits.put(Files.localInput(file), familyIds.get(i));
    }

    ReadBuilder<Record, ?> builder =
        FormatModelRegistry.readBuilder(FileFormat.PARQUET, Record.class, columnSplits);
    if (reuseContainers) {
      builder.reuseContainers();
    }

    return builder
        .project(testSchema)
        .set(FormatModel.MULTI_THREADED, Boolean.toString(multiThreaded))
        .set(FormatModel.QUEUE_CAPACITY, String.valueOf(queueCapacity))
        .set(FormatModel.BATCH_SIZE, String.valueOf(batchSize))
        .build();
  }

  private DataWriter<Record> writer(String prefix) throws IOException {
    Map<EncryptedOutputFile, List<Integer>> columnSplits = new LinkedHashMap<>();
    for (int i = 0; i < familyIds.size(); ++i) {
      String file = TEST_DIR + prefix + columns + "_" + families + "_" + i;
      columnSplits.put(
          EncryptionUtil.plainAsEncryptedOutput(Files.localOutput(file)), familyIds.get(i));
    }

    FileWriterBuilder<DataWriter<Record>, ?> builder =
        FormatModelRegistry.dataWriteBuilder(FileFormat.PARQUET, Record.class, columnSplits);
    return builder
        .schema(testSchema)
        .spec(PartitionSpec.unpartitioned())
        .set(FormatModel.MULTI_THREADED, Boolean.toString(multiThreaded))
        .set(FormatModel.QUEUE_CAPACITY, String.valueOf(queueCapacity))
        .set(FormatModel.BATCH_SIZE, String.valueOf(batchSize))
        .build();
  }

  private void delete(String path) {
    Path pathToBeDeleted = Path.of(TEST_DIR, path);
    try (Stream<Path> paths = java.nio.file.Files.walk(pathToBeDeleted)) {
      paths.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    } catch (Exception e) {
      // Ignore exceptions during deletion
    }
  }

  private void initSourceRecords() throws IOException {
    String file = TEST_DIR + SOURCE + "_" + columns + "_" + testDataSize;
    if (!Files.localInput(file).exists()) {
      // System.err.println("New writer source file: " + file);
      try (FileAppender<Record> writer =
          Parquet.write(Files.localOutput(file))
              .schema(testSchema)
              .createWriterFunc(GenericParquetWriter::create)
              .build()) {
        for (int i = 0; i < testDataSize; i += TEST_BATCH_SIZE) {
          writer.addAll(RandomGenericData.generate(testSchema, TEST_BATCH_SIZE, SEED + i));
          // System.err.println("Status: " + i);
        }
      }
      // System.err.println("New writer source file created: " + file);
    } /* else {
        System.err.println("Writer source file already exists: " + file);
      }*/
  }

  private void initReaderRecords() throws IOException {
    String file1 = readFileName(0);
    if (!Files.localInput(file1).exists()) {
      // System.err.println("Generating new file for readers: " + file1);
      write(READ_DIR);
    }
  }

  private static class LimitedIterator implements CloseableIterator<Record> {
    private final CloseableIterator<Record> iterator;
    private int remaining;

    LimitedIterator(CloseableIterator<Record> iterator, int limit) {
      this.iterator = iterator;
      this.remaining = limit;
    }

    @Override
    public boolean hasNext() {
      return remaining > 0 && iterator.hasNext();
    }

    @Override
    public Record next() {
      if (remaining <= 0) {
        throw new IllegalStateException("No more elements available");
      }

      remaining--;
      return iterator.next();
    }

    @Override
    public void close() throws IOException {
      iterator.close();
    }
  }
}

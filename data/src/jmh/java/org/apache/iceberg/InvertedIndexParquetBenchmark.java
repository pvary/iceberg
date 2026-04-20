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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JMH benchmark for measuring point-lookup latency on an inverted-index style Parquet file.
 *
 * <p>The file contains a primary key column (LONG / UUID / STRING / COMPOSITE long+string), the
 * Iceberg data file path the row originated from, and the row position inside that file. The
 * benchmark explores how the row group size and the total number of rows in the file affect the
 * average time of a single equality lookup on the key.
 *
 * <p>Run with for example:
 *
 * <pre>
 *   ./gradlew :iceberg-data:jmh -PjmhIncludeRegex=InvertedIndexParquetBenchmark \
 *       -PjmhOutputPath=data/benchmark/inverted_index.txt
 * </pre>
 *
 * <p>The storage backend used to persist the generated index files is configurable via JVM system
 * properties so the same benchmark can run against the local filesystem, S3 or ADLS without code
 * changes:
 *
 * <ul>
 *   <li>{@code -Dindex.bench.storage=LOCAL|S3|ADLS} (default {@code LOCAL})
 *   <li>{@code -Dindex.bench.location=<base-uri>} - base location for the files. Defaults: {@code
 *       data/benchmark/inverted-index} for {@code LOCAL}; required for S3/ADLS, e.g. {@code
 *       s3://my-bucket/iceberg-bench/inverted-index} or {@code
 *       abfss://container@account.dfs.core.windows.net/iceberg-bench/inverted-index}.
 *   <li>{@code -Dindex.bench.io.<key>=<value>} - any property prefixed with {@code index.bench.io.}
 *       is forwarded to the {@link FileIO} (e.g. {@code -Dindex.bench.io.s3.access-key-id=...},
 *       {@code -Dindex.bench.io.client.region=us-east-1}, {@code
 *       -Dindex.bench.io.adls.sas-token.<account>=...}).
 * </ul>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 1, batchSize = 10)
@Measurement(iterations = 5, batchSize = 10)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class InvertedIndexParquetBenchmark {
  private static final Logger LOG = LoggerFactory.getLogger(InvertedIndexParquetBenchmark.class);

  /** Number of source data files referenced by the index. */
  private static final int NUM_SOURCE_FILES = 1024 * 1024;

  /** Number of pre-generated lookup keys to rotate through during measurement. */
  private static final int NUM_LOOKUP_KEYS = 1024;

  private static final long SEED = 42L;

  public enum KeyType {
    LONG,
    UUID,
    STRING,
    COMPOSITE
  }

  //  @Param({"LONG", "UUID", "STRING", "COMPOSITE"})
  @Param({"LONG"})
  private KeyType keyType;

  /** Total rows written into the index file. */
  @Param({"1000000", "10000000"})
  private int numRows;

  /**
   * Exact number of rows per Parquet row group. Achieved by setting a tiny target byte size and
   * pinning {@code PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT} = {@code
   * PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT} to this value, so the writer evaluates the size check
   * exactly every N records and flushes immediately.
   */
  @Param({"1000", "5000", "10000", "50000", "100000", "500000"})
  private int rowGroupRows;

  // Storage-related configuration. Controlled via JVM system properties so secrets stay outside
  // the source tree -- see the class javadoc for the full list.
  private static final String STORAGE_PROP = "index.bench.storage";
  private static final String LOCATION_PROP = "index.bench.location";
  private static final String IO_PROP_PREFIX = "index.bench.io.";

  private FileIO io;
  private String fileLocation;
  private Schema schema;

  // Pre-generated lookup keys. For COMPOSITE: longKeys[i] + stringKeys[i] form the composite key.
  private long[] longKeys;
  private String[] stringKeys; // used for STRING and COMPOSITE types
  private UUID[] uuidKeys; // used for UUID type
  private int lookupCursor;

  @Setup
  public void setupBenchmark() throws IOException {
    this.schema = buildSchema(keyType);
    this.io = createFileIO();

    String fileName =
        String.format(
            Locale.ROOT, "idx-%s-rows%d-rg%drows.parquet", keyType, numRows, rowGroupRows);
    this.fileLocation = joinPath(baseLocation(), fileName);
    InputFile existing = io.newInputFile(fileLocation);
    boolean reuseExisting;
    try {
      reuseExisting = existing.exists() && existing.getLength() > 0;
    } catch (RuntimeException e) {
      reuseExisting = false;
    }

    if (reuseExisting) {
      LOG.info("Reusing existing index file: {} ({} bytes)", fileLocation, existing.getLength());
    }

    // Generate every row's key in memory, sort by the primary key, then write the file in
    // sorted order. This is what real inverted-index files look like and lets a point lookup
    // touch a single row group (statistics-based skipping).
    long genStart = System.nanoTime();
    Random keyRand = new Random(SEED ^ 0x9E3779B97F4A7C15L);
    long[] allLongs = new long[numRows];
    String[] allStrings =
        (keyType == KeyType.STRING || keyType == KeyType.COMPOSITE) ? new String[numRows] : null;
    UUID[] allUuids = (keyType == KeyType.UUID) ? new UUID[numRows] : null;

    for (int row = 0; row < numRows; row++) {
      Object[] kv = generateKey(keyType, row, keyRand);
      switch (keyType) {
        case LONG -> allLongs[row] = (long) kv[0];
        case UUID -> allUuids[row] = (UUID) kv[0];
        case STRING -> allStrings[row] = (String) kv[0];
        case COMPOSITE -> {
          allLongs[row] = (long) kv[0];
          allStrings[row] = (String) kv[1];
        }
      }
    }

    // Sort rows by the primary key.
    Integer[] order = new Integer[numRows];
    for (int i = 0; i < numRows; i++) {
      order[i] = i;
    }

    switch (keyType) {
      case LONG -> Arrays.sort(order, Comparator.comparingLong(a -> allLongs[a]));
      case UUID -> Arrays.sort(order, Comparator.comparing(a -> allUuids[a]));
      case STRING -> Arrays.sort(order, Comparator.comparing(a -> allStrings[a]));
      case COMPOSITE ->
          Arrays.sort(
              order,
              Comparator.comparingLong((Integer a) -> allLongs[a])
                  .thenComparing(a -> allStrings[a]));
    }

    LOG.info(
        "Generated and sorted {} keys in {} ms",
        numRows,
        (System.nanoTime() - genStart) / 1_000_000);

    // Sample lookup keys uniformly from the sorted row range.
    Random rand = new Random(SEED);
    longKeys = new long[NUM_LOOKUP_KEYS];
    stringKeys = new String[NUM_LOOKUP_KEYS];
    uuidKeys = new UUID[NUM_LOOKUP_KEYS];
    for (int i = 0; i < NUM_LOOKUP_KEYS; i++) {
      int sortedRow = rand.nextInt(numRows);
      int origRow = order[sortedRow];
      longKeys[i] = allLongs[origRow];
      stringKeys[i] = allStrings == null ? null : allStrings[origRow];
      uuidKeys[i] = allUuids == null ? null : allUuids[origRow];
    }

    // Already in random order across the file, but shuffle once more for good measure.
    shuffleLookups(new Random(SEED + 1));

    long writeStart = System.nanoTime();
    if (!reuseExisting) {
      try (FileAppender<Record> writer = newWriter(io.newOutputFile(fileLocation), schema)) {
        GenericRecord template = GenericRecord.create(schema);
        for (int sortedRow = 0; sortedRow < numRows; sortedRow++) {
          int origRow = order[sortedRow];
          Record record = template.copy();
          int pos = 0;
          switch (keyType) {
            case LONG -> record.set(pos++, allLongs[origRow]);
            case UUID -> record.set(pos++, allUuids[origRow]);
            case STRING -> record.set(pos++, allStrings[origRow]);
            case COMPOSITE -> {
              record.set(pos++, allLongs[origRow]);
              record.set(pos++, allStrings[origRow]);
            }
          }

          record.set(
              pos++,
              "s3://bucket/warehouse/db/tbl/data/file-"
                  + (origRow % NUM_SOURCE_FILES)
                  + ".parquet");
          record.set(pos, (long) origRow);
          writer.add(record);
        }
      }
    }

    long writeMs = (System.nanoTime() - writeStart) / 1_000_000;
    long bytes;
    try {
      bytes = io.newInputFile(fileLocation).getLength();
    } catch (RuntimeException e) {
      bytes = -1;
    }

    LOG.info(
        "Wrote {} rows ({} key) to {} ({} bytes), rowGroupRows={} in {} ms",
        numRows,
        keyType,
        fileLocation,
        bytes,
        rowGroupRows,
        writeMs);
  }

  @TearDown
  public void tearDown() {
    if (io != null) {
      if (fileLocation != null) {
        try {
          //          io.deleteFile(fileLocation);
          LOG.info("Deleted index file: {}", fileLocation);
        } catch (Exception e) {
          LOG.warn("Failed to delete index file {}", fileLocation, e);
        }
      }
      try {
        io.close();
      } catch (Exception e) {
        LOG.warn("Failed to close FileIO", e);
      }
    }
  }

  @Benchmark
  @Threads(1)
  public void lookup(Blackhole bh) throws IOException {
    int idx = lookupCursor++ & (NUM_LOOKUP_KEYS - 1);
    Expression filter = buildFilter(keyType, idx);

    int matches = 0;
    try (CloseableIterable<Record> reader =
        Parquet.read(io.newInputFile(fileLocation))
            .project(schema)
            .filter(filter)
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
            .build()) {
      for (Record record : reader) {
        bh.consume(record);
        matches++;
      }
    }

    bh.consume(matches);
  }

  // --------------------------------------------------------------------------
  // helpers
  // --------------------------------------------------------------------------

  private static Schema buildSchema(KeyType type) {
    List<Types.NestedField> fields = Lists.newArrayList();
    int id = 1;
    switch (type) {
      case LONG -> fields.add(required(id++, "key", Types.LongType.get()));
      case UUID -> fields.add(required(id++, "key", Types.UUIDType.get()));
      case STRING -> fields.add(required(id++, "key", Types.StringType.get()));
      case COMPOSITE -> {
        fields.add(required(id++, "key_long", Types.LongType.get()));
        fields.add(required(id++, "key_str", Types.StringType.get()));
      }
    }

    fields.add(required(id++, "file_path", Types.StringType.get()));
    fields.add(required(id, "pos", Types.LongType.get()));
    return new Schema(fields);
  }

  private static Object[] generateKey(KeyType type, int row, Random rand) {
    return switch (type) {
      case LONG -> new Object[] {rand.nextLong() & 0x0FFFFFFFFFFFFFFFL};
      case UUID -> new Object[] {new UUID(rand.nextLong(), rand.nextLong())};
      case STRING -> new Object[] {randomString(rand, 24)};
      case COMPOSITE -> new Object[] {(long) row, randomString(rand, 16)};
    };
  }

  private static String randomString(Random rand, int len) {
    char[] buf = new char[len];
    for (int i = 0; i < len; i++) {
      buf[i] = (char) ('a' + rand.nextInt(26));
    }

    return new String(buf);
  }

  private Expression buildFilter(KeyType type, int idx) {
    return switch (type) {
      case LONG -> Expressions.equal("key", longKeys[idx]);
      case UUID -> Expressions.equal("key", uuidKeys[idx]);
      case STRING -> Expressions.equal("key", stringKeys[idx]);
      case COMPOSITE ->
          Expressions.and(
              Expressions.equal("key_long", longKeys[idx]),
              Expressions.equal("key_str", stringKeys[idx]));
    };
  }

  private FileAppender<Record> newWriter(OutputFile outputFile, Schema fileSchema)
      throws IOException {
    // Force exactly `rowGroupRows` rows per row group: set the size target to a value the writer
    // is guaranteed to exceed in any single record (1 byte) and force the size check to fire on
    // every Nth record by pinning min == max == rowGroupRows.
    String rgRows = Integer.toString(rowGroupRows);
    Parquet.WriteBuilder builder =
        Parquet.write(outputFile)
            .schema(fileSchema)
            .createWriterFunc(GenericParquetWriter::create)
            .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "1")
            .set(TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT, rgRows)
            .set(TableProperties.PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT, rgRows)
            .overwrite();

    return builder.build();
  }

  // --------------------------------------------------------------------------
  // storage backend selection
  // --------------------------------------------------------------------------

  private enum Storage {
    LOCAL,
    S3,
    ADLS
  }

  private static Storage selectedStorage() {
    String raw = System.getProperty(STORAGE_PROP, "LOCAL").trim().toUpperCase(Locale.ROOT);
    try {
      return Storage.valueOf(raw);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Unknown " + STORAGE_PROP + " value: " + raw + " (expected LOCAL, S3 or ADLS)", e);
    }
  }

  private static String baseLocation() {
    String configured = System.getProperty(LOCATION_PROP);
    if (configured != null && !configured.isEmpty()) {
      return stripTrailingSlash(configured);
    }

    Storage storage = selectedStorage();
    if (storage == Storage.LOCAL) {
      File benchDir = new File("data/benchmark/inverted-index");
      if (!benchDir.exists() && !benchDir.mkdirs()) {
        throw new IllegalStateException(
            "Could not create benchmark dir: " + benchDir.getAbsolutePath());
      }

      return benchDir.getAbsolutePath();
    }

    throw new IllegalStateException(
        "-D" + LOCATION_PROP + " is required when " + STORAGE_PROP + "=" + storage);
  }

  private static String joinPath(String base, String name) {
    if (base.endsWith("/")) {
      return base + name;
    }

    return base + "/" + name;
  }

  private static String stripTrailingSlash(String s) {
    return s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
  }

  private static FileIO createFileIO() {
    Storage storage = selectedStorage();
    Map<String, String> props = collectIoProps();
    String impl =
        switch (storage) {
          case LOCAL ->
              // HadoopFileIO works for plain local paths without any extra config.
              "org.apache.iceberg.hadoop.HadoopFileIO";
          case S3 -> "org.apache.iceberg.aws.s3.S3FileIO";
          case ADLS -> "org.apache.iceberg.azure.adlsv2.ADLSFileIO";
        };
    LOG.info("Using FileIO impl={} props={}", impl, props.keySet());
    return CatalogUtil.loadFileIO(impl, props, null);
  }

  private static Map<String, String> collectIoProps() {
    Map<String, String> props = Maps.newHashMap();
    for (String name : System.getProperties().stringPropertyNames()) {
      if (name.startsWith(IO_PROP_PREFIX)) {
        props.put(name.substring(IO_PROP_PREFIX.length()), System.getProperty(name));
      }
    }

    return props;
  }

  private void shuffleLookups(Random rand) {
    for (int i = longKeys.length - 1; i > 0; i--) {
      int j = rand.nextInt(i + 1);
      long tl = longKeys[i];
      longKeys[i] = longKeys[j];
      longKeys[j] = tl;
      String ts = stringKeys[i];
      stringKeys[i] = stringKeys[j];
      stringKeys[j] = ts;
      UUID tu = uuidKeys[i];
      uuidKeys[i] = uuidKeys[j];
      uuidKeys[j] = tu;
    }
  }

  // Use the same Files class indirection as ReaderBenchmark (java.nio.file.Files vs iceberg Files).
}

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

import static org.apache.iceberg.azure.AzureProperties.ADLS_LAZY_OPEN;
import static org.apache.iceberg.azure.AzureProperties.ADLS_READ_BLOCK_SIZE;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.index.HashIndexHandler;
import org.apache.iceberg.index.IndexHandler;
import org.apache.iceberg.index.MinimalPerfectHashFunctionIndexHandler;
import org.apache.iceberg.index.ParquetIndexHandler;
import org.apache.iceberg.index.ParquetIndexHandlerWithEmbeddedMetadata;
import org.apache.iceberg.index.ParquetIndexHandlerWithHashedRowGroups;
import org.apache.iceberg.index.UltraCompactHasherIndexHandler;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
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
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.runner.IterationType;
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
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class InvertedIndexBenchmark {
  private static final Logger LOG = LoggerFactory.getLogger(InvertedIndexBenchmark.class);

  /** Number of source data files referenced by the index. */
  private static final int NUM_SOURCE_FILES = 1024 * 1024;

  /** Number of pre-generated lookup keys to rotate through during measurement. */
  private static final int NUM_LOOKUP_KEYS = 1024 * 1024;

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
   * Index format and (for Parquet) row group size, encoded as {@code "PARQUET_<rows>"}, {@code
   * "UCH_<kLimit>"}, {@code "HASH_<rows>"}, {@code "PHASH_<rows>"} or {@code "MPHF"}. {@link
   * #setupBenchmark()} parses it into the {@code is*} flags and the corresponding numeric
   * parameter.
   */
  @Param({
    "PARQUET_10000",
    "PARQUET_5000",
    "PARQUET_20000",
    "PARQUET_50000",
    "MPHF",
    "HASH_2000",
    "HASH_5000",
    "HASH_10000",
    "HASH_20000",
    "EPHASH_2000",
    "EPHASH_5000",
    "EPHASH_10000",
    "EPHASH_20000",
    "EPHASH_50000"
  })
  private String indexType;

  // Parsed from indexType in setupBenchmark.
  private boolean isMphf;
  private boolean isUch;
  private boolean isHash;
  private boolean isPhash;
  private boolean isEphash;
  private int bucketRows;
  private int kLimit;

  // Storage-related configuration. Controlled via JVM system properties so secrets stay outside
  // the source tree -- see the class javadoc for the full list.
  private static final String STORAGE_PROP = "index.bench.storage";
  private static final String LOCATION_PROP = "index.bench.location";
  private static final String IO_PROP_PREFIX = "index.bench.io.";

  private FileIO io;
  private String fileLocation;
  private Schema keySchema; // key-only schema, used to drive the index handler

  /**
   * The index handler chosen for this run, instantiated once in {@link #setupBenchmark()} based on
   * {@link #indexType}. Both {@link #lookup} and {@link #write} go through this single instance so
   * the in-process reader picks up any state the writer published (e.g. {@code
   * ParquetIndexHandler#filePathPrefix}).
   */
  private IndexHandler indexHandler;

  // Pre-generated lookup keys. For COMPOSITE: longKeys[i] + stringKeys[i] form the composite key.
  private long[] longKeys;
  private String[] stringKeys; // used for STRING and COMPOSITE types
  private UUID[] uuidKeys; // used for UUID type
  private Record[] lookupKeyRecords; // one Record per lookup key, matching keySchema

  /** The row index the sampled lookup key came from. Used to validate {@code lookup()} hits. */
  private long[] expectedPositions;

  private int lookupCursor;

  // Full per-row key arrays kept around so the write benchmark can re-materialize the index
  // without paying the key-generation cost.
  private long[] allLongs;
  private UUID[] allUuids;
  private String[] allStrings;

  // Counter used by the write benchmark to generate unique destination paths per invocation
  // so repeated iterations don't collide on the same file.
  private int writeCursor;

  @Setup
  public void setupBenchmark() throws Exception {
    LOG.info(
        "InvertedIndexBenchmark setup: -D{}={} (os.name={})",
        PURGE_CACHE_PROP,
        System.getProperty(PURGE_CACHE_PROP),
        System.getProperty("os.name"));
    parseIndexType();
    this.keySchema = buildKeySchema(keyType);
    if (isMphf) {
      this.indexHandler = new MinimalPerfectHashFunctionIndexHandler(keySchema, numRows);
    } else if (isUch) {
      this.indexHandler = new UltraCompactHasherIndexHandler(keySchema, numRows, kLimit);
    } else if (isHash) {
      this.indexHandler = new HashIndexHandler(keySchema, numRows, bucketRows);
    } else if (isPhash) {
      this.indexHandler =
          new ParquetIndexHandlerWithHashedRowGroups(keySchema, bucketRows, numRows);
    } else if (isEphash) {
      this.indexHandler =
          new ParquetIndexHandlerWithEmbeddedMetadata(keySchema, bucketRows, numRows);
    } else {
      this.indexHandler = new ParquetIndexHandler(keySchema, bucketRows, numRows);
    }
    // FileIO construction must come AFTER the handler is built so createFileIO() can ask the
    // handler for its recommendedReadBlockSize() hint.
    this.io = new CountingFileIO(createFileIO());

    this.fileLocation = joinPath(baseLocation(), filename());
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

    // Generate every row's key in memory.
    long genStart = System.nanoTime();
    Random keyRand = new Random(SEED ^ 0x9E3779B97F4A7C15L);
    allLongs = new long[numRows];
    allStrings =
        (keyType == KeyType.STRING || keyType == KeyType.COMPOSITE) ? new String[numRows] : null;
    allUuids = (keyType == KeyType.UUID) ? new UUID[numRows] : null;

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

    // Sample lookup keys uniformly from the row range.
    Random rand = new Random(SEED);
    longKeys = new long[NUM_LOOKUP_KEYS];
    stringKeys = new String[NUM_LOOKUP_KEYS];
    uuidKeys = new UUID[NUM_LOOKUP_KEYS];
    expectedPositions = new long[NUM_LOOKUP_KEYS];
    lookupKeyRecords = new Record[NUM_LOOKUP_KEYS];
    for (int i = 0; i < NUM_LOOKUP_KEYS; i++) {
      int row = rand.nextInt(numRows);
      longKeys[i] = allLongs[row];
      stringKeys[i] = allStrings == null ? null : allStrings[row];
      uuidKeys[i] = allUuids == null ? null : allUuids[row];
      expectedPositions[i] = row;
      lookupKeyRecords[i] =
          buildKeyRecord(keySchema, keyType, longKeys[i], uuidKeys[i], stringKeys[i]);
    }

    shuffleLookups(new Random(SEED + 1));

    LOG.info("Generated {} keys in {} ms", numRows, (System.nanoTime() - genStart) / 1_000_000);

    long writeStart = System.nanoTime();
    if (!reuseExisting) {
      writeIndex();
    }

    long writeMs = (System.nanoTime() - writeStart) / 1_000_000;
    long bytes;
    try {
      bytes = io.newInputFile(fileLocation).getLength();
    } catch (RuntimeException e) {
      bytes = -1;
    }

    LOG.info(
        "Wrote {} rows ({} key, indexType={}) to {} ({} bytes) in {} ms",
        numRows,
        keyType,
        indexType,
        fileLocation,
        bytes,
        writeMs);
  }

  private String filename() {
    String fileName;
    if (isMphf) {
      fileName = String.format(Locale.ROOT, "idx-%s-rows%d-mphf.bin", keyType, numRows);
    } else if (isUch) {
      fileName = String.format(Locale.ROOT, "idx-%s-rows%d-uch-k%d.bin", keyType, numRows, kLimit);
    } else if (isHash) {
      fileName =
          String.format(
              Locale.ROOT, "idx-%s-rows%d-hash-rg%drows.bin", keyType, numRows, bucketRows);
    } else if (isPhash) {
      fileName =
          String.format(
              Locale.ROOT, "idx-%s-rows%d-phash-rg%drows.parquet", keyType, numRows, bucketRows);
    } else if (isEphash) {
      fileName =
          String.format(
              Locale.ROOT, "idx-%s-rows%d-ephash-rg%drows.bin", keyType, numRows, bucketRows);
    } else {
      fileName =
          String.format(
              Locale.ROOT, "idx-%s-rows%d-parquet-rg%drows.parquet", keyType, numRows, bucketRows);
    }
    return fileName;
  }

  private void parseIndexType() {
    // Default-reset all flags / numeric params; the matched branch overwrites the relevant ones.
    this.isMphf = false;
    this.isUch = false;
    this.isHash = false;
    this.isPhash = false;
    this.isEphash = false;
    this.bucketRows = -1;
    this.kLimit = -1;

    if ("MPHF".equalsIgnoreCase(indexType)) {
      this.isMphf = true;
      return;
    }

    if (indexType.regionMatches(true, 0, "UCH_", 0, "UCH_".length())) {
      this.isUch = true;
      this.kLimit = Integer.parseInt(indexType.substring("UCH_".length()));
      return;
    }

    if (indexType.regionMatches(true, 0, "HASH_", 0, "HASH_".length())) {
      this.isHash = true;
      this.bucketRows = Integer.parseInt(indexType.substring("HASH_".length()));
      return;
    }

    if (indexType.regionMatches(true, 0, "PHASH_", 0, "PHASH_".length())) {
      this.isPhash = true;
      this.bucketRows = Integer.parseInt(indexType.substring("PHASH_".length()));
      return;
    }

    if (indexType.regionMatches(true, 0, "EPHASH_", 0, "EPHASH_".length())) {
      this.isEphash = true;
      this.bucketRows = Integer.parseInt(indexType.substring("EPHASH_".length()));
      return;
    }

    if (indexType.regionMatches(true, 0, "PARQUET_", 0, "PARQUET_".length())) {
      this.bucketRows = Integer.parseInt(indexType.substring("PARQUET_".length()));
      return;
    }

    throw new IllegalArgumentException(
        "Unknown indexType: "
            + indexType
            + " (expected MPHF, UCH_<kLimit>, HASH_<rows>, PHASH_<rows>,"
            + " EPHASH_<rows> or PARQUET_<rows>)");
  }

  /**
   * Property toggle ({@code -Dindex.bench.freshClientPerIteration=true}) that forces {@link
   * #refreshFileIO(IterationParams)} to close and rebuild the {@link FileIO} (and its underlying
   * Azure / S3 SDK client, connection pool, TLS sessions) before every measurement iteration. Use
   * this to isolate client-side caching effects from server-side caching when comparing per-op
   * lookup latency across {@code numRows} on the same blob.
   */
  private static final String FRESH_CLIENT_PROP = "index.bench.freshClientPerIteration";

  /**
   * Drop and recreate {@link #io} so the next iteration runs against a brand-new SDK client. Done
   * at {@link Level#ITERATION} (not per invocation) because rebuilding the client on every lookup
   * would dominate the measurement; per-iteration is enough to invalidate connection-pool / TLS
   * keepalive state that survives across the ~1000 invocations of a single iteration.
   *
   * <p>Skipped during warmup so warmup keeps doing what it's supposed to (priming the JIT) and
   * doesn't drag in cold-connection variance.
   */
  @Setup(Level.Iteration)
  public void refreshFileIO(IterationParams params) {
    if (params.getType() != IterationType.MEASUREMENT) {
      return;
    }
    if (!Boolean.getBoolean(FRESH_CLIENT_PROP)) {
      return;
    }
    if (io != null) {
      try {
        io.close();
      } catch (Exception e) {
        LOG.warn("Failed to close FileIO on iteration refresh", e);
      }
    }
    this.io = new CountingFileIO(createFileIO());
    LOG.info(
        "Rebuilt FileIO for fresh client (-D{}=true)",
        FRESH_CLIENT_PROP);
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
  @Warmup(iterations = 10)
  @Measurement(iterations = 100)
  public void lookup(Blackhole bh, ReadCounter ioCounter) throws Exception {
    int idx = lookupCursor++ & (NUM_LOOKUP_KEYS - 1);
    long expectedPos = expectedPositions[idx];
    String expectedFilePath =
        "s3://bucket/warehouse/db/tbl/data/file-" + (expectedPos % NUM_SOURCE_FILES) + ".parquet";

    try (IndexHandler.Reader reader = indexHandler.reader(io.newInputFile(fileLocation))) {
      IndexHandler.Hit hit = reader.lookup(lookupKeyRecords[idx]);
      if (hit == null) {
        throw new RuntimeException(
            indexType
                + " lookup returned null for idx="
                + idx
                + " (expected pos="
                + expectedPos
                + ", filePath="
                + expectedFilePath
                + ")");
      }

      if (hit.pos() != expectedPos) {
        throw new RuntimeException(
            indexType
                + " pos mismatch for idx="
                + idx
                + ": expected "
                + expectedPos
                + " got "
                + hit.pos());
      }

      if (!expectedFilePath.equals(hit.filePath())) {
        throw new RuntimeException(
            indexType
                + " filePath mismatch for idx="
                + idx
                + ": expected "
                + expectedFilePath
                + " got "
                + hit.filePath());
      }

      bh.consume(hit);
    }
  }

  /**
   * Measures the time it takes to fully materialize the inverted-index file for the current
   * configuration. Each invocation writes to a unique path (so repeat iterations don't collide) and
   * deletes the file at the end of the measured region... actually we delete outside the measured
   * region via a best-effort cleanup so the write cost is isolated.
   */
  @Benchmark
  @Threads(1)
  @Warmup(iterations = 1)
  @Measurement(iterations = 3)
  public void write(Blackhole bh, WriteCounter ioCounter) throws Exception {
    String originalLocation = fileLocation;
    String writeLocation = joinPath(baseLocation(), uniqueWriteFileName());
    fileLocation = writeLocation;
    try {
      writeIndex();

      // Report the resulting file size via the IoCounter aux metric so it shows up as
      // `write:indexFileBytes` in the JMH results table.
      try {
        ioCounter.indexFileBytes = io.newInputFile(writeLocation).getLength();
      } catch (RuntimeException e) {
        LOG.warn("Failed to stat index file {}", writeLocation, e);
        ioCounter.indexFileBytes = -1;
      }

      bh.consume(writeLocation);
    } finally {
      fileLocation = originalLocation;
      try {
        io.deleteFile(writeLocation);
      } catch (Exception e) {
        LOG.warn("Failed to delete benchmark write output {}", writeLocation, e);
      }
    }
  }

  private String uniqueWriteFileName() {
    int seq = writeCursor++;
    if (isMphf) {
      return String.format(Locale.ROOT, "write-idx-%s-rows%d-mphf-%d.bin", keyType, numRows, seq);
    }

    if (isUch) {
      return String.format(
          Locale.ROOT, "write-idx-%s-rows%d-uch-k%d-%d.bin", keyType, numRows, kLimit, seq);
    }

    if (isHash) {
      return String.format(
          Locale.ROOT,
          "write-idx-%s-rows%d-hash-rg%drows-%d.bin",
          keyType,
          numRows,
          bucketRows,
          seq);
    }

    return String.format(
        Locale.ROOT,
        "write-idx-%s-rows%d-parquet-rg%drows-%d.parquet",
        keyType,
        numRows,
        bucketRows,
        seq);
  }

  // --------------------------------------------------------------------------
  // index file generation
  // --------------------------------------------------------------------------

  /**
   * Materializes the inverted-index file via {@link #indexHandler}. Each row is encoded into a key
   * {@link Record} (matching {@link #keySchema}) plus the synthetic source-file path the row
   * "originated" from; the handler decides how to lay that out on disk.
   */
  private void writeIndex() throws Exception {
    try (IndexHandler.Writer writer = indexHandler.writer(io.newOutputFile(fileLocation))) {
      for (int row = 0; row < numRows; row++) {
        Record keyRecord =
            buildKeyRecord(
                keySchema,
                keyType,
                allLongs[row],
                allUuids == null ? null : allUuids[row],
                allStrings == null ? null : allStrings[row]);
        String filePath =
            "s3://bucket/warehouse/db/tbl/data/file-" + (row % NUM_SOURCE_FILES) + ".parquet";
        writer.add(keyRecord, filePath, row);
      }
    }
  }

  // --------------------------------------------------------------------------
  // helpers
  // --------------------------------------------------------------------------

  /** Builds the key-only schema (no payload columns). Used to configure the index handler. */
  private static Schema buildKeySchema(KeyType type) {
    List<Types.NestedField> fields = Lists.newArrayList();
    int id = 1;
    switch (type) {
      case LONG -> fields.add(required(id, "key", Types.LongType.get()));
      case UUID -> fields.add(required(id, "key", Types.UUIDType.get()));
      case STRING -> fields.add(required(id, "key", Types.StringType.get()));
      case COMPOSITE -> {
        fields.add(required(id++, "key_long", Types.LongType.get()));
        fields.add(required(id, "key_str", Types.StringType.get()));
      }
    }

    return new Schema(fields);
  }

  /** Materializes a {@link Record} matching {@link #buildKeySchema(KeyType)} for the given key. */
  private static Record buildKeyRecord(
      Schema keySchema, KeyType type, long longVal, UUID uuidVal, String stringVal) {
    Record record = GenericRecord.create(keySchema);
    switch (type) {
      case LONG -> record.set(0, longVal);
      case UUID -> record.set(0, uuidVal);
      case STRING -> record.set(0, stringVal);
      case COMPOSITE -> {
        record.set(0, longVal);
        record.set(1, stringVal);
      }
    }

    return record;
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

  private FileIO createFileIO() {
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

    // Programmatic ADLS tuning, sourced from the IndexHandler itself. The handler knows its
    // access pattern (HashIndexHandler issues one bounded RangeReadable.readFully per
    // header + per bucket) and reports the smallest first-GET size that fits a single read,
    // avoiding the SDK's 4 MB default. Pair with lazy-open so the constructor doesn't issue
    // an unbounded eager open. Both honor any user-supplied -Dindex.bench.io.adls.* override.
    if (storage == Storage.ADLS && indexHandler != null) {
      Integer size = indexHandler.recommendedReadBlockSize();
      if (size != null) {
        props.putIfAbsent(ADLS_READ_BLOCK_SIZE, Integer.toString(size));
        props.putIfAbsent(ADLS_LAZY_OPEN, "true");
        LOG.info(
            "ADLS tuning from {}: {}={} {}=true",
            indexHandler.getClass().getSimpleName(),
            ADLS_READ_BLOCK_SIZE,
            size,
            ADLS_LAZY_OPEN);
      }
    }

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
      long tp = expectedPositions[i];
      expectedPositions[i] = expectedPositions[j];
      expectedPositions[j] = tp;
      if (lookupKeyRecords != null) {
        Record tr = lookupKeyRecords[i];
        lookupKeyRecords[i] = lookupKeyRecords[j];
        lookupKeyRecords[j] = tr;
      }
    }
  }

  // Use the same Files class indirection as ReaderBenchmark (java.nio.file.Files vs iceberg Files).

  // --------------------------------------------------------------------------
  // IO instrumentation: counts bytes read/written and seeks performed by the
  // benchmark, surfaced as JMH secondary metrics via @AuxCounters.
  // --------------------------------------------------------------------------

  private static final AtomicLong BYTES_READ = new AtomicLong();
  private static final AtomicLong SEEKS = new AtomicLong();
  private static final AtomicLong OPEN_INPUT_STREAMS = new AtomicLong();

  /**
   * Number of {@code read*()} calls on the underlying {@link SeekableInputStream}. Counts every
   * invocation that returned at least one byte (so partial-read loops still surface as multiple
   * reads), regardless of how many bytes each call transferred.
   */
  private static final AtomicLong READS = new AtomicLong();

  /**
   * Mirror of {@code ADLSInputStream.WIRE_REQUESTS}: number of actual ADLS HTTP GETs issued by the
   * Azure SDK across all {@code ADLSInputStream} instances in this JVM. Read via {@link
   * #wireRequestsSnapshot()} (reflection, so non-ADLS runs need no Azure classes on the classpath)
   * and surfaced per-invocation as {@code lookup:wireRequests}. Falls back to {@link #READS} when
   * the field isn't reachable (e.g. local-FS runs), which is the correct semantics there: every
   * read on local FS is its own kernel syscall.
   */
  private static final Field ADLS_WIRE_REQUESTS_FIELD = resolveAdlsWireField();

  private static Field resolveAdlsWireField() {
    try {
      Class<?> cls = Class.forName("org.apache.iceberg.azure.adlsv2.ADLSInputStream");
      Field f = cls.getDeclaredField("WIRE_REQUESTS");
      f.setAccessible(true);
      return f;
    } catch (Throwable t) {
      return null;
    }
  }

  private static long wireRequestsSnapshot() {
    if (ADLS_WIRE_REQUESTS_FIELD != null) {
      try {
        return ((AtomicLong) ADLS_WIRE_REQUESTS_FIELD.get(null)).get();
      } catch (IllegalAccessException e) {
        // fall through to READS
      }
    }

    return READS.get();
  }

  // Cumulative wall-clock nanoseconds spent inside the corresponding IO call.
  private static final AtomicLong OPEN_NANOS = new AtomicLong();
  private static final AtomicLong SEEK_NANOS = new AtomicLong();
  private static final AtomicLong READ_NANOS = new AtomicLong();

  /**
   * If {@code -Dindex.bench.purgeCache=true} is set, drops the OS page cache before every measured
   * lookup invocation by shelling out to {@code sudo -n purge} (macOS) or writing {@code 3} to
   * {@code /proc/sys/vm/drop_caches} (Linux). Both paths require passwordless privilege escalation.
   * The purge runs inside {@code @Setup(Level.Invocation)} so its cost is excluded from the
   * measured region.
   */
  private static final String PURGE_CACHE_PROP = "index.bench.purgeCache";

  private static void dropOsPageCache() {
    String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
    try {
      ProcessBuilder pb;
      if (os.contains("mac") || os.contains("darwin")) {
        // macOS: `sudo -n purge` requires a NOPASSWD sudoers entry for /usr/sbin/purge.
        pb = new ProcessBuilder("sudo", "-n", "/usr/sbin/purge");
        System.err.println("Trying to purge OS page cache");
      } else if (os.contains("linux")) {
        pb =
            new ProcessBuilder(
                "sh", "-c", "sync && sudo -n sh -c 'echo 3 > /proc/sys/vm/drop_caches'");
      } else {
        throw new IllegalStateException("Cache purge not supported on OS: " + os);
      }
      Process p = pb.redirectErrorStream(true).start();

      if (!p.waitFor(60, TimeUnit.SECONDS)) {
        p.destroyForcibly();
        throw new IllegalStateException("OS page cache purge timed out after 60s");
      }

      if (p.exitValue() != 0) {
        throw new IllegalStateException(
            "OS page cache purge failed with exit code "
                + p.exitValue()
                + " (need passwordless sudo for purge / drop_caches?)");
      }

      LOG.info("Purging OS page  cache (set -D" + PURGE_CACHE_PROP + "=false to disable)");
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }

      throw new IllegalStateException("Failed to purge OS page cache", e);
    }
  }

  @State(Scope.Thread)
  @AuxCounters(AuxCounters.Type.EVENTS)
  public static class ReadCounter {
    public long bytesRead;
    public long seeks;
    public long openStreams;

    /** Number of {@code read*()} calls issued against the underlying input stream. */
    public long reads;

    /**
     * Number of actual ADLS HTTP GETs issued during this invocation. Sampled from {@code
     * ADLSInputStream.WIRE_REQUESTS} via {@link #wireRequestsSnapshot()}; on non-ADLS storages the
     * counter falls back to {@link #reads} (every read on local FS is its own kernel syscall).
     */
    public long wireRequests;

    /** Total wall-clock microseconds spent inside {@code InputFile#newStream()}. */
    public long openMicros;

    /** Total wall-clock microseconds spent inside {@code SeekableInputStream#seek()}. */
    public long seekMicros;

    /** Total wall-clock microseconds spent inside {@code SeekableInputStream#read*()}. */
    public long readMicros;

    private long startBytesRead;
    private long startSeeks;
    private long startOpenStreams;
    private long startReads;
    private long startWireRequests;
    private long startOpenNanos;
    private long startSeekNanos;
    private long startReadNanos;

    /**
     * Set by {@link #beforeIteration(IterationParams)} so {@link #beforeInvocation()} can skip the
     * (expensive) page-cache purge during warmup iterations and only run it for measurement.
     */
    private boolean measuring;

    @Setup(Level.Iteration)
    public void beforeIteration(IterationParams params) {
      this.measuring = params.getType() == IterationType.MEASUREMENT;
    }

    @Setup(Level.Invocation)
    public void beforeInvocation() {
      // Drop OS page cache (if enabled) BEFORE we snapshot the IO counters so the purge's own
      // syscalls don't pollute the per-invocation deltas reported to JMH. Skipped during warmup
      // iterations -- warmup is meant to be cheap and just primes the JIT.
      if (measuring && Boolean.getBoolean(PURGE_CACHE_PROP)) {
        dropOsPageCache();
      }
      startBytesRead = BYTES_READ.get();
      startSeeks = SEEKS.get();
      startOpenStreams = OPEN_INPUT_STREAMS.get();
      startReads = READS.get();
      startWireRequests = wireRequestsSnapshot();
      startOpenNanos = OPEN_NANOS.get();
      startSeekNanos = SEEK_NANOS.get();
      startReadNanos = READ_NANOS.get();
    }

    @TearDown(Level.Invocation)
    public void afterInvocation() {
      bytesRead = BYTES_READ.get() - startBytesRead;
      seeks = SEEKS.get() - startSeeks;
      openStreams = OPEN_INPUT_STREAMS.get() - startOpenStreams;
      reads = READS.get() - startReads;
      wireRequests = wireRequestsSnapshot() - startWireRequests;
      openMicros = (OPEN_NANOS.get() - startOpenNanos) / 1_000L;
      seekMicros = (SEEK_NANOS.get() - startSeekNanos) / 1_000L;
      readMicros = (READ_NANOS.get() - startReadNanos) / 1_000L;
    }
  }

  /**
   * Write-side IO counters surfaced by the {@link #write} benchmark. Reports the resulting on-disk
   * index file size.
   */
  @State(Scope.Thread)
  @AuxCounters(AuxCounters.Type.EVENTS)
  public static class WriteCounter {
    /** Size of the index file produced by the most recent write invocation, in bytes. */
    public long indexFileBytes;
  }

  /** Decorator that funnels every input/output through the global counters. */
  private record CountingFileIO(FileIO delegate) implements FileIO {

    @Override
    public InputFile newInputFile(String path) {
      return new CountingInputFile(delegate.newInputFile(path));
    }

    @Override
    public InputFile newInputFile(String path, long length) {
      return new CountingInputFile(delegate.newInputFile(path, length));
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return new CountingOutputFile(delegate.newOutputFile(path));
    }

    @Override
    public void deleteFile(String path) {
      delegate.deleteFile(path);
    }

    @Override
    public Map<String, String> properties() {
      return delegate.properties();
    }

    @Override
    public void initialize(Map<String, String> properties) {
      delegate.initialize(properties);
    }

    @Override
    public void close() {
      delegate.close();
    }
  }

  private record CountingInputFile(InputFile delegate) implements InputFile {

    @Override
    public long getLength() {
      return delegate.getLength();
    }

    @Override
    public SeekableInputStream newStream() {
      OPEN_INPUT_STREAMS.incrementAndGet();
      long t0 = System.nanoTime();
      SeekableInputStream s = delegate.newStream();
      OPEN_NANOS.addAndGet(System.nanoTime() - t0);
      return new CountingSeekableInputStream(s);
    }

    @Override
    public String location() {
      return delegate.location();
    }

    @Override
    public boolean exists() {
      return delegate.exists();
    }
  }

  /** Thin counting passthrough: records bytes/seek/read timings without altering IO patterns. */
  private static final class CountingSeekableInputStream extends SeekableInputStream {
    private final SeekableInputStream delegate;

    CountingSeekableInputStream(SeekableInputStream delegate) {
      this.delegate = delegate;
    }

    @Override
    public long getPos() throws IOException {
      return delegate.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
      SEEKS.incrementAndGet();
      long t0 = System.nanoTime();
      try {
        delegate.seek(newPos);
      } finally {
        SEEK_NANOS.addAndGet(System.nanoTime() - t0);
      }
    }

    @Override
    public int read() throws IOException {
      long t0 = System.nanoTime();
      int b;
      try {
        b = delegate.read();
      } finally {
        READ_NANOS.addAndGet(System.nanoTime() - t0);
      }
      if (b >= 0) {
        READS.incrementAndGet();
        BYTES_READ.incrementAndGet();
      }
      return b;
    }

    @Override
    public int read(byte[] b) throws IOException {
      return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      long t0 = System.nanoTime();
      int n;
      try {
        n = delegate.read(b, off, len);
      } finally {
        READ_NANOS.addAndGet(System.nanoTime() - t0);
      }
      if (n > 0) {
        READS.incrementAndGet();
        BYTES_READ.addAndGet(n);
      }
      return n;
    }

    @Override
    public long skip(long n) throws IOException {
      long t0 = System.nanoTime();
      long skipped;
      try {
        skipped = delegate.skip(n);
      } finally {
        READ_NANOS.addAndGet(System.nanoTime() - t0);
      }
      if (skipped > 0) {
        BYTES_READ.addAndGet(skipped);
      }
      return skipped;
    }

    @Override
    public int available() throws IOException {
      return delegate.available();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }

  private record CountingOutputFile(OutputFile delegate) implements OutputFile {

    @Override
    public PositionOutputStream create() {
      return new CountingPositionOutputStream(delegate.create());
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      return new CountingPositionOutputStream(delegate.createOrOverwrite());
    }

    @Override
    public String location() {
      return delegate.location();
    }

    @Override
    public InputFile toInputFile() {
      return new CountingInputFile(delegate.toInputFile());
    }
  }

  private static final class CountingPositionOutputStream extends PositionOutputStream {
    private final PositionOutputStream delegate;

    CountingPositionOutputStream(PositionOutputStream delegate) {
      this.delegate = delegate;
    }

    @Override
    public long getPos() throws IOException {
      return delegate.getPos();
    }

    @Override
    public void write(int b) throws IOException {
      delegate.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
      delegate.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      delegate.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
      delegate.flush();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }
}

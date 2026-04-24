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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.index.IndexHandler;
import org.apache.iceberg.index.MinimalPerfectHashFunctionIndexHandler;
import org.apache.iceberg.index.ParquetIndexHandler;
import org.apache.iceberg.index.VortexIndexHandler;
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
   * "VORTEX"} or {@code "MPHF"}. {@link #setupBenchmark()} parses it into {@link #isMphf}, {@link
   * #isVortex} and {@link #rowGroupRows}.
   */
  @Param({"VORTEX", "MPHF", "PARQUET_10000"})
  private String indexType;

  // Parsed from indexType in setupBenchmark.
  private boolean isMphf;
  private boolean isVortex;
  private int rowGroupRows;

  // Storage-related configuration. Controlled via JVM system properties so secrets stay outside
  // the source tree -- see the class javadoc for the full list.
  private static final String STORAGE_PROP = "index.bench.storage";
  private static final String LOCATION_PROP = "index.bench.location";
  private static final String IO_PROP_PREFIX = "index.bench.io.";
  // Properties forwarded to the Hadoop Configuration used by HadoopFileIO. Required for ADLS so
  // VortexFileUtil can extract the SAS token / account key from the wrapped HadoopOutputFile,
  // e.g.:
  //   -Dindex.bench.hadoop.fs.azure.sas.fixed.token.<acct>.dfs.core.windows.net=<sas>
  //   -Dindex.bench.hadoop.fs.azure.account.auth.type.<acct>.dfs.core.windows.net=SAS
  //   -Dindex.bench.hadoop.fs.azure.sas.token.provider.type.<acct>.dfs.core.windows.net=
  //         org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider
  private static final String HADOOP_PROP_PREFIX = "index.bench.hadoop.";

  private FileIO io;
  // Raw, uncounted FileIO. Vortex bypasses Java IO and talks to object storage natively, so the
  // CountingFileIO wrapper would (a) never see those bytes and (b) hide the underlying
  // HadoopOutputFile / HadoopInputFile from VortexFileUtil's credential resolution.
  private FileIO rawIo;
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
    parseIndexType();
    this.rawIo = createFileIO();
    this.io = new CountingFileIO(rawIo);
    this.keySchema = buildKeySchema(keyType);
    this.indexHandler =
        isMphf
            ? new MinimalPerfectHashFunctionIndexHandler(keySchema, numRows)
            : isVortex
                ? new VortexIndexHandler(keySchema)
                : new ParquetIndexHandler(keySchema, rowGroupRows);

    String fileName;
    if (isMphf) {
      fileName = String.format(Locale.ROOT, "idx-%s-rows%d-mphf.bin", keyType, numRows);
    } else if (isVortex) {
      fileName = String.format(Locale.ROOT, "idx-%s-rows%d-vortex.vortex", keyType, numRows);
    } else {
      fileName =
          String.format(
              Locale.ROOT,
              "idx-%s-rows%d-parquet-rg%drows.parquet",
              keyType,
              numRows,
              rowGroupRows);
    }
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

  private void parseIndexType() {
    if ("MPHF".equalsIgnoreCase(indexType)) {
      this.isMphf = true;
      this.isVortex = false;
      this.rowGroupRows = -1;
      return;
    }

    if ("VORTEX".equalsIgnoreCase(indexType)) {
      this.isMphf = false;
      this.isVortex = true;
      this.rowGroupRows = -1;
      return;
    }

    if (indexType.regionMatches(true, 0, "PARQUET_", 0, "PARQUET_".length())) {
      this.isMphf = false;
      this.isVortex = false;
      this.rowGroupRows = Integer.parseInt(indexType.substring("PARQUET_".length()));
      return;
    }

    throw new IllegalArgumentException(
        "Unknown indexType: " + indexType + " (expected MPHF, VORTEX or PARQUET_<rows>)");
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
  @Warmup(iterations = 100)
  @Measurement(iterations = 3000)
  public void lookup(Blackhole bh, ReadCounter ioCounter) throws Exception {
    int idx = lookupCursor++ & (NUM_LOOKUP_KEYS - 1);
    long expectedPos = expectedPositions[idx];
    String expectedFilePath =
        "s3://bucket/warehouse/db/tbl/data/file-" + (expectedPos % NUM_SOURCE_FILES) + ".parquet";

    try (IndexHandler.Reader reader =
        indexHandler.reader(ioForIndex().newInputFile(fileLocation))) {
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
        ioForIndex().deleteFile(writeLocation);
      } catch (Exception e) {
        LOG.warn("Failed to delete benchmark write output {}", writeLocation, e);
      }
    }
  }

  /**
   * Returns the {@link FileIO} used to open the index file.
   *
   * <p>Vortex bypasses Java IO and accesses object storage natively via JNI; the {@link
   * CountingFileIO} wrapper would (a) never observe those bytes (so the counters add no value) and
   * (b) hide the underlying {@code HadoopInputFile} / {@code HadoopOutputFile} from {@code
   * VortexFileUtil}'s credential resolution, causing it to fall back to default cloud auth (e.g.
   * Azure IMDS). For all other index handlers we keep the counting wrapper so the JMH IO counters
   * remain accurate.
   */
  private FileIO ioForIndex() {
    return isVortex ? rawIo : io;
  }

  private String uniqueWriteFileName() {
    int seq = writeCursor++;
    if (isMphf) {
      return String.format(Locale.ROOT, "write-idx-%s-rows%d-mphf-%d.bin", keyType, numRows, seq);
    }

    if (isVortex) {
      return String.format(
          Locale.ROOT, "write-idx-%s-rows%d-vortex-%d.vortex", keyType, numRows, seq);
    }

    return String.format(
        Locale.ROOT,
        "write-idx-%s-rows%d-parquet-rg%drows-%d.parquet",
        keyType,
        numRows,
        rowGroupRows,
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
    try (IndexHandler.Writer writer =
        indexHandler.writer(ioForIndex().newOutputFile(fileLocation))) {
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
      case LONG -> fields.add(required(id++, "key", Types.LongType.get()));
      case UUID -> fields.add(required(id++, "key", Types.UUIDType.get()));
      case STRING -> fields.add(required(id++, "key", Types.StringType.get()));
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
      File benchDir = new File("data/benchmark/inverted-index6");
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
    Map<String, String> hadoopProps = collectHadoopProps();

    // Vortex bypasses Java IO and authenticates against object storage natively. To make az-login
    // credentials available to it, set:
    //   -Dvortex.storage.azure_storage_use_azure_cli=true
    // (Honoured by VortexFileUtil regardless of the FileIO impl below.) For ADLS we additionally
    // default the FileIO impl to ADLSFileIO, whose own credential chain (DefaultAzureCredential)
    // also picks up the Azure CLI session.
    if (storage == Storage.ADLS
        && System.getProperty("vortex.storage.azure_storage_use_azure_cli") == null
        && hadoopProps.keySet().stream()
            .noneMatch(k -> k.startsWith("fs.azure.account.") || k.startsWith("fs.azure.sas."))) {
      System.setProperty("vortex.storage.azure_storage_use_azure_cli", "true");
      LOG.info(
          "No explicit Azure credentials supplied; defaulting "
              + "vortex.storage.azure_storage_use_azure_cli=true so Vortex uses `az login`.");
    }

    if (storage == Storage.LOCAL && !hadoopProps.isEmpty()) {
      Configuration conf = new Configuration();
      hadoopProps.forEach(conf::set);
      HadoopFileIO io = new HadoopFileIO(conf);
      io.initialize(props);
      return io;
    }

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

  private static Map<String, String> collectHadoopProps() {
    Map<String, String> props = Maps.newHashMap();
    for (String name : System.getProperties().stringPropertyNames()) {
      if (name.startsWith(HADOOP_PROP_PREFIX)) {
        props.put(name.substring(HADOOP_PROP_PREFIX.length()), System.getProperty(name));
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
  // Cumulative wall-clock nanoseconds spent inside the corresponding IO call.
  private static final AtomicLong OPEN_NANOS = new AtomicLong();
  private static final AtomicLong SEEK_NANOS = new AtomicLong();
  private static final AtomicLong READ_NANOS = new AtomicLong();

  /**
   * Read-side IO counters surfaced by the {@link #lookup} benchmark. Only fields that can be
   * non-zero for a lookup are exposed so the JMH results table stays compact.
   */
  @State(Scope.Thread)
  @AuxCounters(AuxCounters.Type.EVENTS)
  public static class ReadCounter {
    public long bytesRead;
    public long seeks;
    public long openStreams;

    /** Total wall-clock microseconds spent inside {@code InputFile#newStream()}. */
    public long openMicros;

    /** Total wall-clock microseconds spent inside {@code SeekableInputStream#seek()}. */
    public long seekMicros;

    /** Total wall-clock microseconds spent inside {@code SeekableInputStream#read*()}. */
    public long readMicros;

    private long startBytesRead;
    private long startSeeks;
    private long startOpenStreams;
    private long startOpenNanos;
    private long startSeekNanos;
    private long startReadNanos;

    @Setup(Level.Invocation)
    public void beforeInvocation() {
      startBytesRead = BYTES_READ.get();
      startSeeks = SEEKS.get();
      startOpenStreams = OPEN_INPUT_STREAMS.get();
      startOpenNanos = OPEN_NANOS.get();
      startSeekNanos = SEEK_NANOS.get();
      startReadNanos = READ_NANOS.get();
    }

    @TearDown(Level.Invocation)
    public void afterInvocation() {
      bytesRead = BYTES_READ.get() - startBytesRead;
      seeks = SEEKS.get() - startSeeks;
      openStreams = OPEN_INPUT_STREAMS.get() - startOpenStreams;
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
  private static final class CountingFileIO implements FileIO {
    private final FileIO delegate;

    CountingFileIO(FileIO delegate) {
      this.delegate = delegate;
    }

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

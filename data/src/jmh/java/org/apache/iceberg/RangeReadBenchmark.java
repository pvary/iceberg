/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */
package org.apache.iceberg;

import static org.apache.iceberg.azure.AzureProperties.ADLS_LAZY_OPEN;
import static org.apache.iceberg.azure.AzureProperties.ADLS_READ_BLOCK_SIZE;

import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
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
 * JMH benchmark that isolates the raw {@link FileIO} download time of a single byte range, with no
 * Iceberg/Parquet decoding overhead.
 *
 * <p>The benchmark writes one large dummy-data file per storage configuration (default {@value
 * #DEFAULT_FILE_BYTES} bytes, override with {@code -Drange.bench.fileBytes=<bytes>}) and then, for
 * each {@link #blockBytes} setting, repeatedly downloads a single contiguous block of that size
 * starting at a random offset inside the file. To defeat any server-side range cache, offsets are:
 *
 * <ul>
 *   <li>uniformly distributed across the entire file (only constrained so the block fits);
 *   <li>shuffled into a random visit order;
 *   <li>aligned to the block size, so the (#regions / block size) addressable regions for a given
 *       block size are visited at most once per pass (no repeats inside the measured window for
 *       practical {@code numLookups}).
 * </ul>
 *
 * <p>For each block-size run the benchmark sets {@link
 * org.apache.iceberg.azure.AzureProperties#ADLS_READ_BLOCK_SIZE} to the same value so the Azure SDK
 * issues exactly one HTTP GET of the requested size (paired with {@link
 * org.apache.iceberg.azure.AzureProperties#ADLS_LAZY_OPEN}={@code true} so the constructor doesn't
 * eagerly open with the SDK default). On non-ADLS storages the property is harmlessly ignored.
 *
 * <p>Run with for example:
 *
 * <pre>
 *   ./gradlew :iceberg-data:jmh -PjmhIncludeRegex=RangeReadBenchmark \
 *       -PjmhOutputPath=data/benchmark/range_read.txt \
 *       -Drange.bench.storage=ADLS \
 *       -Drange.bench.location=abfss://container@account.dfs.core.windows.net/iceberg-bench/range \
 *       -Drange.bench.io.adls.sas-token.account=...
 * </pre>
 *
 * <p>Configuration properties (mirrors {@link InvertedIndexBenchmark}):
 *
 * <ul>
 *   <li>{@code -Drange.bench.storage=LOCAL|S3|ADLS} (default {@code LOCAL})
 *   <li>{@code -Drange.bench.location=<base-uri>}
 *   <li>{@code -Drange.bench.io.<key>=<value>} - forwarded to the {@link FileIO}
 *   <li>{@code -Drange.bench.fileBytes=<bytes>} - dummy file size, default 4 GiB
 *   <li>{@code -Drange.bench.reuseFile=true|false} - if {@code true} (default) and a file with the
 *       expected size already exists at the target location, skip the upload step.
 * </ul>
 */
@Fork(1)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class RangeReadBenchmark {
  private static final Logger LOG = LoggerFactory.getLogger(RangeReadBenchmark.class);

  private static final String STORAGE_PROP = "range.bench.storage";
  private static final String LOCATION_PROP = "range.bench.location";
  private static final String IO_PROP_PREFIX = "range.bench.io.";
  private static final String FILE_BYTES_PROP = "range.bench.fileBytes";
  private static final String REUSE_FILE_PROP = "range.bench.reuseFile";

  /** Default size of the dummy data file: 4 GiB. */
  private static final long DEFAULT_FILE_BYTES = 4L * 1024L * 1024L * 1024L;

  /** Number of distinct random offsets pre-generated per block-size run. */
  private static final int NUM_LOOKUPS = 1024;

  private static final long SEED = 1234567890L;

  /**
   * Block size in bytes for a single range download. Encoded as a string so JMH renders it nicely
   * in the results table.
   */
  @Param({
    "262144", // 256 KiB
//    "524288", // 512 KiB
//    "1048576", // 1 MiB
//    "2097152", // 2 MiB
//    "4194304", // 4 MiB
//    "16777216", // 16 MiB
//    "33554432", // 32 MiB
//    "67108864" // 64 MiB
  })
  private int blockBytes;

  private FileIO io;
  private String fileLocation;
  private long fileBytes;
  private long[] offsets;
  private int cursor;
  private byte[] buffer;

  @Setup
  public void setupBenchmark() throws Exception {
    this.fileBytes = Long.getLong(FILE_BYTES_PROP, DEFAULT_FILE_BYTES);
    if (fileBytes < blockBytes) {
      throw new IllegalArgumentException(
          "File size (" + fileBytes + ") must be >= blockBytes (" + blockBytes + ")");
    }

    this.io = createFileIO();
    this.fileLocation = joinPath(baseLocation(), fileName());
    this.buffer = new byte[blockBytes];

    boolean reuse = Boolean.parseBoolean(System.getProperty(REUSE_FILE_PROP, "true"));
    boolean haveFile = false;
    if (reuse) {
      try {
        InputFile existing = io.newInputFile(fileLocation);
        if (existing.exists() && existing.getLength() == fileBytes) {
          haveFile = true;
          LOG.info("Reusing existing dummy file: {} ({} bytes)", fileLocation, fileBytes);
        }
      } catch (RuntimeException ignore) {
        // fall through and rewrite
      }
    }

    if (!haveFile) {
      writeDummyFile();
    }

    // Pre-generate random, block-aligned, non-repeating offsets covering the whole file.
    long maxAlignedOffset = (fileBytes - blockBytes) / blockBytes; // inclusive
    int numRegions = (int) Math.min(maxAlignedOffset + 1, (long) NUM_LOOKUPS * 4);
    Random rand = new Random(SEED ^ blockBytes);
    // Spread base offsets across the full file then shuffle so the visit order is non-sequential.
    long[] all = new long[numRegions];
    long step = (maxAlignedOffset + 1) / numRegions;
    if (step < 1) {
      step = 1;
    }
    for (int i = 0; i < numRegions; i++) {
      // Spread base offsets across the full file, then jitter inside the stride to avoid any
      // strictly-uniform pattern that a prefetcher could lock onto.
      long base = (long) i * step;
      long jitter = step > 1 ? Math.floorMod(rand.nextLong(), step) : 0L;
      long alignedBlocks = Math.min(maxAlignedOffset, base + jitter);
      all[i] = alignedBlocks * blockBytes;
    }

    // Shuffle to get a non-sequential visit order (defeats both client and server prefetch).
    for (int i = all.length - 1; i > 0; i--) {
      int j = rand.nextInt(i + 1);
      long tmp = all[i];
      all[i] = all[j];
      all[j] = tmp;
    }

    int n = Math.min(NUM_LOOKUPS, all.length);
    this.offsets = new long[n];
    System.arraycopy(all, 0, offsets, 0, n);

    LOG.info(
        "RangeReadBenchmark ready: file={} ({} bytes), blockBytes={}, offsets={}",
        fileLocation,
        fileBytes,
        blockBytes,
        offsets.length);
  }

  @TearDown
  public void tearDown() {
    if (io != null) {
      try {
        io.close();
      } catch (Exception e) {
        LOG.warn("Failed to close FileIO", e);
      }
    }
  }

  /**
   * Single-shot range download: pick the next offset, open the file, read exactly {@link
   * #blockBytes} bytes, close. Each invocation hits a new aligned region (offsets are pre-shuffled
   * and never repeat within a measurement window), so neither client nor server side caches help.
   */
  @Benchmark
  @Threads(1)
  @Warmup(iterations = 1)
  @Measurement(iterations = 5)
  public void rangeRead(Blackhole bh, ReadCounter counter) throws IOException {
    long offset = offsets[cursor++ % offsets.length];
    InputFile in = io.newInputFile(fileLocation, fileBytes);
    try (SeekableInputStream stream = in.newStream()) {
      if (stream instanceof RangeReadable rangeReadable) {
        rangeReadable.readFully(offset, buffer, 0, blockBytes);
      } else {
        stream.seek(offset);
        int read = 0;
        while (read < blockBytes) {
          int n = stream.read(buffer, read, blockBytes - read);
          if (n < 0) {
            throw new IOException(
                "Premature EOF at offset " + (offset + read) + " of " + fileBytes);
          }
          read += n;
        }
      }
    }
    counter.bytesRead = blockBytes;
    bh.consume(buffer);
  }

  // --------------------------------------------------------------------------
  // file generation
  // --------------------------------------------------------------------------

  private void writeDummyFile() throws IOException {
    long start = System.nanoTime();
    OutputFile out = io.newOutputFile(fileLocation);
    // Use a 4 MiB chunk filled with pseudo-random bytes; reuse across writes so we don't pay the
    // RNG cost for every chunk while still producing data that doesn't compress to nothing.
    int chunk = 4 * 1024 * 1024;
    byte[] data = new byte[chunk];
    new Random(SEED).nextBytes(data);
    try (PositionOutputStream stream = out.createOrOverwrite()) {
      long written = 0;
      while (written < fileBytes) {
        int n = (int) Math.min(chunk, fileBytes - written);
        stream.write(data, 0, n);
        written += n;
      }
    }

    LOG.info(
        "Wrote dummy file {} ({} bytes) in {} ms",
        fileLocation,
        fileBytes,
        (System.nanoTime() - start) / 1_000_000);
  }

  private String fileName() {
    return String.format(Locale.ROOT, "range-bench-%dbytes.bin", fileBytes);
  }

  // --------------------------------------------------------------------------
  // storage backend selection (mirrors InvertedIndexBenchmark)
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
      File benchDir = new File("data/benchmark/range-read");
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

    if (storage == Storage.ADLS) {
      // Force the SDK to issue exactly one HTTP GET sized to our block, and avoid the eager
      // unbounded open in the constructor.
      props.put(ADLS_READ_BLOCK_SIZE, Integer.toString(blockBytes));
      props.putIfAbsent(ADLS_LAZY_OPEN, "true");
      LOG.info("ADLS tuning: {}={} {}=true", ADLS_READ_BLOCK_SIZE, blockBytes, ADLS_LAZY_OPEN);
      LOG.info("Using FileIO impl=ADLSFileIO (counting clientSupplier) props={}", props.keySet());
      return new CountingFileIO(buildAdlsFileIO(props));
    }

    String impl =
        switch (storage) {
          case LOCAL -> "org.apache.iceberg.hadoop.HadoopFileIO";
          case S3 -> "org.apache.iceberg.aws.s3.S3FileIO";
          default -> throw new IllegalStateException("unreachable");
        };

    LOG.info("Using FileIO impl={} props={}", impl, props.keySet());
    return new CountingFileIO(CatalogUtil.loadFileIO(impl, props, null));
  }

  /**
   * Builds an {@link org.apache.iceberg.azure.adlsv2.ADLSFileIO} via its {@code clientSupplier}
   * constructor (using a package-local helper to access the package-private {@code ADLSLocation})
   * and attaches a counting {@link com.azure.core.http.policy.HttpPipelinePolicy} so we can report
   * the exact number of HTTP requests (and bytes) the Azure SDK issued per JMH invocation.
   */
  private static FileIO buildAdlsFileIO(Map<String, String> props) {
    return org.apache.iceberg.azure.adlsv2.CountingAdlsFileIOFactory.create(
        props, new CountingHttpPolicy());
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

  // --------------------------------------------------------------------------
  // IO instrumentation
  // --------------------------------------------------------------------------

  private static final AtomicLong BYTES_READ = new AtomicLong();
  private static final AtomicLong OPEN_INPUT_STREAMS = new AtomicLong();
  private static final AtomicLong OPEN_NANOS = new AtomicLong();
  private static final AtomicLong READ_NANOS = new AtomicLong();
  static final AtomicLong ADLS_HTTP_REQUESTS = new AtomicLong();
  static final AtomicLong ADLS_HTTP_RESPONSE_BYTES = new AtomicLong();
  static final AtomicLong ADLS_HTTP_NANOS = new AtomicLong();

  @State(Scope.Thread)
  @AuxCounters(AuxCounters.Type.EVENTS)
  public static class ReadCounter {
    /** Bytes downloaded by the current invocation. */
    public long bytesRead;

    /** Number of {@code InputFile#newStream()} calls during this invocation. */
    public long openStreams;

    /** Microseconds spent inside {@code InputFile#newStream()}. */
    public long openMicros;

    /** Microseconds spent inside read* / readFully on the input stream. */
    public long readMicros;

    /** Number of HTTP requests the Azure SDK issued during this invocation (ADLS only). */
    public long adlsRequests;

    /** Total response body bytes for ADLS HTTP responses observed this invocation. */
    public long adlsResponseBytes;

    /** Microseconds spent waiting on ADLS HTTP responses (policy-level wall time). */
    public long adlsHttpMicros;

    private long startBytesRead;
    private long startOpenStreams;
    private long startOpenNanos;
    private long startReadNanos;
    private long startAdlsRequests;
    private long startAdlsResponseBytes;
    private long startAdlsHttpNanos;

    @Setup(Level.Invocation)
    public void beforeInvocation() {
      startBytesRead = BYTES_READ.get();
      startOpenStreams = OPEN_INPUT_STREAMS.get();
      startOpenNanos = OPEN_NANOS.get();
      startReadNanos = READ_NANOS.get();
      startAdlsRequests = ADLS_HTTP_REQUESTS.get();
      startAdlsResponseBytes = ADLS_HTTP_RESPONSE_BYTES.get();
      startAdlsHttpNanos = ADLS_HTTP_NANOS.get();
    }

    @TearDown(Level.Invocation)
    public void afterInvocation() {
      // bytesRead is set explicitly by the benchmark; we still report the IO-level total so
      // any extra (non-payload) bytes the SDK fetched show up here.
      long io = BYTES_READ.get() - startBytesRead;
      if (io > bytesRead) {
        bytesRead = io;
      }
      adlsRequests = ADLS_HTTP_REQUESTS.get() - startAdlsRequests;
      adlsResponseBytes = ADLS_HTTP_RESPONSE_BYTES.get() - startAdlsResponseBytes;
      adlsHttpMicros = (ADLS_HTTP_NANOS.get() - startAdlsHttpNanos) / 1_000L;
      openStreams = OPEN_INPUT_STREAMS.get() - startOpenStreams;
      openMicros = (OPEN_NANOS.get() - startOpenNanos) / 1_000L;
      readMicros = (READ_NANOS.get() - startReadNanos) / 1_000L;
    }
  }

  // --------------------------------------------------------------------------
  // Counting FileIO decorator (mirrors InvertedIndexBenchmark)
  // --------------------------------------------------------------------------

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
      return delegate.newOutputFile(path);
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

  private static final class CountingSeekableInputStream extends SeekableInputStream
      implements RangeReadable {
    private final SeekableInputStream delegate;
    private final RangeReadable range;

    CountingSeekableInputStream(SeekableInputStream delegate) {
      this.delegate = delegate;
      this.range = delegate instanceof RangeReadable rangeReadable ? rangeReadable : null;
    }

    @Override
    public long getPos() throws IOException {
      return delegate.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
      delegate.seek(newPos);
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
    public void readFully(long position, byte[] buffer, int offset, int length)
        throws IOException {
      long t0 = System.nanoTime();
      try {
        if (range != null) {
          range.readFully(position, buffer, offset, length);
        } else {
          delegate.seek(position);
          int read = 0;
          while (read < length) {
            int n = delegate.read(buffer, offset + read, length - read);
            if (n < 0) {
              throw new IOException("Premature EOF");
            }
            read += n;
          }
        }
      } finally {
        READ_NANOS.addAndGet(System.nanoTime() - t0);
      }
      BYTES_READ.addAndGet(length);
    }

    @Override
    public int readTail(byte[] buffer, int offset, int length) throws IOException {
      if (range != null) {
        long t0 = System.nanoTime();
        try {
          int n = range.readTail(buffer, offset, length);
          BYTES_READ.addAndGet(Math.max(n, 0));
          return n;
        } finally {
          READ_NANOS.addAndGet(System.nanoTime() - t0);
        }
      }
      throw new UnsupportedOperationException("Underlying stream does not support readTail");
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }

  // --------------------------------------------------------------------------
  // ADLS HTTP request counting policy
  // --------------------------------------------------------------------------

  /**
   * {@link com.azure.core.http.policy.HttpPipelinePolicy} that counts every HTTP request the Azure
   * SDK issues as well as the response body length. Installed once per {@code
   * DataLakeFileSystemClient} we build for the benchmark.
   */
  private static final class CountingHttpPolicy
      implements com.azure.core.http.policy.HttpPipelinePolicy {
    @Override
    public reactor.core.publisher.Mono<com.azure.core.http.HttpResponse> process(
        com.azure.core.http.HttpPipelineCallContext context,
        com.azure.core.http.HttpPipelineNextPolicy next) {
      ADLS_HTTP_REQUESTS.incrementAndGet();
      long t0 = System.nanoTime();
      return next.process()
          .doOnSuccess(
              response -> {
                ADLS_HTTP_NANOS.addAndGet(System.nanoTime() - t0);
                if (response != null) {
                  String len =
                      response.getHeaderValue(com.azure.core.http.HttpHeaderName.CONTENT_LENGTH);
                  if (len != null) {
                    try {
                      ADLS_HTTP_RESPONSE_BYTES.addAndGet(Long.parseLong(len));
                    } catch (NumberFormatException ignore) {
                      // skip
                    }
                  }
                }
              })
          .doOnError(err -> ADLS_HTTP_NANOS.addAndGet(System.nanoTime() - t0));
    }
  }
}













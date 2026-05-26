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
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
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
 * JMH benchmark that isolates the raw {@link FileIO} download time of a single byte range, with no
 * Iceberg/Parquet decoding overhead.
 *
 * <p>See class header in version control for usage; configuration properties are unchanged.
 */
@Fork(1)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
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

  @Param({
    "262144", // 256 KiB
    "524288", // 512 KiB
    "1048576", // 1 MiB
    "2097152", // 2 MiB
    "3145728", // 3 MiB
    "4194304", // 4 MiB
    "4194305", // 4 MiB +1
    "5242880", // 5 MiB
    "6291456", // 6 MiB
    "7340032", // 7 MiB
    "8388608", // 8 MiB
    "16777216", // 16 MiB
    "33554432", // 32 MiB
    "67108864" // 64 MiB
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
    long[] all = new long[numRegions];
    long step = (maxAlignedOffset + 1) / numRegions;
    if (step < 1) {
      step = 1;
    }
    for (int i = 0; i < numRegions; i++) {
      long base = (long) i * step;
      long jitter = step > 1 ? Math.floorMod(rand.nextLong(), step) : 0L;
      long alignedBlocks = Math.min(maxAlignedOffset, base + jitter);
      all[i] = alignedBlocks * blockBytes;
    }

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
  @Warmup(iterations = 5)
  @Measurement(iterations = 50)
  public void rangeRead(Blackhole bh) throws IOException {
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
    bh.consume(buffer);
  }

  // --------------------------------------------------------------------------
  // file generation
  // --------------------------------------------------------------------------

  private void writeDummyFile() throws IOException {
    long start = System.nanoTime();
    OutputFile out = io.newOutputFile(fileLocation);
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
      props.put(ADLS_READ_BLOCK_SIZE, Integer.toString(blockBytes));
      props.putIfAbsent(ADLS_LAZY_OPEN, "true");
      LOG.info("ADLS tuning: {}={} {}=true", ADLS_READ_BLOCK_SIZE, blockBytes, ADLS_LAZY_OPEN);
    }

    String impl =
        switch (storage) {
          case LOCAL -> "org.apache.iceberg.hadoop.HadoopFileIO";
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
}

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

import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.index.MetadataHandler;
import org.apache.iceberg.index.MumblingBitmap;
import org.apache.iceberg.index.ParquetMetadataHandler;
import org.apache.iceberg.io.OutputFile;
import org.openjdk.jmh.annotations.AuxCounters;
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
 * JMH benchmark that writes one synthetic metadata file per {@link MetadataHandler} implementation
 * and reports the resulting on-disk size as the primary metric of interest.
 *
 * <p>Write throughput is intentionally <em>not</em> the focus -- the benchmark uses a single
 * measurement iteration with no warmup. The aux counter {@code write:bytes} produced by {@link
 * SizeCounter} is the number to compare across {@code handler} parameters.
 *
 * <p>Inputs are synthetic {@code (filePath, updateFilePath, minValue, maxValue)} triples:
 *
 * <ul>
 *   <li>{@code filePath} and {@code updateFilePath} are drawn as consecutive pairs from one
 *       synthetic {@code s3://.../data/<n>.parquet} path sequence, so row {@code i} uses paths
 *       {@code 2*i} and {@code 2*i + 1}.
 *   <li>{@code minValue} is a long drawn from a seeded RNG.
 *   <li>{@code maxValue} is greater than or equal to {@code minValue} by a seeded random width.
 *   <li>Bitmap handler variants also generate one serialized random {@link MumblingBitmap} per data
 *       file during the write phase, marking 5%, 10%, or 20% of 400,000 rows.
 * </ul>
 *
 * <p>Run with:
 *
 * <pre>
 *   ./gradlew :iceberg-data:jmh \
 *       -PjmhIncludeRegex=IndexMetadataBenchmark \
 *       -PjmhOutputPath=data/benchmark/index_metadata.txt
 * </pre>
 */
@Fork(1)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 0)
@Measurement(iterations = 1)
public class IndexMetadataBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(IndexMetadataBenchmark.class);

  private static final long SEED = 42L;

  private static final int BITMAP_ROW_COUNT = 400_000;
  private static final int BITMAP_PERMUTATION_MASK = 0x7FFFF;

  /** Unpartitioned-table data directory used in synthetic Spark-style paths. */
  private static final String DATA_LOCATION = "s3://bucket/warehouse/db/tbl/data";

  public enum HandlerType {
    PARQUET_ZSTD,
    PARQUET_SNAPPY,
    PARQUET_ZSTD_BITMAP_5,
    PARQUET_ZSTD_BITMAP_10,
    PARQUET_ZSTD_BITMAP_20,
    PARQUET_SNAPPY_BITMAP_5,
    PARQUET_SNAPPY_BITMAP_10,
    PARQUET_SNAPPY_BITMAP_20
  }

  @Param({
    "PARQUET_ZSTD",
    "PARQUET_SNAPPY",
    "PARQUET_ZSTD_BITMAP_5",
    "PARQUET_ZSTD_BITMAP_10",
    "PARQUET_ZSTD_BITMAP_20",
    "PARQUET_SNAPPY_BITMAP_5",
    "PARQUET_SNAPPY_BITMAP_10",
    "PARQUET_SNAPPY_BITMAP_20"
  })
  private HandlerType handler;

  /** Total number of metadata entries written per benchmark invocation. */
  @Param({"1000"})
  private int numEntries;

  private String[] sourceFilePaths;
  private String[] updateFilePaths;
  private long[] entryMinValues;
  private long[] entryMaxValues;

  /**
   * Output directory for generated metadata files. Mirrors {@code InvertedIndexBenchmark}'s
   * convention of writing to {@code data/benchmark/<name>} (relative to the working directory the
   * JMH task is launched from) so the files survive the JVM and can be inspected after the run.
   */
  private static final File OUTPUT_DIR = new File("data/benchmark/iceberg-index-metadata-bench2");

  @Setup
  public void setupBenchmark() throws IOException {
    if (!OUTPUT_DIR.exists() && !OUTPUT_DIR.mkdirs()) {
      throw new IOException("Failed to create output dir: " + OUTPUT_DIR.getAbsolutePath());
    }

    // One source file and one update file per metadata entry. Both are drawn from the same path
    // sequence as consecutive pairs: (0, 1), (2, 3), (4, 5), etc.
    String[] filePaths = bucketFilePaths(numEntries * 2);
    this.sourceFilePaths = new String[numEntries];
    this.updateFilePaths = new String[numEntries];
    for (int i = 0; i < numEntries; i++) {
      sourceFilePaths[i] = filePaths[2 * i];
      updateFilePaths[i] = filePaths[2 * i + 1];
    }

    Random rand = new Random(SEED);
    this.entryMinValues = new long[numEntries];
    this.entryMaxValues = new long[numEntries];

    // minValue is strictly monotonically increasing across all entries (positive random delta)
    // so delta encoders can compress it well and the values resemble a clustered/sorted column
    // like a row position or a timestamp. maxValue is a small seeded range above minValue.
    long minValue = 0L;
    for (int i = 0; i < numEntries; i++) {
      // Strictly positive delta in [1, 1048576]
      minValue += 1L + rand.nextInt(1048576);
      entryMinValues[i] = minValue;
      entryMaxValues[i] = minValue + rand.nextInt(1048576);
    }

    LOG.info(
        "IndexMetadataBenchmark setup: handler={} numEntries={} outputDir={}",
        handler,
        numEntries,
        OUTPUT_DIR.getAbsolutePath());
  }

  @TearDown
  public void tearDown() {
    // Intentionally keep generated metadata files under OUTPUT_DIR so they survive the JVM and
    // can be inspected after the run (same convention as InvertedIndexBenchmark).
  }

  @Benchmark
  @Threads(1)
  public void write(Blackhole bh, SizeCounter sizeCounter) throws Exception {
    MetadataHandler h = newHandler(handler);
    File target = new File(OUTPUT_DIR, metadataFileName(handler, numEntries));
    OutputFile output = Files.localOutput(target);

    try (MetadataHandler.Writer writer = h.writer(output)) {
      Random bitmapRand = new Random(SEED ^ 0xB17A_0000_0000_0000L ^ handler.name().hashCode());
      boolean writeBitmap = isBitmapHandler(handler);
      int bitmapMarkedPercent = bitmapMarkedPercent(handler);
      for (int i = 0; i < numEntries; i++) {
        if (writeBitmap) {
          writer.add(
              sourceFilePaths[i],
              updateFilePaths[i],
              entryMinValues[i],
              entryMaxValues[i],
              randomSerializedBitmap(bitmapRand, bitmapMarkedPercent));
        } else {
          writer.add(sourceFilePaths[i], updateFilePaths[i], entryMinValues[i], entryMaxValues[i]);
        }
      }
    }

    long bytes = target.length();
    sizeCounter.bytes = bytes;
    sizeCounter.bytesPerEntry = (bytes * 1000L) / numEntries; // milli-B/entry

    LOG.info(
        "{} wrote {} entries to {} ({} bytes, {} bits/entry)",
        handler,
        numEntries,
        target.getName(),
        bytes,
        (bytes * 8L) / numEntries);

    bh.consume(target);
  }

  // --------------------------------------------------------------------------
  // helpers
  // --------------------------------------------------------------------------

  private static MetadataHandler newHandler(HandlerType type) {
    return switch (type) {
      case PARQUET_ZSTD -> new ParquetMetadataHandler("zstd");
      case PARQUET_SNAPPY -> new ParquetMetadataHandler("snappy");
      case PARQUET_ZSTD_BITMAP_5, PARQUET_ZSTD_BITMAP_10, PARQUET_ZSTD_BITMAP_20 ->
          new ParquetMetadataHandler("zstd", true);
      case PARQUET_SNAPPY_BITMAP_5, PARQUET_SNAPPY_BITMAP_10, PARQUET_SNAPPY_BITMAP_20 ->
          new ParquetMetadataHandler("snappy", true);
    };
  }

  private static boolean isBitmapHandler(HandlerType type) {
    return switch (type) {
      case PARQUET_ZSTD, PARQUET_SNAPPY -> false;
      case PARQUET_ZSTD_BITMAP_5,
              PARQUET_ZSTD_BITMAP_10,
              PARQUET_ZSTD_BITMAP_20,
              PARQUET_SNAPPY_BITMAP_5,
              PARQUET_SNAPPY_BITMAP_10,
              PARQUET_SNAPPY_BITMAP_20 ->
          true;
    };
  }

  private static int bitmapMarkedPercent(HandlerType type) {
    return switch (type) {
      case PARQUET_ZSTD, PARQUET_SNAPPY -> 0;
      case PARQUET_ZSTD_BITMAP_5, PARQUET_SNAPPY_BITMAP_5 -> 5;
      case PARQUET_ZSTD_BITMAP_10, PARQUET_SNAPPY_BITMAP_10 -> 10;
      case PARQUET_ZSTD_BITMAP_20, PARQUET_SNAPPY_BITMAP_20 -> 20;
    };
  }

  private static byte[] randomSerializedBitmap(Random rand, int markedPercent) {
    MumblingBitmap bitmap = new MumblingBitmap();

    int markedRows = (BITMAP_ROW_COUNT * markedPercent) / 100;
    long seed = rand.nextLong();

    for (int i = 0; i < markedRows; i++) {
      bitmap.set(permutedBitmapRow(i, seed));
    }

    return bitmap.serialize();
  }

  private static int permutedBitmapRow(int row, long seed) {
    int permuted = row;
    do {
      permuted = permutePowerOfTwo(permuted, seed);
    } while (permuted >= BITMAP_ROW_COUNT);

    return permuted;
  }

  private static int permutePowerOfTwo(int value, long seed) {
    int x = value & BITMAP_PERMUTATION_MASK;
    x ^= (int) seed;
    x &= BITMAP_PERMUTATION_MASK;
    x ^= x >>> 11;
    x = (x * 0x5BD1E995) & BITMAP_PERMUTATION_MASK;
    x ^= (int) (seed >>> 32);
    x &= BITMAP_PERMUTATION_MASK;
    x ^= x >>> 9;
    x = (x * 0x27D4EB2D) & BITMAP_PERMUTATION_MASK;
    x ^= x >>> 10;
    return x & BITMAP_PERMUTATION_MASK;
  }

  /**
   * Deterministic file name encoding the full benchmark configuration: handler kind and total entry
   * count. No UUID -- re-running the same config overwrites the previous file so the output
   * directory does not accumulate stale variants.
   */
  private static String metadataFileName(HandlerType type, int numEntries) {
    String ext = "parquet";
    return String.format(
        Locale.ROOT,
        "metadata-%s-entries%d.%s",
        type.name().toLowerCase(Locale.ROOT),
        numEntries,
        ext);
  }

  private static String[] bucketFilePaths(int numSourceFiles) {
    String[] paths = new String[numSourceFiles];
    for (int i = 0; i < numSourceFiles; i++) {
      paths[i] = DATA_LOCATION + "/" + i + ".parquet";
    }

    return paths;
  }

  /** Reports the on-disk metadata size as a JMH aux metric ({@code write:bytes}). */
  @State(Scope.Thread)
  @AuxCounters(AuxCounters.Type.EVENTS)
  public static class SizeCounter {
    /** On-disk size of the produced metadata file, in bytes. */
    public long bytes;

    /** Bits per metadata entry * 1000, so JMH prints a useful integer (milli-bits / entry). */
    public long bytesPerEntry;
  }
}

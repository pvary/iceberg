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

import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.index.AvroMetadataHandler;
import org.apache.iceberg.index.DictionaryBinaryMetadataHandler;
import org.apache.iceberg.index.MetadataHandler;
import org.apache.iceberg.index.ParquetMetadataHandler;
import org.apache.iceberg.index.PlainBinaryMetadataHandler;
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
 * <p>Inputs are synthetic {@code (filePath, offset, minValue)} triples:
 *
 * <ul>
 *   <li>{@code filePath} is drawn from a pool sized {@code ceil(numEntries / blocksPerFile)} of
 *       synthetic {@code s3://.../data/<n>.parquet} paths.
 *   <li>{@code offset} is a row position monotonically increasing within each source file.
 *   <li>{@code minValue} is a long drawn from a seeded RNG.
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

  /** Unpartitioned-table data directory used in synthetic Spark-style paths. */
  private static final String DATA_LOCATION = "s3://bucket/warehouse/db/tbl/data";

  /**
   * Approximate byte stride between consecutive blocks inside a single source file (~4 MiB). Used
   * to assign realistic absolute offsets when {@code blocksPerFile > 1}.
   */
  private static final long BLOCK_STRIDE_BYTES = 4L * 1024 * 1024;

  public enum HandlerType {
    PLAIN_BINARY,
    DICT_BINARY,
    PARQUET_ZSTD,
    PARQUET_SNAPPY,
    AVRO_GZIP,
    AVRO_ZSTD,
    AVRO_SNAPPY,
    AVRO_UNCOMPRESSED
  }

//  @Param({"PLAIN_BINARY", "DICT_BINARY", "PARQUET_ZSTD", "AVRO_GZIP"})
@Param({"AVRO_GZIP"})
  private HandlerType handler;

  /** Total number of metadata entries (blocks) written per benchmark invocation. */
  @Param({"1000000", "100000", "10000", "1000"})
  private int numEntries;

  /**
   * How many blocks share a single source data file.
   *
   * <ul>
   *   <li>{@code 1} - 1:1 mapping between blocks and files. Handlers are instantiated with {@code
   *       storeOffsets=false}, i.e. the {@code offset} column is omitted entirely.
   *   <li>{@code 16 / 32 / 64 / 128} - many blocks per file. Handlers store offsets that step by
   *       roughly {@link #BLOCK_STRIDE_BYTES} (~4 MiB) inside each file.
   * </ul>
   */
  @Param({"1", "16", "32", "64", "128"})
  private int blocksPerFile;

  private String[] sourceFilePaths;
  private int[] entryPathIdx;
  private long[] entryOffsets;
  private long[] entryMinValues;

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

    // One source file per `blocksPerFile` blocks. ceil division so we always have enough paths.
    int numSourceFiles = (numEntries + blocksPerFile - 1) / blocksPerFile;
    this.sourceFilePaths = bucketFilePaths(numSourceFiles);

    Random rand = new Random(SEED);
    this.entryPathIdx = new int[numEntries];
    this.entryOffsets = new long[numEntries];
    this.entryMinValues = new long[numEntries];

    // Lay blocks out file-by-file: blocks 0..blocksPerFile-1 belong to file 0 at offsets
    // ~ k * BLOCK_STRIDE_BYTES (with a small +/- jitter so consecutive offsets are not perfectly
    // periodic and the dictionary/delta encoders still see a realistic distribution).
    // minValue is strictly monotonically increasing across all entries (positive random delta)
    // so delta encoders can compress it well and the values resemble a clustered/sorted column
    // like a row position or a timestamp.
    long minValue = 0L;
    for (int i = 0; i < numEntries; i++) {
      int fileIdx = i / blocksPerFile;
      int blockInFile = i % blocksPerFile;
      entryPathIdx[i] = fileIdx;
      if (blocksPerFile == 1) {
        // 1:1 mapping -> offset is unused by the writers (storeOffsets=false). Leave at 0.
        entryOffsets[i] = 0L;
      } else {
        long jitter = (long) rand.nextInt(64 * 1024) - 32 * 1024; // +/- 32 KiB
        entryOffsets[i] = (long) blockInFile * BLOCK_STRIDE_BYTES + jitter;
        if (entryOffsets[i] < 0L) {
          entryOffsets[i] = 0L;
        }
      }
      // Strictly positive delta in [1, 1048576]
      minValue += 1L + rand.nextInt(1048576);
      entryMinValues[i] = minValue;
    }

    LOG.info(
        "IndexMetadataBenchmark setup: handler={} numEntries={} blocksPerFile={} numFiles={}"
            + " outputDir={}",
        handler,
        numEntries,
        blocksPerFile,
        numSourceFiles,
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
    boolean storeOffsets = blocksPerFile > 1;
    MetadataHandler h = newHandler(handler, storeOffsets);
    File target = new File(OUTPUT_DIR, metadataFileName(handler, numEntries, blocksPerFile));
    OutputFile output = Files.localOutput(target);

    try (MetadataHandler.Writer writer = h.writer(output)) {
      for (int i = 0; i < numEntries; i++) {
        writer.add(sourceFilePaths[entryPathIdx[i]], entryOffsets[i], entryMinValues[i]);
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

  private static MetadataHandler newHandler(HandlerType type, boolean storeOffsets) {
    return switch (type) {
      case PLAIN_BINARY -> new PlainBinaryMetadataHandler(storeOffsets);
      case DICT_BINARY -> new DictionaryBinaryMetadataHandler(storeOffsets);
      case PARQUET_ZSTD -> new ParquetMetadataHandler("zstd", 50_000, storeOffsets);
      case PARQUET_SNAPPY -> new ParquetMetadataHandler("snappy", 50_000, storeOffsets);
      case AVRO_GZIP -> new AvroMetadataHandler("gzip", storeOffsets);
      case AVRO_ZSTD -> new AvroMetadataHandler("zstd", storeOffsets);
      case AVRO_SNAPPY -> new AvroMetadataHandler("snappy", storeOffsets);
      case AVRO_UNCOMPRESSED -> new AvroMetadataHandler("uncompressed", storeOffsets);
    };
  }

  /**
   * Deterministic file name encoding the full benchmark configuration: handler kind, total entry
   * count, and blocks-per-file. No UUID -- re-running the same config overwrites the previous file
   * so the output directory does not accumulate stale variants.
   */
  private static String metadataFileName(HandlerType type, int numEntries, int blocksPerFile) {
    String ext =
        switch (type) {
          case PLAIN_BINARY, DICT_BINARY -> "bin";
          case PARQUET_ZSTD, PARQUET_SNAPPY -> "parquet";
          case AVRO_GZIP, AVRO_ZSTD, AVRO_SNAPPY, AVRO_UNCOMPRESSED -> "avro";
        };
    return String.format(
        Locale.ROOT,
        "metadata-%s-entries%d-bpf%d.%s",
        type.name().toLowerCase(Locale.ROOT),
        numEntries,
        blocksPerFile,
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


















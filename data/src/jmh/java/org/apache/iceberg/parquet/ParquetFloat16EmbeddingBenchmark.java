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
package org.apache.iceberg.parquet;

import static org.apache.iceberg.types.Types.NestedField.required;

import com.sun.management.HotSpotDiagnosticMXBean;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.ref.Reference;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.Float16LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
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
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compares storing float16 embeddings in Parquet as FLOAT and as FLOAT16 columns.
 *
 * <p>The benchmark generates {@link #embeddingCount} random float16 embeddings with {@link
 * #dimension} dimensions (see {@link EmbeddingModel}), and runs the matrix of:
 *
 * <ul>
 *   <li>{@link SourceType}: the in-memory representation of the embeddings, which is written from
 *       and read back into: {@code float[]}, or {@code short[]} (float16 bits)
 *   <li>{@link StorageType}: the Parquet type of the list elements: {@code float}, or {@code
 *       fixed_len_byte_array(2)} annotated as {@code FLOAT16}
 *   <li>{@link EncodingType}: dictionary (Iceberg's default), {@code PLAIN}, or {@code
 *       BYTE_STREAM_SPLIT}
 *   <li>{@link #codec}: the compression codec, {@code zstd} (Iceberg's default), or {@code
 *       uncompressed}; other codecs, like {@code lz4_raw} or {@code snappy}, and compression
 *       levels, like {@code zstd:9}, can be passed as JMH parameters
 * </ul>
 *
 * <p>All embeddings have the same dimension, which the benchmark setup fixes: the writer writes
 * lists of exactly that size, the setup verifies that every embedding read back has that size, and
 * the {@code read} benchmark relies on it, deriving Parquet's repetition and definition levels from
 * the position of each value instead of decoding them. To run a subset of the matrix, pass JMH
 * parameters, for example {@code -p source=SHORT_ARRAY -p storage=FLOAT16 -p codec=zstd}.
 *
 * <p>Every representation holds the same float16 values, so all combinations round trip exactly.
 * Results:
 *
 * <ul>
 *   <li>{@code write}: time to write all embeddings to a Parquet file
 *   <li>{@code read}: time to read all embeddings from a Parquet file with Iceberg's Parquet
 *       reader, including reading back every value of the embeddings before they are discarded.
 *       Each embedding is decoded in bulk with {@link ColumnIterator#nextFloats} or {@link
 *       ColumnIterator#nextFloat16s}, using the fixed size from {@link
 *       ColumnIterator#setFixedListSize}, straight into the array of the embedding when the array
 *       holds the decoded type (see {@link #directDecode}). With {@link #reuseContainers}, every
 *       embedding is read into the array of the previous one
 *   <li>{@code sizes:fileBytes}: size of the Parquet file on disk
 *   <li>{@code sizes:heapBytes}: retained heap size of the embeddings in the source representation,
 *       which is also the representation produced by the reader
 * </ul>
 *
 * <p>Every combination writes and reads the same embeddings, generated with a fixed seed, so the
 * times and the file sizes of the combinations are directly comparable.
 *
 * <p>Both storage types are written with Iceberg's Parquet writer and Iceberg's default Parquet
 * write settings, with a value writer that writes all values of an embedding in one loop (see
 * {@link EmbeddingValueWriter}). Iceberg schemas can only store float as FLOAT, so the benchmark
 * passes the Parquet schema to the writer directly. To run the benchmark:
 *
 * <pre>
 * ./gradlew :iceberg-data:jmh
 *     -PjmhIncludeRegex=ParquetFloat16EmbeddingBenchmark
 *     -PjmhOutputPath=benchmark/parquet-float16-embedding-benchmark-result.txt
 * </pre>
 *
 * <p>To use fewer or more embeddings, change the {@code embeddingCount} {@link Param} or pass
 * {@code -p embeddingCount=...} to JMH. The embeddings are held in memory: 100,000 embeddings with
 * 2560 dimensions take 1 GB of heap as {@code float[]} and 512 MB as {@code short[]}, and from
 * about 435 MB (compressed FLOAT16) to 1 GB (uncompressed FLOAT) on disk. The page and row group
 * sizes default to Iceberg's defaults, and can be changed with {@code -p pageSizeBytes=...}, {@code
 * -p rowGroupSizeBytes=...}, and {@code -p sizeCheckMinRows=...}; the log of the setup reports the
 * resulting row groups and pages.
 */
// -Xmx31g overrides the -Xmx32g default to keep compressed oops, so the references to the
// embeddings are measured with the 4 byte references used by most JVMs
@Fork(value = 1, jvmArgsAppend = "-Xmx31g")
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Threads(1)
public class ParquetFloat16EmbeddingBenchmark {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetFloat16EmbeddingBenchmark.class);

  private static final int EMBEDDING_ID = 1;
  private static final int ELEMENT_ID = 2;
  private static final String EMBEDDING = "embedding";
  private static final String LIST = "list";
  private static final String ELEMENT = "element";
  private static final String[] ELEMENT_PATH = {EMBEDDING, LIST, ELEMENT};
  private static final String ELEMENT_COLUMN = String.join(".", ELEMENT_PATH);
  private static final Schema SCHEMA =
      new Schema(
          required(EMBEDDING_ID, EMBEDDING, ListType.ofRequired(ELEMENT_ID, FloatType.get())));

  private static final long SEED = 42L;
  private static final int GC_ROUNDS = 5;

  /** In-memory representation of an embedding. */
  public enum SourceType {
    FLOAT_ARRAY,
    /** The bits of the half-precision values. */
    SHORT_ARRAY
  }

  /** Parquet physical/logical type used to store the embedding values. */
  public enum StorageType {
    FLOAT,
    FLOAT16
  }

  /** Parquet encoding of the embedding values. */
  public enum EncodingType {
    /**
     * Dictionary encoding, Iceberg's default. The v1 writer only dictionary encodes FLOAT, and
     * writes FLOAT16 ({@code fixed_len_byte_array}) values as PLAIN.
     */
    DICTIONARY,
    /** PLAIN, with dictionary encoding disabled. */
    PLAIN,
    /** BYTE_STREAM_SPLIT without dictionary encoding, which Iceberg does not expose yet. */
    BYTE_STREAM_SPLIT
  }

  /** Settings that determine how the embeddings are stored. */
  private record WriteOptions(
      StorageType storage,
      EncodingType encoding,
      CompressionCodecName codec,
      String compressionLevel,
      WriterVersion writerVersion,
      boolean columnStatistics,
      int pageSize,
      long rowGroupSize,
      int sizeCheckMinRows) {

    /**
     * Returns the configuration of the writer, like the one {@code Parquet.write(...)} uses for
     * local files, which sets the compression level with the same properties as Iceberg's {@code
     * write.parquet.compression-level}.
     */
    ParquetConfiguration conf() {
      ParquetConfiguration conf = new PlainParquetConfiguration();
      if (compressionLevel != null) {
        switch (codec) {
          case GZIP -> conf.set("zlib.compress.level", compressionLevel);
          case ZSTD -> conf.set("parquet.compression.codec.zstd.level", compressionLevel);
          default ->
              throw new IllegalArgumentException("Compression level is not supported by " + codec);
        }
      }

      return conf;
    }
  }

  /**
   * Settings that determine how the embeddings are read.
   *
   * @param fixedSize whether the reader relies on every embedding having {@code dimension} values,
   *     and derives the Parquet levels instead of decoding them
   * @param reuseContainers whether every embedding is read into the array of the previous one
   * @param directDecode whether values are decoded straight into the array of the embedding, when
   *     the array holds the decoded type, instead of into a buffer that is then copied
   */
  private record ReadOptions(boolean fixedSize, boolean reuseContainers, boolean directDecode) {
    /** Returns these options with a reader that decodes the levels of every value. */
    ReadOptions decodingLevels() {
      return new ReadOptions(false, reuseContainers, directDecode);
    }
  }

  /**
   * Number of embeddings in the Parquet files. Every combination writes and reads the same
   * embeddings, so the times and the file sizes are comparable between the combinations.
   */
  @Param("100000")
  private int embeddingCount;

  /** Number of dimensions of the generated embeddings. */
  @Param("2560")
  private int dimension;

  @Param({"FLOAT_ARRAY", "SHORT_ARRAY"})
  private SourceType source;

  @Param({"FLOAT", "FLOAT16"})
  private StorageType storage;

  @Param({"DICTIONARY", "PLAIN", "BYTE_STREAM_SPLIT"})
  private EncodingType encoding;

  /**
   * Parquet compression codec, as a case-insensitive {@link CompressionCodecName}, optionally
   * followed by a compression level, like {@code zstd:9}. Levels are supported for zstd (the
   * default is 3) and gzip, like Iceberg's {@code write.parquet.compression-level}.
   */
  @Param({"zstd", "uncompressed"})
  private String codec;

  /**
   * Parquet writer version; Iceberg defaults to v1. Parquet only dictionary encodes FLOAT16 ({@code
   * fixed_len_byte_array}) values with the v2 writer, FLOAT values are dictionary encoded by both.
   */
  @Param("PARQUET_1_0")
  private WriterVersion writerVersion;

  /**
   * Whether Parquet collects statistics of the embedding values: min/max statistics and size
   * statistics. Iceberg does not collect them by default for values in lists, because it does not
   * keep metrics for them; {@code write.parquet.stats-enabled.column.embedding.element=true}
   * enables them. Computing them is a large part of the cost of writing the values, especially
   * FLOAT16 values, which are compared as half-precision floats.
   */
  @Param("false")
  private boolean columnStatistics;

  /**
   * Target size of Parquet pages in bytes, like Iceberg's {@code write.parquet.page-size-bytes}
   * (default 1 MB). Pages are only closed when the writer checks their size, at least every {@link
   * #sizeCheckMinRows} rows.
   */
  @Param("1048576")
  private int pageSizeBytes;

  /**
   * Target size of Parquet row groups in bytes, like Iceberg's {@code
   * write.parquet.row-group-size-bytes} (default 128 MB). Iceberg measures row groups by their
   * buffered size, which is the compressed size of the finished pages.
   */
  @Param("134217728")
  private long rowGroupSizeBytes;

  /**
   * Minimum number of rows between two checks of the page and row group sizes, like Iceberg's
   * {@code write.parquet.row-group-check-min-record-count} (default 100), which Iceberg also uses
   * for the page size checks. Every row holds a whole embedding, 5 KB of float16 values with 2560
   * dimensions, so the default does not let pages be smaller than about 100 embeddings.
   */
  @Param("100")
  private int sizeCheckMinRows;

  /**
   * Whether the reader reuses containers, like Iceberg's {@code Parquet.read(...)
   * .reuseContainers()}: every embedding is read into the array of the previous embedding, which is
   * only valid until the next embedding is read. Iceberg does not reuse containers by default.
   */
  @Param("false")
  private boolean reuseContainers;

  /**
   * Whether values are decoded straight into the array of each embedding: {@code float[]} from
   * FLOAT, and from FLOAT16 converting while decoding, and {@code short[]} from FLOAT16. Otherwise,
   * and always for {@code short[]} from FLOAT, the values are decoded into a buffer and then copied
   * or converted into the embedding.
   */
  @Param("true")
  private boolean directDecode;

  /**
   * Whether the read benchmark reads back every value of the embeddings, as a checksum, like a
   * consumer would. Without it, the benchmark measures only the reader.
   */
  @Param("true")
  private boolean consumeValues;

  private Embeddings<?> embeddings;
  private WriteOptions writeOptions;
  private ReadOptions readOptions;
  private File tempDir;
  private File readFile;
  private File writeFile;
  private long fileBytes;
  private long heapBytes;

  @Setup(Level.Trial)
  public void setupBenchmark() throws IOException {
    String[] codecAndLevel = codec.split(":", 2);
    this.writeOptions =
        new WriteOptions(
            storage,
            encoding,
            CompressionCodecName.fromConf(codecAndLevel[0]),
            codecAndLevel.length > 1 ? codecAndLevel[1] : null,
            writerVersion,
            columnStatistics,
            pageSizeBytes,
            rowGroupSizeBytes,
            sizeCheckMinRows);
    this.readOptions = new ReadOptions(true, reuseContainers, directDecode);
    this.tempDir = java.nio.file.Files.createTempDirectory("float16-embedding-bench").toFile();
    this.readFile = new File(tempDir, "read.parquet");
    this.writeFile = new File(tempDir, "write.parquet");

    SplittableRandom random = new SplittableRandom(SEED);
    EmbeddingModel model = EmbeddingModel.random(dimension, random);
    List<short[]> float16Embeddings = generate(model.generator(random.split()), embeddingCount);

    long heapBefore = usedHeapAfterGc();
    try {
      this.embeddings = Embeddings.of(source, float16Embeddings, dimension);
      this.heapBytes = usedHeapAfterGc() - heapBefore;
    } finally {
      // the generated values must stay reachable until the heap is measured again
      Reference.reachabilityFence(float16Embeddings);
    }

    embeddings.write(readFile, writeOptions);
    this.fileBytes = readFile.length();

    // checks that the file holds exactly the embeddings of the source representation, every one
    // with dimension values, which the read benchmark relies on to skip the levels, and that the
    // reader of the read benchmark returns the same embeddings
    embeddings.verify(readFile, readOptions);

    Set<Encoding> dataEncodings = dataEncodings(readFile);
    if (!matches(encoding, dataEncodings)) {
      LOG.warn("Requested {} encoding, but the file has {} pages", encoding, dataEncodings);
    }

    LOG.info(
        "Embeddings: source={}, {}, {}, count={}, dimension={}, fileBytes={}, "
            + "heapBytes={}, maxHeap={}, UseCompressedOops={}, file: {}",
        source,
        writeOptions,
        readOptions,
        embeddings.count(),
        dimension,
        fileBytes,
        heapBytes,
        Runtime.getRuntime().maxMemory(),
        vmOption("UseCompressedOops"),
        describeElementColumn(readFile));
  }

  @TearDown(Level.Trial)
  public void tearDownBenchmark() throws IOException {
    java.nio.file.Files.deleteIfExists(readFile.toPath());
    java.nio.file.Files.deleteIfExists(writeFile.toPath());
    java.nio.file.Files.deleteIfExists(tempDir.toPath());
  }

  @Benchmark
  public void write() throws IOException {
    embeddings.write(writeFile, writeOptions);
  }

  @Benchmark
  public void read(Blackhole blackhole) throws IOException {
    embeddings.read(readFile, readOptions, consumeValues, blackhole);
  }

  /**
   * Reports the file and memory sizes as secondary results ({@code sizes:fileBytes} and {@code
   * sizes:heapBytes}). The sizes are deterministic, so a single iteration is enough by default.
   */
  @Benchmark
  @Warmup(iterations = 0)
  @Measurement(iterations = 1)
  public void sizes(SizeCounters counters) {
    counters.update(fileBytes, heapBytes);
  }

  @State(Scope.Thread)
  @AuxCounters(AuxCounters.Type.EVENTS)
  public static class SizeCounters {
    private double scale = 1.0;
    private double fileBytes = 0.0;
    private double heapBytes = 0.0;

    /**
     * JMH sums event counters over all measured iterations and forks, so every iteration reports
     * its share of the size. This keeps the results correct when the iteration or fork count is
     * changed, for example with JMH's {@code -i} or {@code -f} options.
     */
    @Setup(Level.Trial)
    public void setupScale(BenchmarkParams params) {
      long measuredIterations =
          (long) Math.max(params.getForks(), 1) * params.getMeasurement().getCount();
      this.scale = 1.0 / measuredIterations;
    }

    public double fileBytes() {
      return fileBytes;
    }

    public double heapBytes() {
      return heapBytes;
    }

    void update(long newFileBytes, long newHeapBytes) {
      this.fileBytes = newFileBytes * scale;
      this.heapBytes = newHeapBytes * scale;
    }
  }

  /** Generates {@code count} embeddings, as float16 bits. */
  private static List<short[]> generate(Supplier<short[]> generator, int count) {
    Preconditions.checkArgument(count > 0, "Invalid embedding count: %s", count);
    List<short[]> float16Embeddings = Lists.newArrayListWithCapacity(count);
    for (int index = 0; index < count; index += 1) {
      float16Embeddings.add(generator.get());
    }

    return float16Embeddings;
  }

  /**
   * Per-dimension normal distributions of the values of random embeddings.
   *
   * <p>Real embeddings are not isotropic: dimensions have different offsets and spreads, so the
   * embeddings share a common direction and some dimensions have much larger values than others.
   * These characteristics determine the value range and the number of distinct float16 values, so
   * how well the values compress. The model draws the offset (mean) of every dimension from a
   * normal distribution and its spread (standard deviation) from a log-normal distribution, with
   * the characteristics of real query embeddings with 2560 dimensions: the offsets hold about a
   * fifth of the squared length of the embeddings, and the logarithms of the spreads have a
   * standard deviation of 0.25. The generated embeddings are normalized to unit length, like
   * typical embeddings.
   */
  private static final class EmbeddingModel {
    // share of the squared length of the embeddings in the offsets of the dimensions
    private static final double OFFSET_SHARE = 0.22;
    // standard deviation of the logarithms of the spreads of the dimensions
    private static final double LOG_SPREAD_STD_DEV = 0.25;

    private final double[] means;
    private final double[] stdDevs;

    private EmbeddingModel(double[] means, double[] stdDevs) {
      this.means = means;
      this.stdDevs = stdDevs;
    }

    /** Draws the offset and the spread of every dimension. */
    static EmbeddingModel random(int dimension, SplittableRandom random) {
      Preconditions.checkArgument(dimension > 0, "Invalid embedding dimension: %s", dimension);
      double offsetStdDev = Math.sqrt(OFFSET_SHARE / dimension);
      // scales the log-normal spreads so that the mean of their squares is the rest of the squared
      // length per dimension: E[exp(2 * s * Z - 2 * s^2)] = 1 for a standard normal Z
      double spreadScale = Math.sqrt((1.0 - OFFSET_SHARE) / dimension);
      double logSpreadShift = LOG_SPREAD_STD_DEV * LOG_SPREAD_STD_DEV;
      double[] means = new double[dimension];
      double[] stdDevs = new double[dimension];
      for (int pos = 0; pos < dimension; pos += 1) {
        means[pos] = offsetStdDev * random.nextGaussian();
        stdDevs[pos] =
            spreadScale * Math.exp(LOG_SPREAD_STD_DEV * random.nextGaussian() - logSpreadShift);
      }

      return new EmbeddingModel(means, stdDevs);
    }

    /** Returns a generator of unit length embeddings, as float16 bits. */
    Supplier<short[]> generator(SplittableRandom random) {
      int dimension = means.length;
      double[] values = new double[dimension];
      return () -> {
        double squaredNorm = 0.0;
        for (int pos = 0; pos < dimension; pos += 1) {
          values[pos] = means[pos] + stdDevs[pos] * random.nextGaussian();
          squaredNorm += values[pos] * values[pos];
        }

        double norm = Math.sqrt(squaredNorm);
        short[] embedding = new short[dimension];
        for (int pos = 0; pos < dimension; pos += 1) {
          embedding[pos] = ParquetConversions.floatToFloat16((float) (values[pos] / norm));
        }

        return embedding;
      };
    }
  }

  private static long usedHeapAfterGc() {
    for (int round = 0; round < GC_ROUNDS; round += 1) {
      System.gc();
    }

    return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
  }

  private static String vmOption(String name) {
    HotSpotDiagnosticMXBean diagnostics =
        ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
    return diagnostics != null ? diagnostics.getVMOption(name).getValue() : "unknown";
  }

  /**
   * Describes how the embedding values are stored: row groups, data pages, encodings, dictionary
   * sizes. Page sizes are averages over the data pages, in KB; the uncompressed size includes the
   * page headers and the dictionary page, if any.
   */
  private static String describeElementColumn(File file) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file.toPath()))) {
      ColumnDescriptor desc =
          reader.getFileMetaData().getSchema().getColumnDescription(ELEMENT_PATH);
      List<Integer> dictionaryEntries = Lists.newArrayList();
      List<BlockMetaData> rowGroups = reader.getFooter().getBlocks();
      long pages = 0L;
      long compressedPageBytes = 0L;
      long uncompressedBytes = 0L;
      for (BlockMetaData rowGroup : rowGroups) {
        DictionaryPageReadStore dictionaries = reader.getDictionaryReader(rowGroup);
        DictionaryPage dictionary = dictionaries.readDictionaryPage(desc);
        if (dictionary != null) {
          dictionaryEntries.add(dictionary.getDictionarySize());
        }

        ColumnChunkMetaData column = rowGroup.getColumns().get(0);
        uncompressedBytes += column.getTotalUncompressedSize();
        OffsetIndex offsetIndex = reader.readOffsetIndex(column);
        if (offsetIndex != null) {
          pages += offsetIndex.getPageCount();
          for (int page = 0; page < offsetIndex.getPageCount(); page += 1) {
            compressedPageBytes += offsetIndex.getCompressedPageSize(page);
          }
        }
      }

      return String.format(
          Locale.ROOT,
          "rowGroups=%s, dataPages=%s, avgPageKB=%.0f compressed/%.0f uncompressed, "
              + "dataEncodings=%s, dictionaryEntries=%s",
          rowGroups.size(),
          pages,
          pages > 0 ? compressedPageBytes / 1024.0 / pages : 0.0,
          pages > 0 ? uncompressedBytes / 1024.0 / pages : 0.0,
          dataEncodings(reader),
          dictionaryEntries);
    }
  }

  /** Returns the encodings of the data pages of the embedding values. */
  private static Set<Encoding> dataEncodings(File file) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file.toPath()))) {
      return dataEncodings(reader);
    }
  }

  private static Set<Encoding> dataEncodings(ParquetFileReader reader) {
    Set<Encoding> encodings = EnumSet.noneOf(Encoding.class);
    for (BlockMetaData rowGroup : reader.getFooter().getBlocks()) {
      ColumnChunkMetaData column = rowGroup.getColumns().get(0);
      if (column.getEncodingStats() != null) {
        encodings.addAll(column.getEncodingStats().getDataEncodings());
      }
    }

    return encodings;
  }

  /**
   * Returns whether data pages have the requested encoding. Dictionary encoded columns may fall
   * back to PLAIN when the dictionary is too large.
   */
  private static boolean matches(EncodingType encoding, Set<Encoding> dataEncodings) {
    return switch (encoding) {
      case DICTIONARY ->
          dataEncodings.stream().anyMatch(Encoding::usesDictionary)
              && dataEncodings.stream().allMatch(e -> e.usesDictionary() || e == Encoding.PLAIN);
      case PLAIN -> dataEncodings.equals(EnumSet.of(Encoding.PLAIN));
      case BYTE_STREAM_SPLIT -> dataEncodings.equals(EnumSet.of(Encoding.BYTE_STREAM_SPLIT));
    };
  }

  private static MessageType embeddingSchema(StorageType storage) {
    PrimitiveType elementType =
        switch (storage) {
          case FLOAT -> Types.required(PrimitiveTypeName.FLOAT).id(ELEMENT_ID).named(ELEMENT);
          case FLOAT16 ->
              Types.required(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                  .length(Float16LogicalTypeAnnotation.BYTES)
                  .as(LogicalTypeAnnotation.float16Type())
                  .id(ELEMENT_ID)
                  .named(ELEMENT);
        };

    return Types.buildMessage()
        .addField(
            Types.requiredGroup()
                .as(LogicalTypeAnnotation.listType())
                .addField(Types.repeatedGroup().addField(elementType).named(LIST))
                .id(EMBEDDING_ID)
                .named(EMBEDDING))
        .named("table");
  }

  /**
   * Creates a writer of embeddings with Iceberg's Parquet writer, the one {@code
   * Parquet.write(...)} builds, configured like {@code Parquet.write(...)} with Iceberg's default
   * table properties. Iceberg's writer only supports custom Parquet schemas internally, which this
   * benchmark uses to write FLOAT16 values.
   */
  private static <T> FileAppender<T> newWriter(
      File file, WriteOptions options, Representation<T> representation, int dimension) {
    boolean dictionary = options.encoding() == EncodingType.DICTIONARY;
    boolean byteStreamSplit = options.encoding() == EncodingType.BYTE_STREAM_SPLIT;
    ParquetProperties properties =
        ParquetProperties.builder()
            .withWriterVersion(options.writerVersion())
            .withPageSize(options.pageSize())
            .withPageRowCountLimit(TableProperties.PARQUET_PAGE_ROW_LIMIT_DEFAULT)
            .withDictionaryPageSize(TableProperties.PARQUET_DICT_SIZE_BYTES_DEFAULT)
            .withMinRowCountForPageSizeCheck(options.sizeCheckMinRows())
            .withMaxRowCountForPageSizeCheck(
                Math.max(
                    options.sizeCheckMinRows(),
                    TableProperties.PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT_DEFAULT))
            .withDictionaryEncoding(dictionary)
            .withByteStreamSplitEncoding(ELEMENT_COLUMN, byteStreamSplit)
            // like Parquet.write(...), which disables both for list elements unless
            // write.parquet.stats-enabled.column.embedding.element=true
            .withStatisticsEnabled(ELEMENT_COLUMN, options.columnStatistics())
            .withSizeStatisticsEnabled(ELEMENT_COLUMN, options.columnStatistics())
            .build();
    return new ParquetWriter<>(
        options.conf(),
        Files.localOutput(file),
        SCHEMA,
        embeddingSchema(options.storage()),
        options.rowGroupSize(),
        ImmutableMap.of(),
        (icebergSchema, fileSchema) ->
            EmbeddingValueWriter.create(
                fileSchema.getColumnDescription(ELEMENT_PATH),
                options.storage(),
                representation,
                dimension),
        options.codec(),
        properties,
        MetricsConfig.getDefault(),
        ParquetFileWriter.Mode.OVERWRITE,
        null,
        false);
  }

  /** Creates a reader of the embeddings in a file. */
  private static <T> CloseableIterable<T> newReader(
      File file, Representation<T> representation, int dimension, ReadOptions options) {
    Parquet.ReadBuilder builder =
        Parquet.read(Files.localInput(file))
            .project(SCHEMA)
            .createReaderFunc(
                fileSchema -> embeddingReader(fileSchema, representation, dimension, options));
    if (options.reuseContainers()) {
      builder.reuseContainers();
    }

    return builder.build();
  }

  private static <T> ParquetValueReader<T> embeddingReader(
      MessageType fileSchema,
      Representation<T> representation,
      int dimension,
      ReadOptions options) {
    ColumnDescriptor desc = fileSchema.getColumnDescription(ELEMENT_PATH);
    PrimitiveType elementType = desc.getPrimitiveType();
    if (elementType.getLogicalTypeAnnotation() instanceof Float16LogicalTypeAnnotation) {
      return new Float16EmbeddingReader<>(desc, representation, dimension, options);
    } else if (elementType.getPrimitiveTypeName() == PrimitiveTypeName.FLOAT) {
      return new FloatEmbeddingReader<>(desc, representation, dimension, options);
    }

    throw new UnsupportedOperationException("Unsupported embedding element: " + elementType);
  }

  /**
   * Embeddings as one Java array per embedding, {@code float[]} or {@code short[]}, and the logic
   * to write and read them.
   */
  private static final class Embeddings<T> {
    private final Representation<T> representation;
    private final int dimension;
    private final List<T> values;

    static Embeddings<?> of(SourceType source, List<short[]> float16Embeddings, int dimension) {
      return switch (source) {
        case FLOAT_ARRAY -> new Embeddings<>(new FloatArrays(), float16Embeddings, dimension);
        case SHORT_ARRAY -> new Embeddings<>(new ShortArrays(), float16Embeddings, dimension);
      };
    }

    private Embeddings(
        Representation<T> representation, List<short[]> float16Embeddings, int dimension) {
      this.representation = representation;
      this.dimension = dimension;
      this.values = Lists.newArrayListWithCapacity(float16Embeddings.size());
      for (short[] float16Embedding : float16Embeddings) {
        T embedding = representation.allocate(dimension);
        for (int pos = 0; pos < dimension; pos += 1) {
          representation.setFloat16(embedding, pos, float16Embedding[pos]);
        }

        values.add(embedding);
      }
    }

    int count() {
      return values.size();
    }

    /** Writes all embeddings to a Parquet file. */
    void write(File file, WriteOptions options) throws IOException {
      try (FileAppender<T> writer = newWriter(file, options, representation, dimension)) {
        for (T embedding : values) {
          writer.add(embedding);
        }
      }
    }

    /**
     * Reads all embeddings from a Parquet file into this representation. With {@code
     * consumeValues}, every value is read back before the embeddings are discarded, like a consumer
     * would.
     */
    void read(File file, ReadOptions options, boolean consumeValues, Blackhole blackhole)
        throws IOException {
      try (CloseableIterable<T> reader = newReader(file, representation, dimension, options)) {
        for (T embedding : reader) {
          if (consumeValues) {
            blackhole.consume(representation.checksum(embedding));
          }

          blackhole.consume(embedding);
        }
      }
    }

    /**
     * Checks that the file contains exactly these embeddings, first with a reader that decodes all
     * Parquet levels, which checks the length of every embedding, then with the reader of the read
     * benchmark.
     */
    void verify(File file, ReadOptions options) throws IOException {
      verifyWith(file, options.decodingLevels());
      verifyWith(file, options);
    }

    private void verifyWith(File file, ReadOptions options) throws IOException {
      int index = 0;
      T previous = null;
      try (CloseableIterable<T> reader = newReader(file, representation, dimension, options)) {
        for (T embedding : reader) {
          Preconditions.checkState(
              index < values.size(), "Read more than %s embeddings", values.size());
          Preconditions.checkState(
              representation.sameValues(values.get(index), embedding),
              "Embedding %s changed in the round trip",
              index);
          Preconditions.checkState(
              previous == null || (embedding == previous) == options.reuseContainers(),
              "Embedding %s was %s the previous container",
              index,
              options.reuseContainers() ? "not read into" : "read into");
          previous = embedding;
          index += 1;
        }
      }

      Preconditions.checkState(
          index == values.size(), "Read %s embeddings, expected %s", index, values.size());
    }
  }

  /** Accessors for the values of an embedding in a specific in-memory representation. */
  private interface Representation<T> {
    T allocate(int dimension);

    int length(T embedding);

    float getFloat(T embedding, int pos);

    short getFloat16(T embedding, int pos);

    void setFloat16(T embedding, int pos, short float16Bits);

    /** Sets all values of an embedding from an array with one value per dimension. */
    void setFloats(T embedding, float[] values);

    /** Sets all values of an embedding from an array of half-precision bits per dimension. */
    void setFloat16s(T embedding, short[] float16Bits);

    /**
     * Returns the array that holds the values of an embedding as single-precision floats, so that a
     * reader can decode into it directly, or null if the embedding holds its values otherwise.
     */
    default float[] floatValues(T embedding) {
      return null;
    }

    /**
     * Returns the array that holds the values of an embedding as half-precision bits, so that a
     * reader can decode into it directly, or null if the embedding holds its values otherwise.
     */
    default short[] float16Values(T embedding) {
      return null;
    }

    boolean sameValues(T left, T right);

    /**
     * Reads every value of an embedding in its native form, as a checksum, so that reading the
     * embeddings includes consuming their values.
     */
    long checksum(T embedding);
  }

  private static final class FloatArrays implements Representation<float[]> {
    @Override
    public float[] allocate(int dimension) {
      return new float[dimension];
    }

    @Override
    public int length(float[] embedding) {
      return embedding.length;
    }

    @Override
    public float getFloat(float[] embedding, int pos) {
      return embedding[pos];
    }

    @Override
    public short getFloat16(float[] embedding, int pos) {
      return ParquetConversions.floatToFloat16(embedding[pos]);
    }

    @Override
    public void setFloat16(float[] embedding, int pos, short float16Bits) {
      embedding[pos] = ParquetConversions.float16ToFloat(float16Bits);
    }

    @Override
    public void setFloats(float[] embedding, float[] values) {
      System.arraycopy(values, 0, embedding, 0, embedding.length);
    }

    @Override
    public void setFloat16s(float[] embedding, short[] float16Bits) {
      for (int pos = 0; pos < embedding.length; pos += 1) {
        embedding[pos] = ParquetConversions.float16ToFloat(float16Bits[pos]);
      }
    }

    @Override
    public float[] floatValues(float[] embedding) {
      return embedding;
    }

    @Override
    public boolean sameValues(float[] left, float[] right) {
      return Arrays.equals(left, right);
    }

    @Override
    public long checksum(float[] embedding) {
      long checksum = 0L;
      for (float value : embedding) {
        checksum += Float.floatToRawIntBits(value);
      }

      return checksum;
    }
  }

  private static final class ShortArrays implements Representation<short[]> {
    @Override
    public short[] allocate(int dimension) {
      return new short[dimension];
    }

    @Override
    public int length(short[] embedding) {
      return embedding.length;
    }

    @Override
    public float getFloat(short[] embedding, int pos) {
      return ParquetConversions.float16ToFloat(embedding[pos]);
    }

    @Override
    public short getFloat16(short[] embedding, int pos) {
      return embedding[pos];
    }

    @Override
    public void setFloat16(short[] embedding, int pos, short float16Bits) {
      embedding[pos] = float16Bits;
    }

    @Override
    public void setFloats(short[] embedding, float[] values) {
      for (int pos = 0; pos < embedding.length; pos += 1) {
        embedding[pos] = ParquetConversions.floatToFloat16(values[pos]);
      }
    }

    @Override
    public void setFloat16s(short[] embedding, short[] float16Bits) {
      System.arraycopy(float16Bits, 0, embedding, 0, embedding.length);
    }

    @Override
    public short[] float16Values(short[] embedding) {
      return embedding;
    }

    @Override
    public boolean sameValues(short[] left, short[] right) {
      return Arrays.equals(left, right);
    }

    @Override
    public long checksum(short[] embedding) {
      long checksum = 0L;
      for (short value : embedding) {
        checksum += value;
      }

      return checksum;
    }
  }

  /**
   * Writes the values of an embedding, a 3-level list with required elements, straight to Parquet's
   * column writer of the element column, in one loop per embedding.
   *
   * <p>Compared to Iceberg's generic list writer, this skips the per-element writer calls and the
   * boxing of values, and compared to parquet-mr's {@code RecordConsumer}, the calls that start and
   * end the list element group and field around every value. The first value of an embedding starts
   * a new list with the repetition level of the record, and every other value continues it; all
   * values are defined.
   */
  private abstract static class EmbeddingValueWriter<T>
      extends ParquetValueWriters.PrimitiveWriter<T> {
    private final Representation<T> representation;
    private final int dimension;
    // the repetition level of the values that continue a list
    private final int elementRepetitionLevel;

    static <T> EmbeddingValueWriter<T> create(
        ColumnDescriptor desc,
        StorageType storage,
        Representation<T> representation,
        int dimension) {
      return switch (storage) {
        case FLOAT -> new FloatValueWriter<>(desc, representation, dimension);
        case FLOAT16 -> new Float16ValueWriter<>(desc, representation, dimension);
      };
    }

    EmbeddingValueWriter(ColumnDescriptor desc, Representation<T> representation, int dimension) {
      super(desc);
      Preconditions.checkArgument(dimension > 0, "Invalid embedding dimension: %s", dimension);
      this.representation = representation;
      this.dimension = dimension;
      this.elementRepetitionLevel = desc.getMaxRepetitionLevel();
    }

    protected Representation<T> representation() {
      return representation;
    }

    protected int dimension() {
      return dimension;
    }

    protected int elementRepetitionLevel() {
      return elementRepetitionLevel;
    }

    /**
     * Writes all values of an embedding.
     *
     * @param repetitionLevel the repetition level of the first value, which starts the list
     */
    protected abstract void writeValues(int repetitionLevel, T embedding);

    @Override
    public void write(int repetitionLevel, T embedding) {
      Preconditions.checkArgument(
          representation.length(embedding) == dimension,
          "Invalid embedding length: %s, expected %s",
          representation.length(embedding),
          dimension);
      writeValues(repetitionLevel, embedding);
    }
  }

  private static final class FloatValueWriter<T> extends EmbeddingValueWriter<T> {
    private FloatValueWriter(
        ColumnDescriptor desc, Representation<T> representation, int dimension) {
      super(desc, representation, dimension);
    }

    @Override
    protected void writeValues(int repetitionLevel, T embedding) {
      int elementLevel = elementRepetitionLevel();
      float[] floats = representation().floatValues(embedding);
      if (floats != null) {
        column.writeFloat(repetitionLevel, floats[0]);
        for (int pos = 1; pos < floats.length; pos += 1) {
          column.writeFloat(elementLevel, floats[pos]);
        }
      } else {
        column.writeFloat(repetitionLevel, representation().getFloat(embedding, 0));
        for (int pos = 1; pos < dimension(); pos += 1) {
          column.writeFloat(elementLevel, representation().getFloat(embedding, pos));
        }
      }
    }
  }

  private static final class Float16ValueWriter<T> extends EmbeddingValueWriter<T> {
    private final byte[] buffer = new byte[Float16LogicalTypeAnnotation.BYTES];
    // one binary over the buffer for all values: Parquet copies reused binaries before keeping them
    // (for example in statistics or in the dictionary), so there is no allocation per value
    private final Binary binary = Binary.fromReusedByteArray(buffer);

    private Float16ValueWriter(
        ColumnDescriptor desc, Representation<T> representation, int dimension) {
      super(desc, representation, dimension);
    }

    @Override
    protected void writeValues(int repetitionLevel, T embedding) {
      int elementLevel = elementRepetitionLevel();
      short[] float16s = representation().float16Values(embedding);
      if (float16s != null) {
        write(repetitionLevel, float16s[0]);
        for (int pos = 1; pos < float16s.length; pos += 1) {
          write(elementLevel, float16s[pos]);
        }
      } else {
        write(repetitionLevel, representation().getFloat16(embedding, 0));
        for (int pos = 1; pos < dimension(); pos += 1) {
          write(elementLevel, representation().getFloat16(embedding, pos));
        }
      }
    }

    private void write(int repetitionLevel, short float16Bits) {
      // Parquet stores FLOAT16 values in little-endian byte order
      buffer[0] = (byte) float16Bits;
      buffer[1] = (byte) (float16Bits >> 8);
      column.writeBinary(repetitionLevel, binary);
    }
  }

  /**
   * Reads a whole embedding from the element column of a list with required elements, directly into
   * the target representation without boxing the values.
   *
   * <p>The values of each list are decoded in one bulk read, page by page, instead of one call per
   * value. With direct decoding, they are decoded straight into the array of the embedding when it
   * holds the decoded type, otherwise they are decoded into a buffer that is then set in the
   * representation in bulk.
   *
   * <p>With a fixed size, the reader relies on every embedding having exactly {@code dimension}
   * values, so the column derives the repetition and definition levels from the position of each
   * value instead of decoding them. Otherwise, the levels of every value are decoded, which checks
   * the length of every embedding.
   */
  private abstract static class EmbeddingReader<T> extends ParquetValueReaders.PrimitiveReader<T> {
    private final Representation<T> representation;
    private final int dimension;
    private final boolean directDecode;
    // levels of the repeated list group; the element is required, so it adds no levels
    private final int listDefinitionLevel;
    private final int listRepetitionLevel;

    EmbeddingReader(
        ColumnDescriptor desc,
        Representation<T> representation,
        int dimension,
        ReadOptions options) {
      super(desc);
      Preconditions.checkArgument(dimension > 0, "Invalid embedding dimension: %s", dimension);
      this.representation = representation;
      this.dimension = dimension;
      this.directDecode = options.directDecode();
      this.listDefinitionLevel = desc.getMaxDefinitionLevel() - 1;
      this.listRepetitionLevel = desc.getMaxRepetitionLevel() - 1;
      if (options.fixedSize()) {
        column.setFixedListSize(dimension);
      }
    }

    protected Representation<T> representation() {
      return representation;
    }

    protected int dimension() {
      return dimension;
    }

    protected boolean directDecode() {
      return directDecode;
    }

    /**
     * Reads the values of the current list in bulk, up to the dimension, into an embedding.
     *
     * @return the number of values read
     */
    protected abstract int readValues(T embedding);

    @Override
    public T read(T reuse) {
      // the Parquet reader passes the previous embedding only when it reuses containers
      T embedding =
          reuse != null && representation.length(reuse) == dimension
              ? reuse
              : representation.allocate(dimension);
      int length = 0;
      if (column.currentDefinitionLevel() > listDefinitionLevel) {
        length = readValues(embedding);
      } else {
        // an empty list is stored as a single null value
        column.nextNull();
      }

      Preconditions.checkState(
          length == dimension, "Invalid embedding length: %s, expected %s", length, dimension);
      // the bulk read stops after dimension values, so a longer list continues here
      Preconditions.checkState(
          column.currentRepetitionLevel() <= listRepetitionLevel,
          "Invalid embedding length: more than %s",
          dimension);
      return embedding;
    }
  }

  private static final class FloatEmbeddingReader<T> extends EmbeddingReader<T> {
    private final float[] values;

    private FloatEmbeddingReader(
        ColumnDescriptor desc,
        Representation<T> representation,
        int dimension,
        ReadOptions options) {
      super(desc, representation, dimension, options);
      this.values = new float[dimension];
    }

    @Override
    protected int readValues(T embedding) {
      float[] target = directDecode() ? representation().floatValues(embedding) : null;
      if (target != null) {
        return column.nextFloats(target, 0, dimension());
      }

      int length = column.nextFloats(values, 0, dimension());
      representation().setFloats(embedding, values);
      return length;
    }
  }

  private static final class Float16EmbeddingReader<T> extends EmbeddingReader<T> {
    private final short[] values;

    private Float16EmbeddingReader(
        ColumnDescriptor desc,
        Representation<T> representation,
        int dimension,
        ReadOptions options) {
      super(desc, representation, dimension, options);
      this.values = new short[dimension];
    }

    @Override
    protected int readValues(T embedding) {
      if (directDecode()) {
        short[] float16Target = representation().float16Values(embedding);
        if (float16Target != null) {
          // decodes the half-precision bits straight from the pages, without allocating a Binary
          return column.nextFloat16s(float16Target, 0, dimension());
        }

        float[] floatTarget = representation().floatValues(embedding);
        if (floatTarget != null) {
          // converts the values to single precision while decoding them
          return column.nextFloats(floatTarget, 0, dimension());
        }
      }

      int length = column.nextFloat16s(values, 0, dimension());
      representation().setFloat16s(embedding, values);
      return length;
    }
  }
}

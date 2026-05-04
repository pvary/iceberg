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
package org.apache.iceberg.index;

import static org.apache.iceberg.types.Types.NestedField.optional;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.hadoop.util.Sets;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Index handler that materializes each bucket as a complete in-memory Parquet file (single row
 * group, no padding) and concatenates those byte payloads into the final file. After all buckets
 * are written, an outer Parquet footer is appended whose {@link BlockMetaData} entries point at the
 * concatenated payloads via shifted column-chunk offsets, so generic Parquet tooling can still open
 * the resulting file. The per-bucket byte lengths are persisted as a key-value entry in the outer
 * Parquet file metadata under {@link #BUCKET_LENGTHS_KEY}, encoded as little-endian {@code
 * int32[numBuckets]} and base64-wrapped to fit the string-typed kv slot.
 *
 * <p>The {@code metaOffsets[]} concept from {@link ParquetIndexHandlerWithEmbeddedMetadata} is
 * mirrored here: the lengths array is opened from the footer exactly once per input file location
 * and then cached process-wide in {@link #BUCKET_INDEX_CACHE}. The reader reconstructs cumulative
 * file offsets from the lengths and uses them on the hot path with a single ranged read per lookup.
 *
 * <h2>File layout</h2>
 *
 * <pre>
 *   [bucket_0 parquet bytes]   // each starts with PAR1; bucket 0's PAR1 doubles as the file's
 *   [bucket_1 parquet bytes]   //  leading magic
 *   ...
 *   [bucket_{B-1} parquet bytes]
 *   [outer parquet footer (kv: BUCKET_LENGTHS_KEY = base64(int32[B]))][int32 footerLen][PAR1]
 * </pre>
 *
 * <h2>Hot-path lookup</h2>
 *
 * <ol>
 *   <li>Hash key &rarr; bucket via {@link HashIndexHandler#bucketOf(byte[], int)}.
 *   <li>Single ranged read of {@code lengths[bucket]} bytes at {@code offsets[bucket]} into a
 *       {@code byte[]}.
 *   <li>Open an in-memory {@link ParquetFileReader} over those bytes and scan the single row group
 *       for the matching key.
 * </ol>
 */
public class ParquetIndexHandlerWithConcatenatedBuckets implements IndexHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(ParquetIndexHandlerWithConcatenatedBuckets.class);

  /** Field name of the source-file path column. */
  public static final String FILE_PATH_COLUMN = "file_path";

  /** Field name of the row-position column. */
  public static final String POS_COLUMN = "pos";

  /**
   * Outer-footer kv-metadata key under which the writer stores {@code base64(int32[numBuckets])}.
   */
  static final String BUCKET_LENGTHS_KEY = "iceberg.index.bucket-lengths";

  /** Padding factor applied to {@code bucketRows} to size each bucket's row-group capacity. */
  private static final double BUCKET_PAD_FACTOR = 1.5;

  private final Schema schema;
  private final int keyFieldCount;
  private final int numBuckets;
  private final int rowsPerBucket;
  private final Function<Record, byte[]> keyEncoder;
  private final MessageType messageType;

  /**
   * @param keySchema schema of the key columns (must contain at least one field; must not contain
   *     fields named {@code file_path} or {@code pos})
   * @param bucketRows <em>average</em> number of keys per bucket; the actual per-bucket row-group
   *     capacity is {@code ceil(bucketRows * 1.5)}. Must be {@code > 0}.
   * @param keyCount sizing hint that determines the number of buckets via {@code numBuckets =
   *     ceil(keyCount / bucketRows)}. Must be {@code > 0}.
   */
  public ParquetIndexHandlerWithConcatenatedBuckets(
      Schema keySchema, int bucketRows, long keyCount) {
    Preconditions.checkArgument(
        keySchema != null && !keySchema.columns().isEmpty(),
        "Key schema must contain at least one field");
    Preconditions.checkArgument(bucketRows > 0, "bucketRows must be > 0: %s", bucketRows);
    Preconditions.checkArgument(keyCount > 0L, "keyCount must be > 0: %s", keyCount);

    for (Types.NestedField f : keySchema.columns()) {
      Preconditions.checkArgument(
          !FILE_PATH_COLUMN.equals(f.name()) && !POS_COLUMN.equals(f.name()),
          "Key schema must not contain a field named '%s' (reserved for the index payload)",
          f.name());
    }

    this.keyFieldCount = keySchema.columns().size();
    long buckets = (keyCount + bucketRows - 1L) / bucketRows;
    Preconditions.checkArgument(
        buckets > 0L && buckets <= Integer.MAX_VALUE, "numBuckets out of range: %s", buckets);
    this.numBuckets = (int) buckets;
    long maxRowsPerBucket = (long) Math.ceil((double) bucketRows * BUCKET_PAD_FACTOR);
    Preconditions.checkArgument(
        maxRowsPerBucket > 0L && maxRowsPerBucket <= Integer.MAX_VALUE,
        "rowsPerBucket out of range: %s",
        maxRowsPerBucket);
    this.rowsPerBucket = (int) maxRowsPerBucket;

    // On-disk schema: key columns (renumbered from 1) + file_path + pos. All optional so
    // GenericParquetWriter can serialize potentially-empty bucket files cleanly.
    List<Types.NestedField> fields = Lists.newArrayListWithCapacity(keyFieldCount + 2);
    int id = 1;
    for (Types.NestedField keyField : keySchema.columns()) {
      fields.add(optional(id++, keyField.name(), keyField.type()));
    }
    fields.add(optional(id++, FILE_PATH_COLUMN, Types.StringType.get()));
    fields.add(optional(id, POS_COLUMN, Types.LongType.get()));
    this.schema = new Schema(fields);

    this.keyEncoder = MinimalPerfectHashFunctionIndexHandler.keyEncoder(keySchema);
    this.messageType = ParquetSchemaUtil.convert(this.schema, "table");
  }

  @Override
  public IndexHandler.Writer writer(OutputFile output) {
    return new Writer(
        output, schema, messageType, keyFieldCount, numBuckets, rowsPerBucket, keyEncoder);
  }

  @Override
  public IndexHandler.Reader reader(InputFile input) throws IOException {
    BucketIndex idx = recoverBucketIndex(input, numBuckets);
    return new Reader(
        input, idx.lengths, idx.offsets, schema, keyFieldCount, numBuckets, keyEncoder);
  }

  /**
   * Returns the storage-adapter block size that comfortably fits one lookup's bounded range read
   * (one full bucket payload) in a single wire request. Independent of {@code keyCount}: it scales
   * linearly with {@link #rowsPerBucket} only.
   */
  @Override
  public Integer recommendedReadBlockSize() {
    // ~12 B/row of compressed parquet payload (zstd; matches the calibration in EPHASH) plus a
    // small fixed-cost allowance for the embedded per-bucket footer that each bucket carries.
    long approx = (long) rowsPerBucket * 12L + 4096L;
    long capped = Math.min(Math.max(approx, 4096L), 64L * 1024 * 1024);
    int rounded = Integer.highestOneBit(Math.toIntExact(capped - 1)) << 1;
    return Math.max(rounded, 4096);
  }

  // -----------------------------------------------------------------------
  // Per-phase instrumentation hooks (pass-throughs to Reader's static counters)
  // -----------------------------------------------------------------------

  /**
   * Has {@link #resetInstrumentationOnce()} been called yet in this JVM? Used so that, in
   * {@link org.openjdk.jmh.annotations.Mode#SingleShotTime SingleShotTime} benchmarks where
   * every JMH iteration is exactly one lookup, the warmup-to-measurement boundary triggers the
   * counter reset exactly once -- not on every iteration -- so the {@code @TearDown} dump
   * reports an aggregate over all 1000 measurement lookups instead of just the final one.
   */
  private static final AtomicBoolean INSTRUMENTATION_RESET =
      new AtomicBoolean(false);

  /**
   * Resets the per-phase lookup instrumentation counters (net / open / scan, plus the lookup
   * count). Call only when you really want to wipe the counters mid-run; for the typical "zero
   * once at the warmup-to-measurement boundary" use case prefer {@link
   * #resetInstrumentationOnce()}, which CAS-gates the call so it's a no-op after the first
   * invocation. Safe to call from any thread.
   */
  public static void resetInstrumentation() {
    INSTRUMENTATION_RESET.set(true);
    Reader.resetInstrumentation();
  }

  /**
   * CAS-gated variant of {@link #resetInstrumentation()}: the first caller wipes the counters,
   * every subsequent caller is a no-op. Designed for {@link
   * org.openjdk.jmh.annotations.Setup @Setup(Level.Iteration)} hooks in {@link
   * org.openjdk.jmh.annotations.Mode#SingleShotTime SingleShotTime} benchmarks, where each
   * iteration runs a single invocation and an unconditional per-iteration reset would leave the
   * counter holding only the final iteration's sample.
   *
   * <p>Returns {@code true} if this call performed the reset, {@code false} if the counter had
   * already been reset by an earlier call.
   */
  public static boolean resetInstrumentationOnce() {
    if (INSTRUMENTATION_RESET.compareAndSet(false, true)) {
      Reader.resetInstrumentation();
      return true;
    }

    return false;
  }

  /**
   * Logs a one-line summary of the per-phase lookup instrumentation counters. Call from
   * {@code @TearDown} to surface totals at end-of-run, independent of the periodic in-line log
   * emitted every {@code -Diceberg.cbuckets.instrumentLogEvery=N} lookups (default 1000).
   */
  public static void dumpInstrumentation() {
    Reader.dumpInstrumentation();
  }

  // -----------------------------------------------------------------------
  // One-shot footer read: recover bucket lengths + cumulative offsets
  // -----------------------------------------------------------------------

  private record BucketIndex(int[] lengths, long[] offsets) {}

  /**
   * Process-wide cache for {@code (lengths[], offsets[])} keyed by input file location. Mirrors
   * {@link ParquetIndexHandlerWithEmbeddedMetadata}'s {@code META_OFFSETS_CACHE}: index files are
   * immutable, so the lengths array recovered for a given location is stable for the lifetime of
   * the JVM. Recovering it requires opening the outer Parquet footer once -- a multi-KB read +
   * Thrift parse -- which is wasteful when the same file is reopened many times in a row (e.g. JMH
   * iterations, repeated lookups across short-lived {@link Reader} instances).
   */
  private static final Map<String, BucketIndex> BUCKET_INDEX_CACHE = new HashMap<>();

  /**
   * One-shot footer read used solely to recover the per-bucket lengths array (and derive the
   * cumulative offsets it implies). Results are cached in {@link #BUCKET_INDEX_CACHE} keyed by
   * {@code input.location()} so subsequent reader constructions on the same input file skip the
   * footer read entirely.
   */
  private static synchronized BucketIndex recoverBucketIndex(InputFile input, int numBuckets)
      throws IOException {
    BucketIndex cached = BUCKET_INDEX_CACHE.get(input.location());
    if (cached != null) {
      return cached;
    }
    int[] lengths;
    try (ParquetFileReader pfr =
        ParquetFileReader.open(
            new IcebergParquetInputFile(input),
            ParquetReadOptions.builder(new PlainParquetConfiguration()).build())) {
      Map<String, String> kv = pfr.getFooter().getFileMetaData().getKeyValueMetaData();
      String encoded = kv.get(BUCKET_LENGTHS_KEY);
      if (encoded == null) {
        throw new IOException("Footer missing required key-value metadata: " + BUCKET_LENGTHS_KEY);
      }
      lengths = decodeLengths(encoded);
      if (lengths.length != numBuckets) {
        throw new IOException(
            "Stored bucket-lengths count "
                + lengths.length
                + " does not match numBuckets "
                + numBuckets);
      }
    } catch (IOException e) {
      throw new RuntimeIOException(
          e, "Failed to recover bucket lengths from footer of file: %s", input.location());
    }
    long[] offsets = new long[numBuckets];
    long acc = 0L;
    for (int i = 0; i < numBuckets; i++) {
      offsets[i] = acc;
      acc += lengths[i];
    }
    BucketIndex idx = new BucketIndex(lengths, offsets);
    BUCKET_INDEX_CACHE.put(input.location(), idx);
    return idx;
  }

  private static String encodeLengths(int[] lengths) {
    ByteBuffer buf = ByteBuffer.allocate(lengths.length * 4).order(ByteOrder.LITTLE_ENDIAN);
    for (int l : lengths) {
      buf.putInt(l);
    }
    return Base64.getEncoder().encodeToString(buf.array());
  }

  private static int[] decodeLengths(String encoded) {
    byte[] bytes = Base64.getDecoder().decode(encoded);
    int n = bytes.length / 4;
    int[] out = new int[n];
    ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < n; i++) {
      out[i] = buf.getInt();
    }
    return out;
  }

  // -----------------------------------------------------------------------
  // Shared helpers (used by both Writer and Reader)
  // -----------------------------------------------------------------------

  /** Reused across all lookups; thread-safe construction once. */
  private static final ParquetMetadataConverter METADATA_CONVERTER = new ParquetMetadataConverter();

  /**
   * {@link ParquetReadOptions} used on the lookup hot path. All footer-driven filtering is
   * disabled: the bucket has exactly one row group and we never push a predicate -- the row scan
   * does the actual key matching.
   */
  private static final ParquetReadOptions LOOKUP_OPTIONS =
      ParquetReadOptions.builder(new PlainParquetConfiguration())
          .useStatsFilter(false)
          .useColumnIndexFilter(false)
          .useDictionaryFilter(false)
          .useBloomFilter(false)
          .build();

  /**
   * Returns a copy of {@code src} whose column-chunk page offsets are translated by adding {@code
   * shift}. Used by the writer to produce outer-footer block metadata in the concatenated file's
   * coordinate space, and by the reader to translate per-bucket {@link BlockMetaData} (parsed from
   * the bucket's own embedded Parquet footer, with offsets relative to the bucket's PAR1) into the
   * outer file's absolute coordinate space.
   */
  private static BlockMetaData shiftBlock(BlockMetaData src, long shift) {
    BlockMetaData dst = new BlockMetaData();
    dst.setRowCount(src.getRowCount());
    dst.setTotalByteSize(src.getTotalByteSize());
    for (ColumnChunkMetaData c : src.getColumns()) {
      long dictOff = c.getDictionaryPageOffset();
      long firstDataOff = c.getFirstDataPageOffset();
      ColumnChunkMetaData shifted =
          ColumnChunkMetaData.get(
              c.getPath(),
              c.getPrimitiveType(),
              c.getCodec(),
              c.getEncodingStats(),
              c.getEncodings(),
              c.getStatistics(),
              firstDataOff + shift,
              dictOff > 0L ? dictOff + shift : 0L,
              c.getValueCount(),
              c.getTotalSize(),
              c.getTotalUncompressedSize());
      dst.addColumn(shifted);
    }

    return dst;
  }

  // -----------------------------------------------------------------------
  // Writer
  // -----------------------------------------------------------------------

  private static final class Writer implements IndexHandler.Writer {
    private final OutputFile output;
    private final Schema schema;
    private final MessageType parquetSchema;
    private final int keyFieldCount;
    private final int numBuckets;
    private final int rowsPerBucket;
    private final Function<Record, byte[]> keyEncoder;

    private final List<Record> keys = Lists.newArrayList();
    private final List<String> filePaths = Lists.newArrayList();
    private final LongArrayList positions = new LongArrayList();
    private final IntArrayList[] bucketRows;
    private int maxBucketSize;
    private boolean closed;

    Writer(
        OutputFile output,
        Schema schema,
        MessageType parquetSchema,
        int keyFieldCount,
        int numBuckets,
        int rowsPerBucket,
        Function<Record, byte[]> keyEncoder) {
      this.output = output;
      this.schema = schema;
      this.parquetSchema = parquetSchema;
      this.keyFieldCount = keyFieldCount;
      this.numBuckets = numBuckets;
      this.rowsPerBucket = rowsPerBucket;
      this.keyEncoder = keyEncoder;
      this.bucketRows = new IntArrayList[numBuckets];
      for (int b = 0; b < numBuckets; b++) {
        this.bucketRows[b] = new IntArrayList();
      }
    }

    @Override
    public void add(Record key, String filePath, long pos) {
      Preconditions.checkState(!closed, "Writer already closed");
      Preconditions.checkArgument(key != null, "Key record cannot be null");

      Record snapshot = GenericRecord.create(schema);
      for (int i = 0; i < keyFieldCount; i++) {
        snapshot.set(i, key.get(i));
      }

      int bucket = HashIndexHandler.bucketOf(keyEncoder.apply(snapshot), numBuckets);
      IntArrayList rows = bucketRows[bucket];
      int rowIdx = keys.size();
      rows.add(rowIdx);
      if (rows.size() > maxBucketSize) {
        maxBucketSize = rows.size();
      }

      keys.add(snapshot);
      filePaths.add(filePath);
      positions.add(pos);
    }

    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }

      closed = true;

      if (maxBucketSize > rowsPerBucket) {
        throw new IOException(
            "Bucket overflow: max bucket has "
                + maxBucketSize
                + " entries but rowsPerBucket is "
                + rowsPerBucket);
      }

      LOG.info(
          "ParquetIndexHandlerWithConcatenatedBuckets writing keys={} numBuckets={} maxBucket={}",
          keys.size(),
          numBuckets,
          maxBucketSize);

      int[] lengths = new int[numBuckets];
      List<BlockMetaData> outerBlocks = Lists.newArrayListWithCapacity(numBuckets);

      try (PositionOutputStream out = output.createOrOverwrite()) {
        long fileOffset = 0L;
        for (int b = 0; b < numBuckets; b++) {
          // 1) Materialize bucket b as a complete in-memory single-row-group Parquet file (no
          //    padding -- buckets are emitted at their natural occupancy).
          InMemoryOutputFile inMem = new InMemoryOutputFile();
          IntArrayList rows = bucketRows[b];
          int size = rows.size();
          try (FileAppender<Record> appender = newAppender(inMem)) {
            for (int j = 0; j < size; j++) {
              int origRow = rows.getInt(j);
              Record record = keys.get(origRow);
              record.set(keyFieldCount, filePaths.get(origRow));
              record.set(keyFieldCount + 1, positions.getLong(origRow));
              appender.add(record);
            }
          }

          byte[] bucketBytes = inMem.toByteArray();
          if (bucketBytes.length == 0) {
            throw new IOException("Bucket " + b + " produced empty parquet payload");
          }

          // 2) Recover the single row group's metadata so we can shift its column-chunk offsets
          //    into the outer file's coordinate space. The outer footer that we append in step
          //    4 references those shifted offsets so generic Parquet tooling can still open the
          //    concatenated file end-to-end.
          BlockMetaData srcBlock;
          try (ParquetFileReader pfr =
              ParquetFileReader.open(
                  new IcebergParquetInputFile(InMemoryInputFile.wrap(bucketBytes)),
                  ParquetReadOptions.builder(new PlainParquetConfiguration()).build())) {
            List<BlockMetaData> blocks = pfr.getRowGroups();
            if (blocks.size() != 1) {
              throw new IOException(
                  "Expected exactly 1 row group per bucket, got " + blocks.size());
            }

            srcBlock = blocks.get(0);
          }

          outerBlocks.add(shiftBlock(srcBlock, fileOffset));

          // 3) Copy the bucket's parquet bytes verbatim into the outer file. Bucket 0's leading
          //    PAR1 doubles as the outer file's leading magic.
          out.write(bucketBytes);
          lengths[b] = bucketBytes.length;
          fileOffset += bucketBytes.length;
        }

        // 4) Append a valid outer Parquet footer with bucket-lengths in kv-metadata.
        writeOuterFooter(out, parquetSchema, outerBlocks, lengths);
      }

      LOG.info(
          "ParquetIndexHandlerWithConcatenatedBuckets footer: {} block(s), totalBucketBytes={}",
          numBuckets,
          sumLengths(lengths));

      validateWrittenFile();
    }

    private static long sumLengths(int[] lengths) {
      long s = 0L;
      for (int l : lengths) {
        s += l;
      }

      return s;
    }


    /**
     * Serializes a standard Parquet footer (Thrift {@code FileMetaData} + {@code [int32
     * footerLen][PAR1]} trailer) at the current stream position. The kv-metadata carries the
     * per-bucket lengths array under {@link #BUCKET_LENGTHS_KEY}.
     */
    private static void writeOuterFooter(
        PositionOutputStream out,
        MessageType parquetSchema,
        List<BlockMetaData> blocks,
        int[] lengths)
        throws IOException {
      Map<String, String> kv = Maps.newHashMapWithExpectedSize(1);
      kv.put(BUCKET_LENGTHS_KEY, encodeLengths(lengths));
      FileMetaData fmd = new FileMetaData(parquetSchema, kv, "iceberg-index-concat");
      ParquetMetadata pmd = new ParquetMetadata(fmd, blocks);
      ParquetMetadataConverter converter = new ParquetMetadataConverter();
      org.apache.parquet.format.FileMetaData thrift = converter.toParquetMetadata(1, pmd);

      long footerStart = out.getPos();
      Util.writeFileMetaData(thrift, out);
      long footerLen = out.getPos() - footerStart;

      ByteBuffer lenBuf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
      lenBuf.putInt(Math.toIntExact(footerLen));
      out.write(lenBuf.array());
      out.write(ParquetFileWriter.MAGIC);
    }

    private FileAppender<Record> newAppender(OutputFile bucketOutput) throws IOException {
      // Force a single row group per bucket: huge rowgroup-size hint plus a row-count check
      // window pinned at rowsPerBucket so GenericParquetWriter never speculatively rolls.
      String rgRows = Integer.toString(rowsPerBucket);
      Parquet.WriteBuilder builder =
          Parquet.write(bucketOutput)
              .schema(schema)
              .createWriterFunc(GenericParquetWriter::create)
              .set(TableProperties.PARQUET_COMPRESSION, "zstd")
              .set(
                  TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Integer.toString(Integer.MAX_VALUE))
              .set(TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT, rgRows)
              .set(TableProperties.PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT, rgRows)
              .overwrite();
      // Suppress per-column min/max stats: lookups jump straight to the bucket and never push
      // a predicate, so column-chunk stats are pure footer bloat.
      for (Types.NestedField f : schema.columns()) {
        builder =
            builder.set(TableProperties.PARQUET_COLUMN_STATS_ENABLED_PREFIX + f.name(), "false");
      }

      return builder.build();
    }

    /**
     * Reopens the just-written file via Iceberg's high-level {@link Parquet#read(InputFile)}
     * pipeline (the same code path real callers use) and asserts that the multiset of {@code
     * (file_path, pos)} pairs equals exactly what the caller {@link #add added}. Throws {@link
     * IOException} on any mismatch so the writer's {@code close()} surfaces the problem.
     */
    private void validateWrittenFile() throws IOException {
      InputFile in = output.toInputFile();

      Set<String> expected = Sets.newHashSetWithExpectedSize(keys.size() * 2 + 1);
      for (int i = 0; i < keys.size(); i++) {
        expected.add(filePaths.get(i) + "\u0001" + positions.getLong(i));
      }

      int total = 0;
      try (CloseableIterable<Record> records =
          Parquet.read(in)
              .project(schema)
              .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
              .reuseContainers()
              .build()) {
        for (Record r : records) {
          total++;
          Object filePathVal = r.getField(FILE_PATH_COLUMN);
          Object posVal = r.getField(POS_COLUMN);
          if (filePathVal == null || posVal == null) {
            throw new IOException(
                "validateWrittenFile: row has key but null payload (filePath="
                    + filePathVal
                    + ", pos="
                    + posVal
                    + ")");
          }

          String composite = filePathVal + "\u0001" + posVal;
          if (!expected.remove(composite)) {
            throw new IOException(
                "validateWrittenFile: unexpected (filePath, pos)=("
                    + filePathVal
                    + ", "
                    + posVal
                    + ")");
          }
        }
      }

      if (total != keys.size()) {
        throw new IOException(
            "validateWrittenFile: total rows=" + total + " expected=" + keys.size());
      }

      if (!expected.isEmpty()) {
        throw new IOException(
            "validateWrittenFile: missing "
                + expected.size()
                + " expected (filePath, pos) entries; first="
                + expected.iterator().next());
      }

      LOG.info(
          "ParquetIndexHandlerWithConcatenatedBuckets validateWrittenFile: OK ({} rows across {} blocks)",
          total,
          numBuckets);
    }
  }

  // -----------------------------------------------------------------------
  // Reader
  // -----------------------------------------------------------------------

  private static final class Reader implements IndexHandler.Reader {

    // -----------------------------------------------------------------------
    // Per-phase instrumentation -- helps decide whether the per-lookup cost
    // sits in the network ranged read, the per-bucket Parquet open (footer
    // Thrift parse + reader-builder construction + zstd decompressor init),
    // or the actual page decode / row scan. Always-on; the three nanoTime
    // calls in the hot path cost <1us vs the multi-millisecond phases they
    // bracket. Disable per-window logging by setting the system property to 0:
    //
    //   -Diceberg.cbuckets.instrumentLogEvery=0
    //
    // The benchmark may call resetInstrumentation() between warmup and
    // measurement and dumpInstrumentation() in @TearDown to print final totals.
    // -----------------------------------------------------------------------
    private static final long INSTRUMENT_LOG_EVERY =
        Long.getLong("iceberg.cbuckets.instrumentLogEvery", 1000L);
    private static final AtomicLong NET_NANOS = new AtomicLong();
    private static final AtomicLong OPEN_NANOS = new AtomicLong();
    private static final AtomicLong SCAN_NANOS = new AtomicLong();
    private static final AtomicLong LOOKUP_COUNT = new AtomicLong();

    /** Resets the per-phase instrumentation counters. Safe to call from any thread. */
    public static void resetInstrumentation() {
      NET_NANOS.set(0L);
      OPEN_NANOS.set(0L);
      SCAN_NANOS.set(0L);
      LOOKUP_COUNT.set(0L);
    }

    /**
     * Logs a one-line summary of the per-phase instrumentation counters at INFO level. Reports
     * both totals (us) and per-op averages (us/op). Safe to call concurrently with active
     * lookups; values are a coherent snapshot only if no lookups are in flight.
     */
    public static void dumpInstrumentation() {
      long n = LOOKUP_COUNT.get();
      if (n == 0L) {
        LOG.info("CBUCKETS instrumentation: no lookups recorded");
        return;
      }

      long net = NET_NANOS.get();
      long open = OPEN_NANOS.get();
      long scan = SCAN_NANOS.get();
      LOG.info(
          "CBUCKETS instrumentation @ {} lookups: "
              + "net={} us/op ({} us total), open={} us/op ({} us total), "
              + "scan={} us/op ({} us total), sum={} us/op",
          n,
          net / n / 1000L,
          net / 1000L,
          open / n / 1000L,
          open / 1000L,
          scan / n / 1000L,
          scan / 1000L,
          (net + open + scan) / n / 1000L);
    }

    /**
     * Records one lookup's per-phase wall-clock and, every {@link #INSTRUMENT_LOG_EVERY} calls,
     * emits a running summary at INFO level. {@code netNanos} is the bucket-payload ranged read,
     * {@code openNanos} is the in-memory Parquet open (footer Thrift parse + reader-builder
     * construction), {@code scanNanos} is the row-iteration loop including {@code records.close()}.
     */
    private static void recordTimings(long netNanos, long openNanos, long scanNanos) {
      NET_NANOS.addAndGet(netNanos);
      OPEN_NANOS.addAndGet(openNanos);
      SCAN_NANOS.addAndGet(scanNanos);
      long n = LOOKUP_COUNT.incrementAndGet();
      if (INSTRUMENT_LOG_EVERY > 0L && n % INSTRUMENT_LOG_EVERY == 0L) {
        dumpInstrumentation();
      }
    }

    private final InputFile input;
    private final int[] lengths;
    private final long[] offsets;
    private final int keyFieldCount;
    private final List<Types.NestedField> keyFields;
    private final int filePathOrd;
    private final int posOrd;
    private final int numBuckets;
    private final Function<Record, byte[]> keyEncoder;

    /**
     * Caches derived <em>only</em> from the on-disk schema (not from any footer read). Both the
     * writer and the reader build the same {@link MessageType} via {@link
     * ParquetSchemaUtil#convert(Schema, String)}, so the column IO pipeline is deterministic and
     * needs no runtime discovery.
     */
    private final FileMetaData fileMetaData;

    private final MessageColumnIO columnIO;
    private final GroupRecordConverter converter;

    /**
     * Construct a reader from the externally-recovered per-bucket lengths and the cumulative
     * offsets they imply. This constructor performs <em>no</em> I/O against {@code input}: the
     * outer Parquet footer is opened at most once per file location -- in {@link
     * #recoverBucketIndex(InputFile, int)} -- and cached process-wide. Critically, on the lookup
     * hot path the outer footer is never consulted: each {@link #lookup(Record)} navigates to the
     * start of the targeted bucket parquet file, parses that bucket's own embedded footer, and
     * uses the column-chunk offsets it carries (after a constant {@code +offsets[bucket]} shift)
     * to drive a single low-level {@link ParquetFileReader#readRowGroup} call.
     */
    Reader(
        InputFile input,
        int[] lengths,
        long[] offsets,
        Schema schema,
        int keyFieldCount,
        int numBuckets,
        Function<Record, byte[]> keyEncoder) {
      Preconditions.checkArgument(
          lengths.length == numBuckets && offsets.length == numBuckets,
          "lengths/offsets length mismatch with numBuckets=%s",
          numBuckets);

      this.input = input;
      this.lengths = lengths;
      this.offsets = offsets;
      this.keyFieldCount = keyFieldCount;
      this.keyFields = schema.columns().subList(0, keyFieldCount);
      this.filePathOrd = keyFieldCount;
      this.posOrd = keyFieldCount + 1;
      this.numBuckets = numBuckets;
      this.keyEncoder = keyEncoder;

      // Derive Parquet-side decode pipeline from the Iceberg schema. Both writer and reader use
      // ParquetSchemaUtil.convert(schema, "table"), so this matches the on-disk per-bucket
      // schema exactly. createdBy is intentionally null -- we never read the outer footer on
      // the hot path, and the bucket footers we do parse aren't consulted for createdBy here.
      MessageType derivedMessageType = ParquetSchemaUtil.convert(schema, "table");
      this.fileMetaData =
          new FileMetaData(derivedMessageType, Collections.emptyMap(), /* createdBy */ null);
      this.columnIO = new ColumnIOFactory(/* createdBy */ null).getColumnIO(derivedMessageType);
      this.converter = new GroupRecordConverter(derivedMessageType);
    }

    @Override
    public IndexHandler.Hit lookup(Record key) throws IOException {
      Preconditions.checkArgument(key != null, "Lookup key cannot be null");

      byte[] encoded = keyEncoder.apply(key);
      int bucket = HashIndexHandler.bucketOf(encoded, numBuckets);
      long bucketStart = offsets[bucket];
      int bucketLen = lengths[bucket];
      if (bucketLen < 12) {
        throw new IOException(
            "Bucket " + bucket + " too small (" + bucketLen + " bytes) to hold a parquet trailer");
      }

      // ---------------------------------------------------------------------------------
      // 1) Single forward ranged read: pull the whole bucket payload into RAM in one go.
      //
      // This is the *only* wire request per lookup. Going end-of-bucket -> footer -> start
      // (the previous "navigate the embedded footer in place" approach) issued three GETs
      // because each backwards seek discarded the storage adapter's prefetch buffer. A
      // single forward read of `bucketLen` bytes -- typically ~24 KB at rowsPerBucket=2000
      // -- collapses that to one round trip and lets the adapter satisfy any internal
      // sub-reads from its already-warm buffer.
      //
      // openMicros / readMicros / wireRequests in JMH should now match EPHASH:
      //   openStreams=1, seeks=1, reads=1 (or 2 if the adapter splits header/payload),
      //   wireRequests=1.
      // ---------------------------------------------------------------------------------
      long t0 = System.nanoTime();
      byte[] bucketBytes = new byte[bucketLen];
      try (SeekableInputStream rawStream = input.newStream()) {
        rawStream.seek(bucketStart);
        readFully(rawStream, bucketBytes);
      }

      // ---------------------------------------------------------------------------------
      // 2) Parse the bucket's own embedded Parquet footer entirely from RAM.
      //
      // The trailer ([int32-LE footerLen][PAR1]) sits at the tail of bucketBytes. Offsets
      // recovered from the bucket's footer are bucket-local (relative to the bucket's
      // PAR1, i.e. the start of bucketBytes), which is exactly the coordinate space of
      // the InMemoryInputFile we wrap below. No shiftBlock() needed on the read path --
      // shiftBlock() is still used by the writer to translate into the outer file's
      // coordinate space for the outer footer.
      // ---------------------------------------------------------------------------------
      if (bucketBytes[bucketLen - 4] != (byte) 'P'
          || bucketBytes[bucketLen - 3] != (byte) 'A'
          || bucketBytes[bucketLen - 2] != (byte) 'R'
          || bucketBytes[bucketLen - 1] != (byte) '1') {
        throw new IOException("Bucket " + bucket + " missing PAR1 trailer");
      }
      int footerLen =
          ByteBuffer.wrap(bucketBytes, bucketLen - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
      if (footerLen <= 0 || footerLen > bucketLen - 8) {
        throw new IOException("Bucket " + bucket + " has invalid footerLen=" + footerLen);
      }
      int footerOff = bucketLen - 8 - footerLen;
      org.apache.parquet.format.FileMetaData formatMd =
          Util.readFileMetaData(new ByteArrayInputStream(bucketBytes, footerOff, footerLen));
      ParquetMetadata bucketPmd = METADATA_CONVERTER.fromParquetMetadata(formatMd);
      List<BlockMetaData> bucketBlocks = bucketPmd.getBlocks();
      if (bucketBlocks.size() != 1) {
        throw new IOException(
            "Bucket "
                + bucket
                + " expected exactly 1 row group in embedded footer, got "
                + bucketBlocks.size());
      }
      // Wrap the bucket-local block metadata in a ParquetMetadata that uses the
      // schema-derived FileMetaData (we don't need anything from the bucket footer's
      // FileMetaData -- schema, createdBy, kv-meta are all redundant with what we cached
      // at construction time).
      ParquetMetadata pmd =
          new ParquetMetadata(fileMetaData, Collections.singletonList(bucketBlocks.get(0)));

      // ---------------------------------------------------------------------------------
      // 3) Hand the prebuilt ParquetMetadata + an in-memory stream straight to
      //    ParquetFileReader. Same low-level decode pipeline as
      //    ParquetIndexHandlerWithEmbeddedMetadata's hot path; all column-chunk reads
      //    issued by readRowGroup(0) are served from RAM.
      // ---------------------------------------------------------------------------------
      InMemoryInputFile bucketInput = InMemoryInputFile.wrap(bucketBytes);
      org.apache.parquet.io.SeekableInputStream parquetStream =
          new IcebergParquetSeekableInputStream(bucketInput.newStream());
      long t1 = System.nanoTime();
      try (ParquetFileReader pfr =
          new ParquetFileReader(
              new IcebergParquetInputFile(bucketInput), pmd, LOOKUP_OPTIONS, parquetStream)) {
        PageReadStore pages = pfr.readRowGroup(0);
        long t2 = System.nanoTime();
        long t3;
        try {
          if (pages == null) {
            return null;
          }

          long rows = pages.getRowCount();
          RecordReader<Group> rrdr = columnIO.getRecordReader(pages, converter);
          for (long i = 0; i < rows; i++) {
            Group g = rrdr.read();
            if (g == null) {
              continue;
            }
            // This handler doesn't pad short buckets, but defensively skip rows whose first
            // key field is null -- they cannot match any non-null key.
            if (g.getFieldRepetitionCount(0) == 0) {
              continue;
            }
            if (!keyMatches(g, key)) {
              continue;
            }
            if (g.getFieldRepetitionCount(filePathOrd) == 0
                || g.getFieldRepetitionCount(posOrd) == 0) {
              continue;
            }

            String filePath = g.getString(filePathOrd, 0);
            long pos = g.getLong(posOrd, 0);
            return new HitImpl(filePath, pos);
          }

          return null;
        } finally {
          // Capture scan timing whether we hit (early return), missed (fall-through return
          // null), or threw -- so the per-phase totals stay coherent across all outcomes.
          t3 = System.nanoTime();
          recordTimings(t1 - t0, t2 - t1, t3 - t2);
        }
      }
    }

    /** Repeated read into {@code buf} until full; throws {@link EOFException} on premature EOF. */
    private static void readFully(org.apache.iceberg.io.SeekableInputStream s, byte[] buf)
        throws IOException {
      int off = 0;
      int rem = buf.length;
      while (rem > 0) {
        int n = s.read(buf, off, rem);
        if (n < 0) {
          throw new EOFException("Unexpected EOF after " + off + " of " + buf.length + " bytes");
        }
        off += n;
        rem -= n;
      }
    }

    /**
     * Field-by-field comparison of a parquet-mr {@link Group} to the lookup key. Type dispatch
     * mirrors the on-disk encoding emitted by Iceberg's Parquet writer.
     */
    private boolean keyMatches(Group row, Record key) {
      for (int i = 0; i < keyFieldCount; i++) {
        Types.NestedField f = keyFields.get(i);
        Object kv = key.get(i);
        if (row.getFieldRepetitionCount(i) == 0) {
          if (kv != null) {
            return false;
          }
          continue;
        }
        if (kv == null) {
          return false;
        }
        switch (f.type().typeId()) {
          case LONG -> {
            if (row.getLong(i, 0) != (long) kv) {
              return false;
            }
          }
          case INTEGER -> {
            if (row.getInteger(i, 0) != (int) kv) {
              return false;
            }
          }
          case STRING -> {
            if (!row.getString(i, 0).equals(kv.toString())) {
              return false;
            }
          }
          case UUID -> {
            UUID u = (UUID) kv;
            byte[] expected = new byte[16];
            ByteBuffer.wrap(expected)
                .putLong(u.getMostSignificantBits())
                .putLong(u.getLeastSignificantBits());
            byte[] actual = row.getBinary(i, 0).getBytes();
            if (actual.length != 16) {
              return false;
            }
            for (int b = 0; b < 16; b++) {
              if (actual[b] != expected[b]) {
                return false;
              }
            }
          }
          default ->
              throw new IllegalStateException(
                  "Unsupported key field type at position " + i + ": " + f.type());
        }
      }

      return true;
    }

    @Override
    public void close() {
      // Each lookup owns its own stream / parquet reader.
    }
  }

  // -----------------------------------------------------------------------
  // Iceberg <-> parquet-mr adapters
  // -----------------------------------------------------------------------

  /**
   * Wraps an Iceberg {@link InputFile} as a parquet-mr {@link org.apache.parquet.io.InputFile}.
   * Used by {@link #recoverBucketIndex(InputFile, int)} for the one-shot outer-footer read and by
   * the {@link Writer} to recover each bucket's {@link BlockMetaData} for offset shifting.
   */
  private static final class IcebergParquetInputFile implements org.apache.parquet.io.InputFile {
    private final InputFile delegate;

    IcebergParquetInputFile(InputFile delegate) {
      this.delegate = delegate;
    }

    @Override
    public long getLength() {
      return delegate.getLength();
    }

    @Override
    public org.apache.parquet.io.SeekableInputStream newStream() {
      return new IcebergParquetSeekableInputStream(delegate.newStream());
    }
  }

  /**
   * Wraps an Iceberg {@link org.apache.iceberg.io.SeekableInputStream} as a parquet-mr {@link
   * DelegatingSeekableInputStream}. Read methods are inherited; only {@code getPos}/{@code seek}
   * need explicit forwarding.
   */
  private static final class IcebergParquetSeekableInputStream
      extends DelegatingSeekableInputStream {
    private final org.apache.iceberg.io.SeekableInputStream src;

    IcebergParquetSeekableInputStream(org.apache.iceberg.io.SeekableInputStream src) {
      super(src);
      this.src = src;
    }

    @Override
    public long getPos() throws IOException {
      return src.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
      if (newPos == src.getPos()) {
        return;
      }

      src.seek(newPos);
    }
  }
}

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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.DelegatingSeekableInputStream;
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
     * Returns a copy of {@code src} whose column-chunk page offsets are translated into the outer
     * file's coordinate space by adding {@code shift}.
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
    private final Schema schema;
    private final List<String> keyFieldNames;
    private final int keyFieldCount;
    private final int numBuckets;
    private final Function<Record, byte[]> keyEncoder;

    /**
     * Construct a reader from the externally-recovered per-bucket lengths and the cumulative
     * offsets they imply. This constructor performs <em>no</em> I/O against {@code input}: the
     * outer Parquet footer is opened at most once per file location -- in {@link
     * #recoverBucketIndex(InputFile, int)} -- and cached process-wide.
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
      this.schema = schema;
      this.keyFieldCount = keyFieldCount;
      List<String> names = Lists.newArrayListWithCapacity(keyFieldCount);
      for (int i = 0; i < keyFieldCount; i++) {
        names.add(schema.columns().get(i).name());
      }

      this.keyFieldNames = Collections.unmodifiableList(names);
      this.numBuckets = numBuckets;
      this.keyEncoder = keyEncoder;
    }

    @Override
    public IndexHandler.Hit lookup(Record key) throws IOException {
      Preconditions.checkArgument(key != null, "Lookup key cannot be null");

      byte[] encoded = keyEncoder.apply(key);
      int bucket = HashIndexHandler.bucketOf(encoded, numBuckets);
      long off = offsets[bucket];
      int len = lengths[bucket];

      // 1) Pull the entire bucket parquet payload into memory. We deliberately route through
      //    SeekableInputStream.seek() + IOUtil.readFully(stream, ...) instead of the
      //    InputFile-based IOUtil.readFully(input, off, buf, 0, len) overload, because the
      //    latter dispatches to RangeReadable.readFully on object-store adapters (S3/ADLS/GCS),
      //    which issues a tight HTTP GET sized exactly to `len`. On ADLS, that path's
      //    per-request fixed cost is not amortized for sub-MB reads -- the buffered-stream
      //    path requests `adls.read.block-size-bytes` and serves subsequent reads locally,
      //    which empirically halves per-lookup latency for ~30-700 KB bucket payloads. See
      //    the per-phase instrumentation block below for the measurement that motivated this
      //    choice. (EPHASH wins against CBUCKETS specifically because it accidentally
      //    benefits from the buffered path via its two-shot read pattern.)
      long t0 = System.nanoTime();
      byte[] bucketBytes = new byte[len];
      try (org.apache.iceberg.io.SeekableInputStream stream = input.newStream()) {
        stream.seek(off);
        IOUtil.readFully(stream, bucketBytes, 0, len);
      }

      long t1 = System.nanoTime();

      // 2) Hand the in-memory bucket bytes to Iceberg's standard Parquet read pipeline and
      //    scan the single row group for the matching key. wrap() adopts the buffer without
      //    a defensive copy -- bucketBytes is owned by this stack frame and never mutated.
      //    The build() call performs the in-memory footer Thrift parse + reader-builder
      //    construction; t2 captures wall-clock at the boundary between "open" and "scan".
      InputFile bucketInput = InMemoryInputFile.wrap(bucketBytes);
      try (CloseableIterable<Record> records =
          Parquet.read(bucketInput)
              .project(schema)
              .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
              .reuseContainers()
              .build()) {
        long t2 = System.nanoTime();
        try {
          for (Record row : records) {
            if (!keyMatches(row, key)) {
              continue;
            }

            Object filePath = row.getField(FILE_PATH_COLUMN);
            Object pos = row.getField(POS_COLUMN);
            if (filePath == null || pos == null) {
              continue;
            }

            return new HitImpl((String) filePath, (Long) pos);
          }

          return null;
        } finally {
          // Capture scan timing whether we hit (early return), missed (fall-through return null),
          // or threw -- so the per-phase totals stay coherent across all outcomes.
          long t3 = System.nanoTime();
          recordTimings(t1 - t0, t2 - t1, t3 - t2);
        }
      }
    }

    private boolean keyMatches(Record row, Record key) {
      for (int i = 0; i < keyFieldCount; i++) {
        Object expected = key.get(i);
        Object actual = row.getField(keyFieldNames.get(i));
        if (expected == null) {
          if (actual != null) {
            return false;
          }
        } else if (!expected.equals(actual)) {
          return false;
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

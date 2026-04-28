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
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import org.apache.hadoop.util.Sets;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parquet-derived inverted-index file format that embeds, before every row group, a compact binary
 * metadata block describing exactly that row group's column chunks. The resulting file is
 * <em>also</em> a fully valid Parquet file -- the writer emits a standard Parquet footer at the end
 * whose {@link BlockMetaData} entries point at the same column chunks the embedded meta blocks
 * describe. Generic Parquet tooling (parquet-tools, Spark, DuckDB, ...) can therefore open the file
 * as-is.
 *
 * <p>Lookups never touch the Parquet footer on the hot path: the reader uses the footer
 * <em>once</em> at open time to recover {@code metaOffsets[]} (one per row group, computed as
 * {@code block.getStartingPos() - metaBlockSize}) and the per-column codec / encoding caches, then
 * performs each {@link Reader#lookup(Record) lookup} via a single small range read of the
 * fixed-size meta block followed by one bounded range read of the targeted row-group payload.
 *
 * <h2>File layout</h2>
 *
 * <pre>
 *   [PAR1]
 *   [meta_block_0][row_group_payload_0]
 *   [meta_block_1][row_group_payload_1]
 *   ...
 *   [meta_block_{B-1}][row_group_payload_{B-1}]
 *   [parquet footer (thrift FileMetaData)][int32 footerLen][PAR1]
 * </pre>
 *
 * <h2>Meta block (per row group)</h2>
 *
 * Fixed size = {@code 8 + numCols * 64} bytes, little-endian. Each per-column slot is
 * self-describing so a lookup needs <em>nothing</em> from the Parquet footer:
 *
 * <pre>
 *   int32  rowCount
 *   int32  numCols  // sanity check
 *   per leaf column c:
 *     int64 firstDataPageOffset    // absolute, in the final file
 *     int64 dictionaryPageOffset   // absolute, -1 if none
 *     int64 valueCount
 *     int64 totalSize              // compressed bytes of the column chunk
 *     int64 totalUncompressedSize
 *     int32 codecOrdinal           // CompressionCodecName.values()[ord]
 *     int32 numEncodings           // 0..MAX_ENCODINGS_PER_COLUMN
 *     int32 encOrdinal[4]          // unused slots padded with 0; only first numEncodings read
 * </pre>
 *
 * The embedded meta block intentionally duplicates information the Parquet footer also carries. The
 * point is that a hot-path lookup can jump straight to {@code metaOffsets[bucket]} and deserialize
 * a fixed-size 8 + 40&nbsp;*&nbsp;numCols byte block with no Thrift parsing, compared to reading
 * and parsing the (potentially multi-MB) Parquet footer.
 *
 * <h2>Bucketing &amp; padding</h2>
 *
 * Bucketing follows {@link ParquetIndexHandlerWithHashedRowGroups}: keys are hashed via {@link
 * HashIndexHandler#bucketOf(byte[], int)}, every short bucket is padded with all-null rows so each
 * on-disk row group holds exactly {@code rowsPerBucket} rows, and bucket overflow is a hard error
 * at write time.
 */
public class ParquetIndexHandlerWithEmbeddedMetadata implements IndexHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(ParquetIndexHandlerWithEmbeddedMetadata.class);

  /** Field name of the source-file path column. */
  public static final String FILE_PATH_COLUMN = "file_path";

  /** Field name of the row-position column. */
  public static final String POS_COLUMN = "pos";

  /** Padding factor applied to the average bucket occupancy to size each row group. */
  private static final double BUCKET_PAD_FACTOR = 1.5;

  /** Magic bytes used both for the file leading magic and the trailer terminator. */
  private static final byte[] MAGIC = new byte[] {'P', 'A', 'R', '1'};

  /**
   * Per-column meta-block payload bytes:
   *
   * <ul>
   *   <li>5 longs (40 B): firstDataPageOffset, dictionaryPageOffset, valueCount, totalSize,
   *       totalUncompressedSize
   *   <li>2 ints (8 B): codecOrdinal, numEncodings
   *   <li>{@link #MAX_ENCODINGS_PER_COLUMN} x int (16 B): encoding ordinals, zero-padded
   * </ul>
   *
   * Total = 64 bytes.
   */
  private static final int META_PER_COL_BYTES = 64;

  /**
   * Upper bound on the number of distinct {@link Encoding}s a single column chunk can advertise via
   * the embedded meta block. Parquet writers typically emit 1-3 encodings per column chunk (e.g.
   * {@code RLE}, {@code PLAIN_DICTIONARY}, {@code RLE_DICTIONARY}); 4 is a comfortable cap.
   */
  private static final int MAX_ENCODINGS_PER_COLUMN = 4;

  /** Meta-block fixed header: rowCount (int32) + numCols (int32). */
  private static final int META_HEADER_BYTES = 8;

  private final Schema schema;
  private final int keyFieldCount;
  private final int numBuckets;
  private final int rowsPerBucket;
  private final int rowGroupRows;
  private final Function<Record, byte[]> keyEncoder;

  /** Cached leaf-column descriptors derived from {@link #schema}. */
  private final ColumnPath[] columnPaths;

  private final PrimitiveType[] primitiveTypes;
  private final int numCols;
  private final MessageType messageType;

  /**
   * @param keySchema schema of the key columns (must contain at least one field; must not contain
   *     fields named {@code file_path} or {@code pos})
   * @param rowGroupRows <em>average</em> number of keys per row group; the actual row-group
   *     capacity is {@code ceil(rowGroupRows * 1.5)}. Must be {@code > 0}.
   * @param expectedKeyCount sizing hint that determines the number of buckets / row groups via
   *     {@code numBuckets = ceil(expectedKeyCount / rowGroupRows)}. Must be {@code > 0}.
   */
  public ParquetIndexHandlerWithEmbeddedMetadata(
      Schema keySchema, int rowGroupRows, long expectedKeyCount) {
    if (keySchema == null || keySchema.columns().isEmpty()) {
      throw new IllegalArgumentException("Key schema must contain at least one field");
    }
    if (rowGroupRows <= 0) {
      throw new IllegalArgumentException("rowGroupRows must be > 0: " + rowGroupRows);
    }
    if (expectedKeyCount <= 0L) {
      throw new IllegalArgumentException("expectedKeyCount must be > 0: " + expectedKeyCount);
    }
    for (Types.NestedField f : keySchema.columns()) {
      if (FILE_PATH_COLUMN.equals(f.name()) || POS_COLUMN.equals(f.name())) {
        throw new IllegalArgumentException(
            "Key schema must not contain a field named '"
                + f.name()
                + "' (reserved for the index payload)");
      }
    }

    this.keyFieldCount = keySchema.columns().size();
    this.rowGroupRows = rowGroupRows;
    long buckets = (expectedKeyCount + rowGroupRows - 1L) / rowGroupRows;
    if (buckets <= 0L || buckets > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("numBuckets out of range: " + buckets);
    }
    this.numBuckets = (int) buckets;
    long padded = (long) Math.ceil((double) rowGroupRows * BUCKET_PAD_FACTOR);
    if (padded <= 0L || padded > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("rowsPerBucket out of range: " + padded);
    }
    this.rowsPerBucket = (int) padded;

    // On-disk schema: key columns (renumbered from 1) + file_path + pos. All optional so we can
    // pad short buckets with all-null rows.
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
    this.numCols = messageType.getColumns().size();
    this.columnPaths = new ColumnPath[numCols];
    this.primitiveTypes = new PrimitiveType[numCols];
    for (int c = 0; c < numCols; c++) {
      org.apache.parquet.column.ColumnDescriptor cd = messageType.getColumns().get(c);
      this.columnPaths[c] = ColumnPath.get(cd.getPath());
      this.primitiveTypes[c] = cd.getPrimitiveType();
    }
  }

  @Override
  public IndexHandler.Writer writer(OutputFile output) {
    return new Writer(
        output,
        schema,
        keyFieldCount,
        numBuckets,
        rowsPerBucket,
        keyEncoder,
        numCols,
        rowGroupRows);
  }

  @Override
  public IndexHandler.Reader reader(InputFile input) throws IOException {
    // Reader is intentionally constructed from a minimal externally-supplied set:
    //   (input, metaOffsets, schema, keyFieldCount, numBuckets, rowsPerBucket, keyEncoder,
    //    numCols).
    // It performs NO I/O against `input` at construction time and never reads the Parquet
    // footer on the lookup path either. To simulate "metaOffsets came from somewhere external
    // (sidecar manifest, compaction job output, ...)" we open the footer here, exactly once
    // outside the Reader, just to recover the offsets. In a real deployment this step would
    // be replaced by reading those offsets from wherever the writer published them.
    long[] metaOffsets = recoverMetaOffsets(input, numBuckets, numCols);
    return new Reader(
        input, metaOffsets, schema, keyFieldCount, numBuckets, rowsPerBucket, keyEncoder, numCols);
  }

  /**
   * Empirically-measured recommended read block sizes (bytes), keyed only by {@code rowGroupRows}.
   *
   * <p>Calibrated by probing the {@code
   * data/data/benchmark/inverted-index/idx-LONG-rows*-ephash-rg*.bin} benchmark files
   * (zstd-compressed, {@code numCols=3} for a long key + {@code file_path} + {@code pos}) with
   * pyarrow and taking the max compressed row-group payload across all row groups, rounded up to
   * the next power of two. Each value is large enough that one bounded range read covers both the
   * small ({@code 8 + numCols * 64}-byte) embedded meta block and the targeted row-group payload in
   * a single wire request.
   *
   * <p>Unlike {@link ParquetIndexHandler#EMPIRICAL_BLOCK_SIZES}, EPHASH values are stable across
   * {@code expectedKeyCount} for the same {@code rowGroupRows}: each row group is independently
   * sized to {@code rowsPerBucket = ceil(rowGroupRows * 1.5)} regardless of the file's total row
   * count, and the Parquet footer is never read on the lookup hot path (it's only consulted once
   * per file at reader construction, then cached process-wide -- see {@link #META_OFFSETS_CACHE}).
   *
   * <pre>
   *   rowGroupRows  rowsPerBucket  measured maxRgComp (1M / 10M)  -> rounded
   *   2000          3000            30 105 /  31 815              -> 32 KB
   *   5000          7500            71 654 /  75 645              -> 128 KB
   *   10 000        15 000         139 070 / 147 899              -> 256 KB
   *   20 000        30 000         269 674 / 292 690              -> 512 KB
   *   50 000        75 000         647 319 / 715 450              -> 1024 KB
   * </pre>
   */
  private static final Map<Integer, Integer> EMPIRICAL_BLOCK_SIZES =
      Map.of(
          2_000, 32 * 1024,
          5_000, 128 * 1024,
          10_000, 256 * 1024,
          20_000, 512 * 1024,
          50_000, 1024 * 1024);

  /**
   * Calibration constants for the fallback path of {@link #recommendedReadBlockSize()}.
   *
   * <ul>
   *   <li>{@link #ESTIMATED_BYTES_PER_ROW} -- compressed bytes per row of row-group payload,
   *       derived empirically (~10 B/row for LONG with zstd; rounded up to 12 B to leave headroom
   *       for non-LONG key types like UUID/STRING).
   * </ul>
   */
  private static final long ESTIMATED_BYTES_PER_ROW = 12L;

  private static final int MIN_RECOMMENDED_BLOCK_BYTES = 4096;
  private static final long MAX_RECOMMENDED_BLOCK_BYTES = 64L * 1024 * 1024;

  /**
   * Returns the storage-adapter block size that comfortably fits one lookup's bounded range read
   * (meta block + row-group payload) in a single wire request.
   *
   * <p>Lookups touch nothing outside the {@code [metaOffsets[bucket], +metaBlockSize +
   * rowGroupPayload]} range, so this estimate is independent of {@code expectedKeyCount}: it scales
   * linearly with {@link #rowsPerBucket} only. For known {@code rowGroupRows} values the answer
   * comes from {@link #EMPIRICAL_BLOCK_SIZES}; otherwise we fall back to {@code rowsPerBucket *
   * ESTIMATED_BYTES_PER_ROW + metaBlockSize}, floored at {@link #MIN_RECOMMENDED_BLOCK_BYTES},
   * capped at {@link #MAX_RECOMMENDED_BLOCK_BYTES}, and rounded up to the next power of two so the
   * recommendation always lands on a "nice" size.
   */
  @Override
  public Integer recommendedReadBlockSize() {
    return recommendedReadBlockSizeFor(rowGroupRows, rowsPerBucket, numCols);
  }

  /**
   * Static counterpart of {@link #recommendedReadBlockSize()} so callers that already have the
   * three sizing inputs (e.g. {@link Writer#close()} for its post-write sanity check) can compute
   * the same value without holding a handler instance.
   */
  static int recommendedReadBlockSizeFor(int rowGroupRows, int rowsPerBucket, int numCols) {
    Integer measured = EMPIRICAL_BLOCK_SIZES.get(rowGroupRows);
    if (measured != null) {
      return measured;
    }

    long metaBlockSize = META_HEADER_BYTES + (long) numCols * META_PER_COL_BYTES;
    long estimatedPayloadBytes = (long) rowsPerBucket * ESTIMATED_BYTES_PER_ROW;
    long candidate = estimatedPayloadBytes + metaBlockSize;
    long capped =
        Math.min(Math.max(candidate, MIN_RECOMMENDED_BLOCK_BYTES), MAX_RECOMMENDED_BLOCK_BYTES);
    int rounded = Integer.highestOneBit(Math.toIntExact(capped - 1)) << 1;
    return Math.max(rounded, MIN_RECOMMENDED_BLOCK_BYTES);
  }

  /**
   * Process-wide cache for {@code metaOffsets[]} keyed by the input file location. Index files are
   * immutable once written, so the offsets recovered for a given location are stable for the
   * lifetime of the JVM. Recovering them requires opening the Parquet footer once -- a multi-KB
   * read + Thrift parse -- which is wasteful when the same file is reopened many times in a row
   * (e.g. JMH iterations, repeated lookups across short-lived {@link Reader} instances).
   *
   * <p>Unbounded with no eviction by design: each entry is just {@code 8 * numBuckets} bytes and
   * the JMH benchmarks that motivate this handler open at most a handful of distinct files per JVM.
   * All access goes through the {@code synchronized} {@link #recoverMetaOffsets} method, so a plain
   * {@link java.util.HashMap} is sufficient.
   */
  private static final Map<String, long[]> META_OFFSETS_CACHE = new HashMap<>();

  /**
   * One-shot footer read used solely to recover {@code metaOffsets[]}. Each row group's
   * column-chunk payload sits {@code metaBlockSize} bytes after its preceding meta block, so {@code
   * metaOffsets[b] = block.getStartingPos() - metaBlockSize}. Results are cached in {@link
   * #META_OFFSETS_CACHE} keyed by {@code input.location()} so subsequent reader constructions on
   * the same input file skip the footer read entirely.
   */
  private static synchronized long[] recoverMetaOffsets(
      InputFile input, int numBuckets, int numCols) throws IOException {
    return META_OFFSETS_CACHE.computeIfAbsent(
        input.location(),
        k -> {
          int metaBlockSize = META_HEADER_BYTES + numCols * META_PER_COL_BYTES;
          long[] offsets;
          try (ParquetFileReader pfr =
              ParquetFileReader.open(
                  new IcebergParquetInputFile(input),
                  ParquetReadOptions.builder(new PlainParquetConfiguration()).build())) {
            List<BlockMetaData> blocks = pfr.getRowGroups();
            if (blocks.size() != numBuckets) {
              throw new IOException(
                  "Footer block count "
                      + blocks.size()
                      + " does not match numBuckets "
                      + numBuckets);
            }
            offsets = new long[numBuckets];
            for (int b = 0; b < numBuckets; b++) {
              offsets[b] = blocks.get(b).getStartingPos() - metaBlockSize;
            }
          } catch (IOException e) {
            throw new RuntimeIOException(
                e,
                "Failed to recover meta offsets from Parquet footer of file: %s",
                input.location());
          }
          return offsets;
        });
  }

  // -----------------------------------------------------------------------
  // Writer
  // -----------------------------------------------------------------------

  private static final class Writer implements IndexHandler.Writer {
    private final OutputFile output;
    private final Schema schema;
    private final int keyFieldCount;
    private final int numBuckets;
    private final int rowsPerBucket;
    private final int numCols;
    private final int rowGroupRows;
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
        int keyFieldCount,
        int numBuckets,
        int rowsPerBucket,
        Function<Record, byte[]> keyEncoder,
        int numCols,
        int rowGroupRows) {
      this.output = output;
      this.schema = schema;
      this.keyFieldCount = keyFieldCount;
      this.numBuckets = numBuckets;
      this.rowsPerBucket = rowsPerBucket;
      this.keyEncoder = keyEncoder;
      this.numCols = numCols;
      this.rowGroupRows = rowGroupRows;
      this.bucketRows = new IntArrayList[numBuckets];
      for (int b = 0; b < numBuckets; b++) {
        this.bucketRows[b] = new IntArrayList(rowsPerBucket);
      }
    }

    @Override
    public void add(Record key, String filePath, long pos) {
      if (closed) {
        throw new IllegalStateException("Writer already closed");
      }
      if (key == null) {
        throw new IllegalArgumentException("Key record cannot be null");
      }

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

      int n = keys.size();
      if (maxBucketSize > rowsPerBucket) {
        throw new IOException(
            "Bucket overflow: max bucket has "
                + maxBucketSize
                + " entries but rowsPerBucket is "
                + rowsPerBucket);
      }

      LOG.info(
          "ParquetIndexHandlerWithEmbeddedMetadata writing keys={} numBuckets={} rowsPerBucket={} maxBucket={}",
          n,
          numBuckets,
          rowsPerBucket,
          maxBucketSize);

      long[] metaOffsets = new long[numBuckets];
      // Per-column globals captured from bucket 0 (writer pins them per file). Kept as locals
      // because the standard Parquet footer at the end already carries the authoritative
      // per-bucket BlockMetaData; these are only used for the optional sanity log below.
      CompressionCodecName[] codecs = new CompressionCodecName[numCols];
      @SuppressWarnings("unchecked")
      Set<Encoding>[] encodings = new Set[numCols];
      String[] createdByHolder = new String[1];
      MessageType[] sourceSchemaHolder = new MessageType[1];

      // Absolute-offset block metadata accumulated as we lay out each bucket -- used to
      // produce a standard Parquet footer so the resulting file is a valid Parquet file.
      List<BlockMetaData> finalBlocks = Lists.newArrayListWithCapacity(numBuckets);

      try (PositionOutputStream out = output.createOrOverwrite()) {
        // Leading magic so the file looks vaguely Parquet-y at the head; the reader does not
        // require it but it makes hex-dumping less surprising.
        out.write(MAGIC);

        Record padding = GenericRecord.create(schema);
        for (int b = 0; b < numBuckets; b++) {
          // 1) Materialize bucket b as a single-row-group Parquet file in memory.
          InMemoryOutputFile inMem = new InMemoryOutputFile();
          try (FileAppender<Record> appender = newAppender(inMem)) {
            IntArrayList rows = bucketRows[b];
            int size = rows.size();
            for (int j = 0; j < size; j++) {
              int origRow = rows.getInt(j);
              Record record = keys.get(origRow);
              record.set(keyFieldCount, filePaths.get(origRow));
              record.set(keyFieldCount + 1, positions.getLong(origRow));
              appender.add(record);
            }
            for (int p = size; p < rowsPerBucket; p++) {
              appender.add(padding);
            }
          }
          byte[] perBucketBytes = inMem.toByteArray();

          // 2) Read back the in-memory parquet to extract the single block's chunk metadata.
          BlockMetaData block;
          FileMetaData blockFileMeta;
          try (ParquetFileReader pfr =
              ParquetFileReader.open(
                  new ByteArrayParquetInputFile(perBucketBytes),
                  ParquetReadOptions.builder(new PlainParquetConfiguration()).build())) {
            List<BlockMetaData> blocks = pfr.getRowGroups();
            if (blocks.size() != 1) {
              throw new IOException(
                  "Expected exactly 1 row group per bucket, got " + blocks.size());
            }
            block = blocks.get(0);
            blockFileMeta = pfr.getFileMetaData();
          }

          if (b == 0) {
            for (int c = 0; c < numCols; c++) {
              ColumnChunkMetaData ccmd = block.getColumns().get(c);
              codecs[c] = ccmd.getCodec();
              encodings[c] = ccmd.getEncodings();
            }
            createdByHolder[0] = blockFileMeta.getCreatedBy();
            sourceSchemaHolder[0] = blockFileMeta.getSchema();
          }

          // 3) Compute the contiguous payload byte range in the in-memory file.
          List<ColumnChunkMetaData> cols = block.getColumns();
          long inMemPayloadStart = Long.MAX_VALUE;
          long inMemPayloadEnd = Long.MIN_VALUE;
          for (int c = 0; c < numCols; c++) {
            ColumnChunkMetaData ccmd = cols.get(c);
            long dictOff = ccmd.getDictionaryPageOffset();
            long firstDataOff = ccmd.getFirstDataPageOffset();
            long chunkStart = (dictOff > 0L && dictOff < firstDataOff) ? dictOff : firstDataOff;
            long chunkEnd = chunkStart + ccmd.getTotalSize();
            if (chunkStart < inMemPayloadStart) {
              inMemPayloadStart = chunkStart;
            }
            if (chunkEnd > inMemPayloadEnd) {
              inMemPayloadEnd = chunkEnd;
            }
          }

          // 4) Record meta-block offset and payload byte range in the FINAL file.
          long metaOffset = out.getPos();
          metaOffsets[b] = metaOffset;
          long metaSize = META_HEADER_BYTES + (long) numCols * META_PER_COL_BYTES;
          long finalPayloadStart = metaOffset + metaSize;
          long shift = finalPayloadStart - inMemPayloadStart;

          // 5) Build & write the meta block. Each per-column slot is self-describing
          //    (offsets, sizes, value count, codec, encodings) so a lookup never needs to
          //    consult the Parquet footer.
          ByteBuffer meta = ByteBuffer.allocate((int) metaSize).order(ByteOrder.LITTLE_ENDIAN);
          meta.putInt((int) block.getRowCount());
          meta.putInt(numCols);
          for (int c = 0; c < numCols; c++) {
            ColumnChunkMetaData ccmd = cols.get(c);
            long dictOff = ccmd.getDictionaryPageOffset();
            meta.putLong(ccmd.getFirstDataPageOffset() + shift);
            meta.putLong(dictOff > 0L ? dictOff + shift : -1L);
            meta.putLong(ccmd.getValueCount());
            meta.putLong(ccmd.getTotalSize());
            meta.putLong(ccmd.getTotalUncompressedSize());
            // codec + encodings
            @SuppressWarnings("EnumOrdinal")
            int codecOrd = ccmd.getCodec().ordinal();
            meta.putInt(codecOrd);
            Set<Encoding> enc = ccmd.getEncodings();
            if (enc.size() > MAX_ENCODINGS_PER_COLUMN) {
              throw new IOException(
                  "Column "
                      + c
                      + " advertises "
                      + enc.size()
                      + " encodings, exceeds MAX_ENCODINGS_PER_COLUMN="
                      + MAX_ENCODINGS_PER_COLUMN);
            }
            meta.putInt(enc.size());
            int written = 0;
            for (Encoding e : enc) {
              @SuppressWarnings("EnumOrdinal")
              int eOrd = e.ordinal();
              meta.putInt(eOrd);
              written++;
            }
            for (int pad = written; pad < MAX_ENCODINGS_PER_COLUMN; pad++) {
              meta.putInt(0);
            }
          }
          out.write(meta.array());

          // 6) Stream the row-group payload bytes from the in-memory file.
          int payloadLen = Math.toIntExact(inMemPayloadEnd - inMemPayloadStart);
          out.write(perBucketBytes, (int) inMemPayloadStart, payloadLen);

          // 7) Build the corresponding absolute-offset BlockMetaData entry for the final
          //    Parquet footer. We mirror the in-memory ColumnChunkMetaData but shift its page
          //    offsets so they point into the final file.
          BlockMetaData finalBlock = new BlockMetaData();
          finalBlock.setRowCount(block.getRowCount());
          long totalUncompressed = 0L;
          for (int c = 0; c < numCols; c++) {
            ColumnChunkMetaData src = cols.get(c);
            long dictOff = src.getDictionaryPageOffset();
            ColumnChunkMetaData shifted =
                ColumnChunkMetaData.get(
                    src.getPath(),
                    src.getPrimitiveType(),
                    src.getCodec(),
                    src.getEncodingStats(),
                    src.getEncodings(),
                    src.getStatistics(),
                    src.getFirstDataPageOffset() + shift,
                    dictOff > 0L ? dictOff + shift : 0L,
                    src.getValueCount(),
                    src.getTotalSize(),
                    src.getTotalUncompressedSize());
            finalBlock.addColumn(shifted);
            totalUncompressed += src.getTotalUncompressedSize();
          }
          finalBlock.setTotalByteSize(totalUncompressed);
          finalBlocks.add(finalBlock);
        }

        // 8) Standard Parquet footer. metaOffsets are derivable as
        //    block.getStartingPos() - metaBlockSize, so they are NOT stored anywhere extra --
        //    the file is now a valid Parquet file and the reader recovers everything it needs
        //    from the parquet footer alone.
        writeParquetFooter(out, finalBlocks, sourceSchemaHolder[0], createdByHolder[0]);
      }

      // Sanity log: which constants the reader will inherit.
      LOG.info(
          "ParquetIndexHandlerWithEmbeddedMetadata footer: {} block(s), codec[0]={}, encodings[0]={}, metaOffsets[0]={}",
          finalBlocks.size(),
          codecs[0],
          encodings[0],
          metaOffsets[0]);

      // Block-size fit check: warn if the recommendedReadBlockSize() value ends up either too
      // small to hold one (meta + max row-group payload) read in a single wire request, or so
      // large that we are recommending the storage adapter prefetch many row groups worth of
      // bytes per lookup.
      checkRecommendedReadBlockSize(finalBlocks);

      // Read-back self-check: open the just-written file via standard Parquet (this on its
      // own validates that the appended footer is well-formed and that its column-chunk
      // offsets land on real pages -- the embedded meta blocks sit between row groups, so any
      // off-by-one in shift / payloadLen would surface here as a decode failure) and verify
      // that every (filePath, pos) the caller submitted comes back from the right bucket.
      validateWrittenFile();
    }

    /**
     * Compares the largest actual {@code (metaBlock + rowGroupPayload)} byte range against {@link
     * #recommendedReadBlockSizeFor}. Logs a warning when the recommendation undershoots (a single
     * lookup will spill into a second wire request) or overshoots by &gt;4x (the storage adapter
     * will prefetch far more bytes than one lookup actually needs).
     */
    private void checkRecommendedReadBlockSize(List<BlockMetaData> finalBlocks) {
      long metaBlockSize = META_HEADER_BYTES + (long) numCols * META_PER_COL_BYTES;
      long maxPayload = 0L;
      for (BlockMetaData b : finalBlocks) {
        long compressed = 0L;
        for (ColumnChunkMetaData c : b.getColumns()) {
          compressed += c.getTotalSize();
        }
        if (compressed > maxPayload) {
          maxPayload = compressed;
        }
      }
      long actualMax = maxPayload + metaBlockSize;
      int recommended = recommendedReadBlockSizeFor(rowGroupRows, rowsPerBucket, numCols);

      if (actualMax > recommended) {
        LOG.warn(
            "EPHASH actual max (meta + row-group) bytes={} exceeds recommendedReadBlockSize={} "
                + "for rowGroupRows={} rowsPerBucket={} numCols={} -- single-lookup reads will "
                + "spill into a second wire request; consider raising the empirical entry.",
            actualMax,
            recommended,
            rowGroupRows,
            rowsPerBucket,
            numCols);
      } else if (actualMax * 2L < recommended) {
        LOG.warn(
            "EPHASH actual max (meta + row-group) bytes={} is <1/4 of recommendedReadBlockSize={} "
                + "for rowGroupRows={} rowsPerBucket={} numCols={} -- the storage adapter will "
                + "prefetch ~{}x more bytes than a single lookup needs; consider lowering the "
                + "empirical entry.",
            actualMax,
            recommended,
            rowGroupRows,
            rowsPerBucket,
            numCols,
            recommended / Math.max(1L, actualMax));
      } else {
        LOG.info(
            "EPHASH actual max (meta + row-group) bytes={} fits comfortably in "
                + "recommendedReadBlockSize={} (rowGroupRows={}, rowsPerBucket={}, numCols={})",
            actualMax,
            recommended,
            rowGroupRows,
            rowsPerBucket,
            numCols);
      }
    }

    /**
     * Reopens the just-written file via Iceberg's high-level {@link Parquet#read(InputFile)}
     * pipeline (the same code path real callers use) and asserts that the multiset of non-padding
     * {@code (file_path, pos)} pairs equals exactly what the caller {@link #add added}. Throws
     * {@link IOException} on any mismatch so the writer's {@code close()} surfaces the problem.
     */
    private void validateWrittenFile() throws IOException {
      InputFile in = output.toInputFile();

      // Build the expected multiset (filePath\u0001pos) once across all buckets -- using a
      // single global set sidesteps having to recover the bucket assignment from the file.
      Set<String> expected = Sets.newHashSetWithExpectedSize(keys.size() * 2 + 1);
      for (int i = 0; i < keys.size(); i++) {
        expected.add(filePaths.get(i) + "\u0001" + positions.getLong(i));
      }
      int totalNonPadding = 0;

      try (org.apache.iceberg.io.CloseableIterable<Record> records =
          Parquet.read(in)
              .project(schema)
              .createReaderFunc(
                  fileSchema ->
                      org.apache.iceberg.data.parquet.GenericParquetReaders.buildReader(
                          schema, fileSchema))
              .reuseContainers()
              .build()) {
        for (Record r : records) {
          // Padding rows have all key columns null -- the writer fills key columns and both
          // payload columns together, so the first key column is a sufficient canary.
          if (r.get(0) == null) {
            continue;
          }
          totalNonPadding++;
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

      if (totalNonPadding != keys.size()) {
        throw new IOException(
            "validateWrittenFile: total non-padding rows="
                + totalNonPadding
                + " expected="
                + keys.size());
      }
      if (!expected.isEmpty()) {
        throw new IOException(
            "validateWrittenFile: missing "
                + expected.size()
                + " expected (filePath, pos) entries; first="
                + expected.iterator().next());
      }
      LOG.info(
          "ParquetIndexHandlerWithEmbeddedMetadata validateWrittenFile: OK ({} rows across {} blocks)",
          totalNonPadding,
          numBuckets);
    }

    private FileAppender<Record> newAppender(OutputFile bucketOutput) throws IOException {
      String rgRows = Integer.toString(rowsPerBucket);
      Parquet.WriteBuilder builder =
          Parquet.write(bucketOutput)
              .schema(schema)
              .createWriterFunc(GenericParquetWriter::create)
              .set(TableProperties.PARQUET_COMPRESSION, "zstd")
              .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "1")
              .set(TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT, rgRows)
              .set(TableProperties.PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT, rgRows)
              .overwrite();
      // Suppress per-column min/max stats: lookups jump to the row group by bucket ordinal and
      // never push a predicate, so column-chunk stats are pure footer bloat -- and we don't even
      // keep the parquet footer.
      for (Types.NestedField f : schema.columns()) {
        builder =
            builder.set(TableProperties.PARQUET_COLUMN_STATS_ENABLED_PREFIX + f.name(), "false");
      }
      return builder.build();
    }

    private void writeParquetFooter(
        PositionOutputStream out,
        List<BlockMetaData> blocks,
        MessageType parquetSchema,
        String createdBy)
        throws IOException {
      ParquetMetadataConverter mdConverter = new ParquetMetadataConverter();
      FileMetaData fmd = new FileMetaData(parquetSchema, Collections.emptyMap(), createdBy);
      ParquetMetadata pmd = new ParquetMetadata(fmd, blocks);
      org.apache.parquet.format.FileMetaData formatMd =
          mdConverter.toParquetMetadata(ParquetFileWriter.CURRENT_VERSION, pmd);
      ByteArrayOutputStream baos = new ByteArrayOutputStream(2048);
      Util.writeFileMetaData(formatMd, baos);
      int footerLen = baos.size();
      out.write(baos.toByteArray());

      ByteBuffer trailer = ByteBuffer.allocate(4 + MAGIC.length).order(ByteOrder.LITTLE_ENDIAN);
      trailer.putInt(footerLen);
      trailer.put(MAGIC);
      out.write(trailer.array());
    }
  }

  // -----------------------------------------------------------------------
  // Reader
  // -----------------------------------------------------------------------

  private static final class Reader implements IndexHandler.Reader {
    private final InputFile input;
    private final int keyFieldCount;
    private final List<Types.NestedField> keyFields;
    private final int filePathOrd;
    private final int posOrd;
    private final Function<Record, byte[]> keyEncoder;

    private final int numCols;
    private final int numBuckets;
    private final long[] metaOffsets;
    private final int metaBlockSize;

    /**
     * Caches derived <em>only</em> from the on-disk schema (not from any footer read). Since both
     * the writer and the reader build the same {@link MessageType} via {@link
     * ParquetSchemaUtil#convert(Schema, String)}, the column paths / primitive types / message type
     * are deterministic and need no runtime discovery.
     */
    private final ColumnPath[] columnPaths;

    private final PrimitiveType[] primitiveTypes;
    private final FileMetaData fileMetaData;
    private final MessageColumnIO columnIO;
    private final GroupRecordConverter converter;

    /**
     * Decoder lookup tables for the per-column codec / encoding ordinals carried inside each meta
     * block. We snapshot the enum {@code values()} arrays once so per-lookup decoding stays
     * branch-free.
     */
    private static final CompressionCodecName[] CODEC_VALUES = CompressionCodecName.values();

    private static final Encoding[] ENCODING_VALUES = Encoding.values();

    private static final ParquetReadOptions LOOKUP_OPTIONS =
        ParquetReadOptions.builder(new PlainParquetConfiguration())
            .useStatsFilter(false)
            .useColumnIndexFilter(false)
            .useDictionaryFilter(false)
            .useBloomFilter(false)
            .build();

    /**
     * Construct a reader from the externally-supplied per-bucket meta-block offsets and a minimal
     * description of the file. Critically, this constructor performs <em>no</em> I/O against {@code
     * input}: the Parquet footer is never opened. Every lookup on the hot path starts at the
     * position determined by hashing the key into a bucket and seeking to {@code
     * metaOffsets[bucket]}.
     *
     * @param input file to range-read from on every {@link #lookup(Record)}
     * @param metaOffsets absolute byte offsets of each per-bucket meta block (length must equal
     *     {@code numBuckets})
     * @param schema on-disk schema (key columns + {@code file_path} + {@code pos}); used only to
     *     rebuild the deterministic Parquet {@link MessageType}
     * @param keyFieldCount number of leading key columns in {@code schema}
     * @param numBuckets total number of row groups / buckets in the file
     * @param rowsPerBucket fixed row-group size used at write time (not used by the lookup path;
     *     kept for symmetry / sanity)
     * @param keyEncoder key → bytes encoder used to compute the bucket via {@link
     *     HashIndexHandler#bucketOf(byte[], int)}
     * @param numCols number of leaf Parquet columns (used to size each meta block)
     */
    Reader(
        InputFile input,
        long[] metaOffsets,
        Schema schema,
        int keyFieldCount,
        int numBuckets,
        int rowsPerBucket,
        Function<Record, byte[]> keyEncoder,
        int numCols) {
      if (metaOffsets.length != numBuckets) {
        throw new IllegalArgumentException(
            "metaOffsets.length=" + metaOffsets.length + " must equal numBuckets=" + numBuckets);
      }
      if (rowsPerBucket <= 0) {
        throw new IllegalArgumentException("rowsPerBucket must be > 0: " + rowsPerBucket);
      }

      this.input = input;
      this.metaOffsets = metaOffsets;
      this.keyFieldCount = keyFieldCount;
      this.keyFields = schema.columns().subList(0, keyFieldCount);
      this.filePathOrd = keyFieldCount;
      this.posOrd = keyFieldCount + 1;
      this.numBuckets = numBuckets;
      this.keyEncoder = keyEncoder;
      this.numCols = numCols;
      this.metaBlockSize = META_HEADER_BYTES + numCols * META_PER_COL_BYTES;

      // Derive Parquet schema-side caches from the Iceberg schema. Both writer and reader use
      // ParquetSchemaUtil.convert(schema, "table"), so this matches the on-disk schema exactly.
      MessageType derivedMessageType = ParquetSchemaUtil.convert(schema, "table");
      if (derivedMessageType.getColumns().size() != numCols) {
        throw new IllegalArgumentException(
            "Schema-derived numCols="
                + derivedMessageType.getColumns().size()
                + " does not match supplied numCols="
                + numCols);
      }
      this.columnPaths = new ColumnPath[numCols];
      this.primitiveTypes = new PrimitiveType[numCols];
      for (int c = 0; c < numCols; c++) {
        org.apache.parquet.column.ColumnDescriptor cd = derivedMessageType.getColumns().get(c);
        this.columnPaths[c] = ColumnPath.get(cd.getPath());
        this.primitiveTypes[c] = cd.getPrimitiveType();
      }
      // createdBy is intentionally null -- we have no footer to read it from. ColumnIOFactory
      // accepts null and treats it as "unknown writer", which is fine for our self-produced files.
      this.fileMetaData =
          new FileMetaData(derivedMessageType, Collections.emptyMap(), /* createdBy */ null);
      this.columnIO = new ColumnIOFactory(/* createdBy */ null).getColumnIO(derivedMessageType);
      this.converter = new GroupRecordConverter(derivedMessageType);
    }

    @Override
    public IndexHandler.Hit lookup(Record key) throws IOException {
      if (key == null) {
        throw new IllegalArgumentException("Lookup key cannot be null");
      }

      byte[] encoded = keyEncoder.apply(key);
      int bucket = HashIndexHandler.bucketOf(encoded, numBuckets);
      long metaOff = metaOffsets[bucket];

      // Open the file ONCE per lookup. Both the meta block read and the subsequent column
      // chunk reads issued by ParquetFileReader.readRowGroup go through this same stream, so
      // (a) the storage adapter's per-stream prefetch buffer can serve them as one contiguous
      // run when its block size covers them, and (b) we incur exactly one openStream per
      // lookup -- no separate stream for the meta-block range read, and no second stream for
      // the parquet reader.
      org.apache.iceberg.io.SeekableInputStream rawStream = input.newStream();
      try {
        // 1) Read the (small, fixed-size) meta block directly off the file.
        byte[] metaBytes = new byte[metaBlockSize];
        rawStream.seek(metaOff);
        readFully(rawStream, metaBytes);
        ByteBuffer mb = ByteBuffer.wrap(metaBytes).order(ByteOrder.LITTLE_ENDIAN);
        int rowCount = mb.getInt();
        int storedCols = mb.getInt();
        if (storedCols != numCols) {
          throw new IOException(
              "Meta block numCols mismatch at bucket " + bucket + ": " + storedCols);
        }

        // 2) Build a synthetic single-block ParquetMetadata. Codec + encodings come from
        //    inside the meta block, so we never consult the Parquet footer.
        BlockMetaData block = new BlockMetaData();
        block.setRowCount(rowCount);
        long totalByteSize = 0L;
        for (int c = 0; c < numCols; c++) {
          long firstDataPageOffset = mb.getLong();
          long dictionaryPageOffset = mb.getLong();
          long valueCount = mb.getLong();
          long totalSize = mb.getLong();
          long totalUncompressedSize = mb.getLong();
          int codecOrd = mb.getInt();
          int numEnc = mb.getInt();
          if (numEnc < 0 || numEnc > MAX_ENCODINGS_PER_COLUMN) {
            throw new IOException(
                "Meta block bucket "
                    + bucket
                    + " column "
                    + c
                    + " has illegal numEncodings="
                    + numEnc);
          }
          Set<Encoding> enc = numEnc == 0 ? Collections.emptySet() : EnumSet.noneOf(Encoding.class);
          for (int j = 0; j < MAX_ENCODINGS_PER_COLUMN; j++) {
            int eOrd = mb.getInt();
            if (j < numEnc) {
              enc.add(ENCODING_VALUES[eOrd]);
            }
          }
          CompressionCodecName codec = CODEC_VALUES[codecOrd];
          Statistics<?> emptyStats = Statistics.createStats(primitiveTypes[c]);
          ColumnChunkMetaData ccmd =
              ColumnChunkMetaData.get(
                  columnPaths[c],
                  primitiveTypes[c],
                  codec,
                  /* encodingStats */ null,
                  enc,
                  emptyStats,
                  firstDataPageOffset,
                  dictionaryPageOffset,
                  valueCount,
                  totalSize,
                  totalUncompressedSize);
          block.addColumn(ccmd);
          totalByteSize += totalUncompressedSize;
        }
        block.setTotalByteSize(totalByteSize);
        ParquetMetadata pmd = new ParquetMetadata(fileMetaData, Collections.singletonList(block));

        // 3) Hand the prebuilt ParquetMetadata + the already-open stream straight to
        //    ParquetFileReader. This constructor (parquet-mr 1.17+) skips the footer read and
        //    reuses the supplied SeekableInputStream for chunk reads -- exactly what we want.
        IcebergParquetSeekableInputStream wrapped =
            new IcebergParquetSeekableInputStream(rawStream);
        // From here on the parquet reader owns the stream; we set rawStream to null so the
        // outer try/finally does NOT close it twice (ParquetFileReader.close closes f).
        rawStream = null;
        try (ParquetFileReader pfr =
            new ParquetFileReader(
                new IcebergParquetInputFile(input), pmd, LOOKUP_OPTIONS, wrapped)) {
          PageReadStore pages = pfr.readRowGroup(0);
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
            if (g.getFieldRepetitionCount(0) == 0) {
              // Padding row.
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
        }
      } finally {
        if (rawStream != null) {
          try {
            rawStream.close();
          } catch (IOException ignore) {
            // best-effort: lookup already failed if we got here with rawStream != null
          }
        }
      }

      return null;
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
      // Each lookup opens and closes its own ParquetFileReader.
    }
  }

  // -----------------------------------------------------------------------
  // Iceberg → parquet-mr InputFile adapters
  // -----------------------------------------------------------------------

  /** Wraps a byte[] buffer as a parquet-mr InputFile (used to re-read in-memory bucket files). */
  private static final class ByteArrayParquetInputFile implements org.apache.parquet.io.InputFile {
    private final byte[] data;

    ByteArrayParquetInputFile(byte[] data) {
      this.data = data;
    }

    @Override
    public long getLength() {
      return data.length;
    }

    @Override
    public org.apache.parquet.io.SeekableInputStream newStream() {
      return new ByteArraySeekableInputStream(data);
    }
  }

  private static final class ByteArraySeekableInputStream
      extends org.apache.parquet.io.SeekableInputStream {
    private final byte[] data;
    private int pos;

    ByteArraySeekableInputStream(byte[] data) {
      this.data = data;
      this.pos = 0;
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void seek(long newPos) {
      this.pos = (int) newPos;
    }

    @Override
    public int read() {
      if (pos >= data.length) {
        return -1;
      }
      return data[pos++] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) {
      if (pos >= data.length) {
        return -1;
      }
      int n = Math.min(len, data.length - pos);
      System.arraycopy(data, pos, b, off, n);
      pos += n;
      return n;
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
      readFully(bytes, 0, bytes.length);
    }

    @Override
    public void readFully(byte[] bytes, int start, int len) throws IOException {
      if (pos + len > data.length) {
        throw new EOFException();
      }
      System.arraycopy(data, pos, bytes, start, len);
      pos += len;
    }

    @Override
    public int read(ByteBuffer buf) {
      int rem = buf.remaining();
      if (pos >= data.length) {
        return -1;
      }
      int n = Math.min(rem, data.length - pos);
      buf.put(data, pos, n);
      pos += n;
      return n;
    }

    @Override
    public void readFully(ByteBuffer buf) throws IOException {
      while (buf.hasRemaining()) {
        int n = read(buf);
        if (n < 0) {
          throw new EOFException();
        }
      }
    }

    @Override
    public void close() {
      // no-op
    }
  }

  /**
   * Wraps an Iceberg {@link InputFile} as a parquet-mr {@link org.apache.parquet.io.InputFile}.
   * Used by the reader to feed the real index file to {@code ParquetFileReader.open(...)} for the
   * one-shot footer read at construction time.
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
   * SeekableInputStream}. Read methods are inherited from {@link DelegatingSeekableInputStream};
   * only {@code getPos} / {@code seek} need explicit forwarding.
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
      // Short-circuit no-op seeks. After the lookup reads the meta block, the stream is
      // sitting at exactly firstDataPageOffset (the meta block immediately precedes the
      // row-group payload), but parquet-mr's readRowGroup unconditionally calls
      // f.seek(chunk.getStartingPos()) before issuing chunk reads. Without this guard the
      // CountingFileIO's seek counter ticks for what is bytewise a no-op, and on adapters
      // whose seek() flushes the prefetch buffer (ADLS, S3) it would also throw away the
      // window we just primed.
      if (newPos == src.getPos()) {
        return;
      }
      src.seek(newPos);
    }
  }

  // -----------------------------------------------------------------------
}

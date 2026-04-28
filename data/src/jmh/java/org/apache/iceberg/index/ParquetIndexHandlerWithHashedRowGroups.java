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
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
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
 * Parquet-backed inverted-index file format that places each hash bucket in its own Parquet row
 * group.
 *
 * <p>Keys are bucketed by {@link HashIndexHandler#bucketOf(byte[], int)} (MurmurHash3 + Lemire
 * fastrange — the same routine used by {@link HashIndexHandler}), and each bucket is materialised
 * as a single, fixed-size Parquet row group. Sizing is driven by {@code rowGroupRows} (the
 * <em>average</em> bucket fill, mirroring {@link ParquetIndexHandler}'s parameter of the same name)
 * and the total {@code expectedKeyCount}:
 *
 * <pre>
 *   numBuckets    = ceil(expectedKeyCount / rowGroupRows)
 *   rowsPerBucket = ceil(rowGroupRows * 1.5)
 * </pre>
 *
 * Short buckets are padded with all-null rows so every row group on disk has exactly {@code
 * rowsPerBucket} rows. This means:
 *
 * <ul>
 *   <li>Row group {@code b} holds bucket {@code b}: at lookup time we hash the key, jump straight
 *       to row group {@code bucket}, and read only that one row group — no statistics-based
 *       pruning, no full-file scan.
 *   <li>Statistics, dictionary and bloom-filter metadata are disabled for every column. Since
 *       lookup uses bucket-index addressing and never pushes a predicate, this metadata would only
 *       bloat the footer.
 * </ul>
 *
 * <p>Per-bucket overflow ({@code bucket.size > rowsPerBucket}) is treated as a hard error at write
 * time — pick a smaller {@code rowGroupRows} (or accept a larger 1.5x padding factor) so the
 * worst-case bucket fits.
 */
public class ParquetIndexHandlerWithHashedRowGroups implements IndexHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(ParquetIndexHandlerWithHashedRowGroups.class);

  /** Field name of the source-file path column. */
  public static final String FILE_PATH_COLUMN = "file_path";

  /** Field name of the row-position column. */
  public static final String POS_COLUMN = "pos";

  /** Padding factor applied to the average bucket occupancy to size each row group. */
  private static final double BUCKET_PAD_FACTOR = 1.5;

  private final Schema schema;
  private final int keyFieldCount;
  private final int numBuckets;
  private final int rowsPerBucket;

  /**
   * Shared key encoder built once from the user-supplied {@code keySchema}. The encoder only
   * consults {@code field.type()} (field IDs and nullability are ignored), so we don't need to
   * project the on-disk schema down to its key columns -- the input schema is already exactly
   * what's needed.
   */
  private final Function<Record, byte[]> keyEncoder;

  /**
   * Per-leaf-column descriptors derived once from {@link #schema} via {@link
   * ParquetSchemaUtil#convert(Schema, String)}. They match what the writer emits, so the {@link
   * Reader} can use them when synthesizing per-bucket {@link ParquetMetadata} without re-walking
   * the parsed file's {@code MessageType}.
   */
  private final ColumnPath[] columnPaths;

  private final PrimitiveType[] primitiveTypes;
  private final int numCols;

  /**
   * @param keySchema schema of the key columns (must contain at least one field; must not contain
   *     fields named {@code file_path} or {@code pos})
   * @param rowGroupRows <em>average</em> number of keys per row group; the actual row-group
   *     capacity is {@code ceil(rowGroupRows * 1.5)} so each bucket has slack to absorb hash skew.
   *     Must be {@code > 0}.
   * @param expectedKeyCount sizing hint that, together with {@code rowGroupRows}, determines the
   *     number of buckets / row groups via {@code numBuckets = ceil(expectedKeyCount /
   *     rowGroupRows)}. Must be {@code > 0}.
   */
  public ParquetIndexHandlerWithHashedRowGroups(
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
    long buckets = (expectedKeyCount + rowGroupRows - 1L) / rowGroupRows;
    if (buckets <= 0L || buckets > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          "numBuckets out of range: "
              + buckets
              + " (expectedKeyCount="
              + expectedKeyCount
              + ", rowGroupRows="
              + rowGroupRows
              + ")");
    }
    this.numBuckets = (int) buckets;
    long padded = (long) Math.ceil((double) rowGroupRows * BUCKET_PAD_FACTOR);
    if (padded <= 0L || padded > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          "rowsPerBucket out of range: " + padded + " (rowGroupRows=" + rowGroupRows + ")");
    }
    this.rowsPerBucket = (int) padded;

    // Build the on-disk schema: key columns (renumbered from 1) followed by file_path and pos.
    // All fields are OPTIONAL so we can pad short buckets with all-null rows.
    List<Types.NestedField> fields = Lists.newArrayListWithCapacity(keyFieldCount + 2);
    int id = 1;
    for (Types.NestedField keyField : keySchema.columns()) {
      fields.add(optional(id++, keyField.name(), keyField.type()));
    }
    fields.add(optional(id++, FILE_PATH_COLUMN, Types.StringType.get()));
    fields.add(optional(id, POS_COLUMN, Types.LongType.get()));
    this.schema = new Schema(fields);

    // The encoder only consults field.type() (it ignores both field IDs and nullability), so the
    // user-supplied keySchema is exactly what it needs -- no projection of the on-disk schema is
    // required.
    this.keyEncoder = MinimalPerfectHashFunctionIndexHandler.keyEncoder(keySchema);

    // Derive the leaf-column descriptors directly from the Iceberg schema -- no need to wait for
    // a Reader to parse the file footer. The writer emits a MessageType built by the same
    // ParquetSchemaUtil.convert(...) call, so paths and primitive types are guaranteed to match.
    MessageType messageType = ParquetSchemaUtil.convert(this.schema, "table");
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
    return new Writer(output, schema, keyFieldCount, numBuckets, rowsPerBucket, keyEncoder);
  }

  @Override
  public IndexHandler.Reader reader(InputFile input) throws IOException {
    return new Reader(
        input,
        schema,
        keyFieldCount,
        numBuckets,
        rowsPerBucket,
        keyEncoder,
        numCols,
        columnPaths,
        primitiveTypes);
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

    /**
     * {@link MinimalPerfectHashFunctionIndexHandler#keyEncoder(Schema)} byte form, used only for
     * hashing.
     */
    private final Function<Record, byte[]> keyEncoder;

    private final List<Record> keys = Lists.newArrayList();
    private final List<String> filePaths = Lists.newArrayList();
    private final LongArrayList positions = new LongArrayList();

    /**
     * Per-bucket lists of row indices into {@link #keys} / {@link #filePaths} / {@link #positions}.
     * Populated incrementally by {@link #add} so {@link #close} only needs one pass to emit rows
     * (no separate counting / prefix-sum / scatter phases).
     */
    private final IntArrayList[] bucketRows;

    /**
     * Largest bucket size observed so far -- updated in {@link #add} and validated in {@link
     * #close}.
     */
    private int maxBucketSize;

    private boolean closed;

    Writer(
        OutputFile output,
        Schema schema,
        int keyFieldCount,
        int numBuckets,
        int rowsPerBucket,
        Function<Record, byte[]> keyEncoder) {
      this.output = output;
      this.schema = schema;
      this.keyFieldCount = keyFieldCount;
      this.numBuckets = numBuckets;
      this.rowsPerBucket = rowsPerBucket;
      this.keyEncoder = keyEncoder;
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

      // Snapshot the key into a GenericRecord shaped like the on-disk schema. The trailing
      // file_path / pos columns stay null here and are filled in by close() right before each
      // row is handed to the appender. This single record then serves both the key encoder
      // (reads positions 0..keyFieldCount-1) and the final output row, so we never copy keys
      // again later.
      Record snapshot = GenericRecord.create(schema);
      for (int i = 0; i < keyFieldCount; i++) {
        snapshot.set(i, key.get(i));
      }

      // Hash and bucket the row right here so close() doesn't need a separate counting /
      // prefix-sum / scatter phase. The shared encoder reads positions 0..keyFieldCount-1
      // off the snapshot we just built (the trailing payload nulls are irrelevant).
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
                + rowsPerBucket
                + " (numBuckets="
                + numBuckets
                + ", keys="
                + n
                + "). Increase numBuckets or the padding factor.");
      }

      logSpaceReport(n, maxBucketSize);

      try (FileAppender<Record> writer = newAppender()) {
        // All-null padding row, reused across buckets.
        Record padding = GenericRecord.create(schema);

        for (int b = 0; b < numBuckets; b++) {
          IntArrayList rows = bucketRows[b];
          int size = rows.size();
          for (int j = 0; j < size; j++) {
            int origRow = rows.getInt(j);
            // Reuse the snapshot record taken in add(): key columns are already populated, just
            // stamp file_path / pos into the trailing columns and hand it straight to Parquet.
            Record record = keys.get(origRow);
            record.set(keyFieldCount, filePaths.get(origRow));
            record.set(keyFieldCount + 1, positions.getLong(origRow));
            writer.add(record);
          }
          // Pad the bucket up to rowsPerBucket with all-null rows so every row group has the
          // same number of rows and the bucket ordinal can be used as the row-group ordinal.
          for (int p = size; p < rowsPerBucket; p++) {
            writer.add(padding);
          }
        }
      }
    }

    private FileAppender<Record> newAppender() throws IOException {
      // Force exactly `rowsPerBucket` rows per row group: pin min == max == rowsPerBucket so the
      // size check fires precisely on every Nth record, and set the byte-budget to 1 so the size
      // check unconditionally rolls a new row group.
      String rgRows = Integer.toString(rowsPerBucket);
      Parquet.WriteBuilder builder =
          Parquet.write(output)
              .schema(schema)
              .createWriterFunc(GenericParquetWriter::create)
              .set(TableProperties.PARQUET_COMPRESSION, "zstd")
              .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "1")
              .set(TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT, rgRows)
              .set(TableProperties.PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT, rgRows)
              // Suppress min/max statistics for every column. We never push a predicate
              // (lookup uses the bucket ordinal to pick the row group directly), so column-chunk
              // stats are pure footer bloat.
              .overwrite();
      for (Types.NestedField f : schema.columns()) {
        builder =
            builder.set(TableProperties.PARQUET_COLUMN_STATS_ENABLED_PREFIX + f.name(), "false");
      }
      return builder.build();
    }

    private void logSpaceReport(int n, int maxBucketSize) {
      long totalSlots = (long) numBuckets * rowsPerBucket;
      double slotUtilization = totalSlots == 0 ? 0.0 : (double) n / (double) totalSlots;
      double avgLoad = numBuckets == 0 ? 0.0 : (double) n / (double) numBuckets;
      double skew = avgLoad == 0.0 ? Double.NaN : (double) maxBucketSize / avgLoad;
      LOG.info(
          "ParquetIndexHandlerWithHashedRowGroups space report: keys={}, numBuckets={}, "
              + "rowsPerBucket={}, avgLoad={}, maxBucketSize={} (skew x{}), totalSlots={}, "
              + "slotUtilization={}%",
          n,
          numBuckets,
          rowsPerBucket,
          String.format(java.util.Locale.ROOT, "%.2f", avgLoad),
          maxBucketSize,
          String.format(java.util.Locale.ROOT, "%.2f", skew),
          totalSlots,
          String.format(java.util.Locale.ROOT, "%.2f", slotUtilization * 100.0));
    }
  }

  // -----------------------------------------------------------------------
  // Reader
  // -----------------------------------------------------------------------

  /**
   * Stateless-per-lookup reader that opens (and closes) a fresh {@link ParquetFileReader} on every
   * {@link #lookup(Record)} call, but <b>without re-reading the Parquet footer</b>. The constructor
   * reads the footer exactly once, distills it down to a compact directory of primitive arrays
   * (per-RG-per-column offsets / sizes / value counts plus per-column codec / encodings), then per
   * lookup synthesizes a single-block {@link ParquetMetadata} from those arrays and feeds it to
   * {@link ParquetFileReader#ParquetFileReader(org.apache.parquet.io.InputFile, ParquetMetadata,
   * ParquetReadOptions) the no-footer-read ctor}. The actual on-the-wire IO per lookup is therefore
   * reduced to the targeted row-group payload only -- no footer HEAD/range read, no metadata parse.
   *
   * <p>The cached state holds zero parquet-mr {@link ColumnChunkMetaData} objects and zero {@link
   * BlockMetaData}s; just primitive arrays and a handful of small per-column ref arrays (path /
   * type / codec / encodings) plus the original {@link FileMetaData} (small: schema + createdBy +
   * KV metadata).
   */
  private static final class Reader implements IndexHandler.Reader {
    private final InputFile input;
    private final int keyFieldCount;
    private final int numBuckets;
    private final List<Types.NestedField> keyFields;
    private final int filePathOrd;
    private final int posOrd;

    private final Function<Record, byte[]> keyEncoder;

    // ---- footer-derived cache -------------------------------------------------------------
    private final FileMetaData fileMetaData;

    /** Number of leaf columns in the on-disk schema. */
    private final int numCols;

    /** Per-column descriptors precomputed by the handler from the on-disk schema (no IO). */
    private final ColumnPath[] columnPaths;

    private final PrimitiveType[] primitiveTypes;

    /**
     * Per-column codec / encodings -- read off RG 0 of the actual file once, then assumed
     * homogeneous across row groups. The writer pins both, so this holds in our setup.
     */
    private final CompressionCodecName[] codecs;

    private final Set<Encoding>[] encodings;

    /** Per-(RG, col) primitive arrays, flattened as {@code idx = b * numCols + c}. */
    private final long[] firstDataPageOffsets;

    private final long[] dictionaryPageOffsets;
    private final long[] totalSizes;
    private final long[] totalUncompressedSizes;
    private final long[] valueCounts;

    /**
     * Constant row count per row group: the writer pads every short bucket up to this value, so
     * every block on disk holds exactly {@code rowsPerBucket} rows. Supplied by the handler (which
     * derives it from {@code rowGroupRows} at construction time) -- no need to sample it from the
     * footer.
     */
    private final int rowsPerBucket;

    /** Cached decode pipeline, derived once from the file schema. */
    private final MessageColumnIO columnIO;

    private final GroupRecordConverter converter;

    /** Reusable across lookups; thread-safe construction once. */
    private final ParquetMetadataConverter metadataConverter = new ParquetMetadataConverter();

    private static final ParquetReadOptions LOOKUP_OPTIONS =
        ParquetReadOptions.builder(new PlainParquetConfiguration())
            .useStatsFilter(false)
            .useColumnIndexFilter(false)
            .useDictionaryFilter(false)
            .useBloomFilter(false)
            .build();

    @SuppressWarnings("unchecked")
    Reader(
        InputFile input,
        Schema schema,
        int keyFieldCount,
        int numBuckets,
        int rowsPerBucket,
        Function<Record, byte[]> keyEncoder,
        int numCols,
        ColumnPath[] columnPaths,
        PrimitiveType[] primitiveTypes)
        throws IOException {
      this.input = input;
      this.keyFieldCount = keyFieldCount;
      this.numBuckets = numBuckets;
      this.rowsPerBucket = rowsPerBucket;
      this.keyFields = schema.columns().subList(0, keyFieldCount);
      this.filePathOrd = keyFieldCount;
      this.posOrd = keyFieldCount + 1;
      this.keyEncoder = keyEncoder;
      this.numCols = numCols;
      this.columnPaths = columnPaths;
      this.primitiveTypes = primitiveTypes;

      // One-shot footer parse: extract everything we'll need later, then drop the
      // ParquetFileReader / BlockMetaData / ColumnChunkMetaData objects on the floor.
      try (ParquetFileReader pfr =
          ParquetFileReader.open(new IcebergParquetInputFile(input), LOOKUP_OPTIONS)) {
        List<BlockMetaData> blocks = pfr.getRowGroups();
        if (blocks.size() != numBuckets) {
          throw new IOException(
              "Index file row-group count ("
                  + blocks.size()
                  + ") does not match expected numBuckets ("
                  + numBuckets
                  + ") for "
                  + input.location());
        }

        this.fileMetaData = pfr.getFileMetaData();
        MessageType fileSchema = fileMetaData.getSchema();

        this.codecs = new CompressionCodecName[numCols];
        this.encodings = (Set<Encoding>[]) new Set[numCols];
        // Take per-column codec / encodings from RG 0 (homogeneous in our writer setup).
        BlockMetaData first = blocks.get(0);
        for (int c = 0; c < numCols; c++) {
          ColumnChunkMetaData ccmd = first.getColumns().get(c);
          this.codecs[c] = ccmd.getCodec();
          this.encodings[c] = ccmd.getEncodings();
        }

        int n = numBuckets * numCols;
        this.firstDataPageOffsets = new long[n];
        this.dictionaryPageOffsets = new long[n];
        this.totalSizes = new long[n];
        this.totalUncompressedSizes = new long[n];
        this.valueCounts = new long[n];

        for (int b = 0; b < numBuckets; b++) {
          BlockMetaData bm = blocks.get(b);
          List<ColumnChunkMetaData> cols = bm.getColumns();
          for (int c = 0; c < numCols; c++) {
            ColumnChunkMetaData ccmd = cols.get(c);
            int idx = b * numCols + c;
            firstDataPageOffsets[idx] = ccmd.getFirstDataPageOffset();
            dictionaryPageOffsets[idx] = ccmd.getDictionaryPageOffset();
            totalSizes[idx] = ccmd.getTotalSize();
            totalUncompressedSizes[idx] = ccmd.getTotalUncompressedSize();
            valueCounts[idx] = ccmd.getValueCount();
          }
        }

        // Cache decode pipeline (derived purely from schema + createdBy, no IO).
        this.columnIO = new ColumnIOFactory(fileMetaData.getCreatedBy()).getColumnIO(fileSchema);
        this.converter = new GroupRecordConverter(fileSchema);
      }
    }

    @Override
    public IndexHandler.Hit lookup(Record key) throws IOException {
      if (key == null) {
        throw new IllegalArgumentException("Lookup key cannot be null");
      }

      byte[] encoded = keyEncoder.apply(key);
      int bucket = HashIndexHandler.bucketOf(encoded, numBuckets);

      // Synthesize a single-block ParquetMetadata for just the bucket we need. No new IO yet.
      ParquetMetadata md =
          buildSingleBlockMetadata(
              fileMetaData,
              bucket,
              numCols,
              rowsPerBucket,
              columnPaths,
              primitiveTypes,
              codecs,
              encodings,
              firstDataPageOffsets,
              dictionaryPageOffsets,
              valueCounts,
              totalSizes,
              totalUncompressedSizes);

      // Parquet 1.17 has no public ctor that accepts pre-built ParquetMetadata + an InputFile, so
      // we serialize the (now tiny -- 1 block) synthesized footer to thrift bytes and wrap the
      // real input as a "FootedInputFile" that pretends those bytes are appended to the file.
      // ParquetFileReader.open then "reads the footer" -- but it's our synthetic footer (a few
      // hundred bytes, in-memory), not the real ~footerBytes(numBuckets) one on disk. The chunk
      // offsets inside our footer still point into the real file, so readRowGroup(0) seeks
      // straight to the bucket payload.
      byte[] appended = serializeSyntheticFooter(md);

      try (ParquetFileReader pfr =
          ParquetFileReader.open(new FootedInputFile(input, appended), LOOKUP_OPTIONS)) {
        PageReadStore pages = pfr.readRowGroup(0);
        if (pages == null) {
          return null;
        }

        long rowCount = pages.getRowCount();
        RecordReader<Group> rrdr = columnIO.getRecordReader(pages, converter);
        for (long i = 0; i < rowCount; i++) {
          Group g = rrdr.read();
          if (g == null) {
            continue;
          }

          // Padding rows have every column null -- the file_path repetition count is 0 for those.
          // Cheap guard: skip rows whose first key field is missing.
          if (g.getFieldRepetitionCount(0) == 0) {
            continue;
          }

          if (!keyMatches(g, key)) {
            continue;
          }

          if (g.getFieldRepetitionCount(filePathOrd) == 0
              || g.getFieldRepetitionCount(posOrd) == 0) {
            // Defensive: a real entry should have both payload columns populated.
            continue;
          }

          String filePath = g.getString(filePathOrd, 0);
          long pos = g.getLong(posOrd, 0);
          return new HitImpl(filePath, pos);
        }
      }

      return null;
    }

    /**
     * Builds a {@link ParquetMetadata} containing exactly one {@link BlockMetaData} -- the row
     * group for {@code bucket} -- by reading the per-(RG, col) primitive arrays cached at reader
     * construction time. The returned metadata is sufficient for {@link
     * ParquetFileReader#readRowGroup(int) readRowGroup(0)} to seek straight to the bucket's column
     * chunks; {@link Statistics} are emitted empty since this reader never pushes predicates.
     */
    private static ParquetMetadata buildSingleBlockMetadata(
        FileMetaData fileMetaData,
        int bucket,
        int numCols,
        int rowsPerBucket,
        ColumnPath[] columnPaths,
        PrimitiveType[] primitiveTypes,
        CompressionCodecName[] codecs,
        Set<Encoding>[] encodings,
        long[] firstDataPageOffsets,
        long[] dictionaryPageOffsets,
        long[] valueCounts,
        long[] totalSizes,
        long[] totalUncompressedSizes) {
      BlockMetaData block = new BlockMetaData();
      block.setRowCount(rowsPerBucket);
      long totalByteSize = 0L;
      for (int c = 0; c < numCols; c++) {
        int idx = bucket * numCols + c;
        // Empty stats sentinel -- we never push a predicate, so the reader will not consult it.
        Statistics<?> emptyStats = Statistics.createStats(primitiveTypes[c]);
        ColumnChunkMetaData ccmd =
            ColumnChunkMetaData.get(
                columnPaths[c],
                primitiveTypes[c],
                codecs[c],
                /* encodingStats */ null,
                encodings[c],
                emptyStats,
                firstDataPageOffsets[idx],
                dictionaryPageOffsets[idx],
                valueCounts[idx],
                totalSizes[idx],
                totalUncompressedSizes[idx]);
        block.addColumn(ccmd);
        totalByteSize += totalUncompressedSizes[idx];
      }
      block.setTotalByteSize(totalByteSize);
      return new ParquetMetadata(fileMetaData, Collections.singletonList(block));
    }

    /**
     * Serializes a single-block {@link ParquetMetadata} into the on-disk Parquet footer trailer
     * format: {@code [thrift FileMetaData][int32-LE footer length][PAR1]}. The result is what
     * {@link ParquetFileReader#open} expects to find at the tail of an input file.
     */
    private byte[] serializeSyntheticFooter(ParquetMetadata md) throws IOException {
      org.apache.parquet.format.FileMetaData formatMd =
          metadataConverter.toParquetMetadata(ParquetFileWriter.CURRENT_VERSION, md);
      ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
      Util.writeFileMetaData(formatMd, baos);
      int footerLen = baos.size();
      // Append [int32-LE footerLen][PAR1].
      ByteBuffer trailer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
      trailer.putInt(footerLen);
      trailer.put((byte) 'P').put((byte) 'A').put((byte) 'R').put((byte) '1');
      baos.write(trailer.array());
      return baos.toByteArray();
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
          // Row has null at this field -> only matches if the key is also null.
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
  // Iceberg → parquet-mr InputFile adapter (mirrors ParquetIndexHandlerWithPageFilter)
  // -----------------------------------------------------------------------

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
      src.seek(newPos);
    }
  }

  // -----------------------------------------------------------------------
  // FootedInputFile -- overlays a synthetic Parquet footer on top of an Iceberg InputFile.
  // -----------------------------------------------------------------------

  /**
   * Pretends the byte sequence {@code [thrift FileMetaData][int32-LE footerLen][PAR1]} is appended
   * to {@code delegate}. Reads at offsets {@code < delegate.getLength()} fall through to the real
   * Iceberg stream (so row-group payload bytes still come from the actual file); reads at offsets
   * {@code >= delegate.getLength()} are served from the in-memory {@code appended} array. This lets
   * {@link ParquetFileReader#open(org.apache.parquet.io.InputFile, ParquetReadOptions)} parse a
   * tiny single-block footer instead of the real (potentially MB-sized) one.
   */
  private static final class FootedInputFile implements org.apache.parquet.io.InputFile {
    private final InputFile delegate;
    private final byte[] appended;
    private final long delegateLength;

    FootedInputFile(InputFile delegate, byte[] appended) {
      this.delegate = delegate;
      this.appended = appended;
      this.delegateLength = delegate.getLength();
    }

    @Override
    public long getLength() {
      return delegateLength + appended.length;
    }

    @Override
    public SeekableInputStream newStream() {
      return new FootedSeekableInputStream(delegate.newStream(), delegateLength, appended);
    }
  }

  private static final class FootedSeekableInputStream extends SeekableInputStream {
    private final org.apache.iceberg.io.SeekableInputStream src;
    private final long delegateLength;
    private final byte[] appended;
    private long pos;

    FootedSeekableInputStream(
        org.apache.iceberg.io.SeekableInputStream src, long delegateLength, byte[] appended) {
      this.src = src;
      this.delegateLength = delegateLength;
      this.appended = appended;
      this.pos = 0L;
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void seek(long newPos) throws IOException {
      if (newPos < delegateLength) {
        src.seek(newPos);
      }
      pos = newPos;
    }

    @Override
    public int read() throws IOException {
      if (pos < delegateLength) {
        int b = src.read();
        if (b >= 0) {
          pos++;
        }
        return b;
      }
      int idx = (int) (pos - delegateLength);
      if (idx >= appended.length) {
        return -1;
      }
      int b = appended[idx] & 0xFF;
      pos++;
      return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (pos < delegateLength) {
        int avail = (int) Math.min((long) len, delegateLength - pos);
        int n = src.read(b, off, avail);
        if (n > 0) {
          pos += n;
        }
        return n;
      }
      int idx = (int) (pos - delegateLength);
      int avail = appended.length - idx;
      if (avail <= 0) {
        return -1;
      }
      int n = Math.min(len, avail);
      System.arraycopy(appended, idx, b, off, n);
      pos += n;
      return n;
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
      readFully(bytes, 0, bytes.length);
    }

    @Override
    public void readFully(byte[] bytes, int start, int len) throws IOException {
      int off = start;
      int rem = len;
      while (rem > 0) {
        int n = read(bytes, off, rem);
        if (n < 0) {
          throw new EOFException();
        }
        off += n;
        rem -= n;
      }
    }

    @Override
    public int read(ByteBuffer buf) throws IOException {
      if (buf.hasArray()) {
        int n = read(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
        if (n > 0) {
          buf.position(buf.position() + n);
        }
        return n;
      }
      byte[] tmp = new byte[buf.remaining()];
      int n = read(tmp, 0, tmp.length);
      if (n > 0) {
        buf.put(tmp, 0, n);
      }
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
    public void close() throws IOException {
      src.close();
    }
  }
}

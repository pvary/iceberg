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

import static org.apache.iceberg.types.Types.NestedField.required;

import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parquet-backed inverted-index file format.
 *
 * <p>The index is stored as a single Parquet file whose schema is the user-supplied key schema
 * concatenated with two payload columns: {@code file_path} (string) and {@code pos} (long). Each
 * key column is stored in its declared Iceberg type, which lets the Parquet reader push down the
 * usual type-aware filters (min/max, dictionary, bloom filters) and avoids any byte-level encoding
 * on the caller side.
 *
 * <p>Rows are sorted by the key columns at write time so a point lookup touches a single row group
 * (statistics-based row-group skipping). Statistics for the payload columns are disabled because
 * only the key columns participate in predicate push-down.
 *
 * <h2>Page-index based lookup</h2>
 *
 * <p>Iceberg's high-level {@code Parquet.read(...)} pipeline does <em>not</em> use the Parquet
 * column index for page-level pruning today: it implements its own row-group filtering against
 * Iceberg expressions and then reads each surviving row group in full via {@code
 * ParquetFileReader.readNextRowGroup()}. To actually exploit the column index, this handler
 * bypasses {@code Parquet.read(...)} on the read path and drives parquet-mr's {@link
 * ParquetFileReader} directly:
 *
 * <ul>
 *   <li>The lookup builds a parquet-mr {@link FilterPredicate} (only equality + AND are needed) and
 *       installs it on {@link ParquetReadOptions} via {@link
 *       ParquetReadOptions.Builder#withRecordFilter(FilterCompat.Filter)}.
 *   <li>Page-level pruning is enabled via {@link
 *       ParquetReadOptions.Builder#useColumnIndexFilter(boolean)}.
 *   <li>Each iteration calls {@link ParquetFileReader#readNextFilteredRowGroup()}, which returns a
 *       {@link PageReadStore} pruned to the row ranges that can match the predicate (using both the
 *       row-group statistics and the column index).
 *   <li>The pruned store is decoded with parquet-mr's own {@link RecordReader} + {@link
 *       GroupRecordConverter}; we reach into the resulting {@link Group} positionally since the
 *       schema layout is fixed (key columns first, then {@code file_path}, then {@code pos}).
 * </ul>
 *
 * <p>The writer caps the per-data-page row count via {@code parquet.page.row.count.limit} so the
 * column index has multiple per-page min/max entries within each row group; without this cap each
 * row group would be a single ~1&nbsp;MB page and the column index would have nothing to bite into.
 *
 * <p>Each handler instance is bound to both a key {@link Schema} and a {@code rowGroupRows} value
 * that controls how many rows are packed into a single Parquet row group at write time.
 */
public class ParquetIndexHandlerWithPageFilter implements IndexHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(ParquetIndexHandlerWithPageFilter.class);

  /** Field name of the source-file path column. */
  public static final String FILE_PATH_COLUMN = "file_path";

  /** Field name of the row-position column. */
  public static final String POS_COLUMN = "pos";

  /**
   * Target maximum number of rows per Parquet data page. Each page produces one entry in the column
   * index (min/max), so we want enough pages per row group for page-level pruning to be meaningful.
   * With 1 024 rows/page, a 50 000-row row group ends up with ~49 pages and a point lookup can be
   * pruned to a single page.
   *
   * <p>Capped at {@link #rowGroupRows} so we do not force a tinier-than-needed page size for
   * already-small row groups.
   */
  private static final int TARGET_PAGE_ROW_COUNT = 1024;

  // NOTE: do NOT use the parquet-native key "parquet.page.row.count.limit" here.
  // Iceberg's Parquet.WriteBuilder builds ParquetProperties from its own typed property
  // (TableProperties.PARQUET_PAGE_ROW_LIMIT = "write.parquet.page-row-limit") and ignores raw
  // parquet-mr keys passed via .set(...). Setting the parquet-native key only populates the
  // Hadoop Configuration object, which ParquetProperties never reads, so the cap silently
  // falls back to PARQUET_PAGE_ROW_LIMIT_DEFAULT (20_000).

  private final Schema schema;
  private final int keyFieldCount;
  private final int rowGroupRows;

  /**
   * Creates a handler for the given key {@link Schema}. Every {@link Record} supplied to {@link
   * Writer#add(Record, String, long)} and {@link Reader#lookup(Record)} must match it.
   *
   * @param keySchema the schema of the key columns (must contain at least one field and must not
   *     contain fields named {@code file_path} or {@code pos})
   * @param rowGroupRows number of rows packed into each Parquet row group at write time
   */
  public ParquetIndexHandlerWithPageFilter(Schema keySchema, int rowGroupRows) {
    if (keySchema == null || keySchema.columns().isEmpty()) {
      throw new IllegalArgumentException("Key schema must contain at least one field");
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

    // Build the on-disk schema: key columns (renumbered from 1) followed by file_path and pos.
    List<Types.NestedField> fields = Lists.newArrayListWithCapacity(keyFieldCount + 2);
    int id = 1;
    for (Types.NestedField keyField : keySchema.columns()) {
      fields.add(required(id++, keyField.name(), keyField.type()));
    }

    fields.add(required(id++, FILE_PATH_COLUMN, Types.StringType.get()));
    fields.add(required(id, POS_COLUMN, Types.LongType.get()));
    this.schema = new Schema(fields);
  }

  @Override
  public IndexHandler.Writer writer(OutputFile output) {
    return new Writer(output, schema, keyFieldCount, rowGroupRows);
  }

  @Override
  public IndexHandler.Reader reader(InputFile input) {
    return new Reader(input, schema, keyFieldCount);
  }

  // -----------------------------------------------------------------------
  // Writer
  // -----------------------------------------------------------------------

  private static final class Writer implements IndexHandler.Writer {
    private final OutputFile output;
    private final Schema schema;
    private final int keyFieldCount;
    private final int rowGroupRows;
    private final List<Object[]> keyValues = Lists.newArrayList();
    private final List<String> filePaths = Lists.newArrayList();
    private final LongArrayList positions = new LongArrayList();
    private boolean closed;

    Writer(OutputFile output, Schema schema, int keyFieldCount, int rowGroupRows) {
      this.output = output;
      this.schema = schema;
      this.keyFieldCount = keyFieldCount;
      this.rowGroupRows = rowGroupRows;
    }

    @Override
    public void add(Record key, String filePath, long pos) {
      if (closed) {
        throw new IllegalStateException("Writer already closed");
      }

      if (key == null) {
        throw new IllegalArgumentException("Key record cannot be null");
      }

      // Snapshot the key values so the caller is free to reuse the Record instance.
      Object[] snapshot = new Object[keyFieldCount];
      for (int i = 0; i < keyFieldCount; i++) {
        snapshot[i] = key.get(i);
      }

      keyValues.add(snapshot);
      filePaths.add(filePath);
      positions.add(pos);
    }

    /** Sorts the buffered entries by key and writes the Parquet file. */
    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }

      closed = true;

      int n = keyValues.size();
      // Primitive int[] order keeps the sorting workspace at 4 bytes/row.
      int[] order = new int[n];
      for (int i = 0; i < n; i++) {
        order[i] = i;
      }

      IntArrays.quickSort(order, (a, b) -> compareKeys(keyValues.get(a), keyValues.get(b)));

      try (FileAppender<Record> writer = newAppender()) {
        GenericRecord template = GenericRecord.create(schema);
        for (int sortedRow = 0; sortedRow < n; sortedRow++) {
          int origRow = order[sortedRow];
          Record record = template.copy();
          Object[] ks = keyValues.get(origRow);
          for (int i = 0; i < keyFieldCount; i++) {
            record.set(i, ks[i]);
          }

          record.set(keyFieldCount, filePaths.get(origRow));
          record.set(keyFieldCount + 1, positions.getLong(origRow));
          writer.add(record);
        }
      }
    }

    private FileAppender<Record> newAppender() throws IOException {
      // We want roughly `rowGroupRows` rows per row group AND ~TARGET_PAGE_ROW_COUNT rows per
      // data page (so the column index has multiple per-page min/max entries within each row
      // group, enabling page-level pruning at read time).
      //
      // Subtle parquet-mr trap that nuked the previous version of this method: the
      // PARQUET_ROW_GROUP_CHECK_{MIN,MAX}_RECORD_COUNT properties also feed
      // ParquetProperties.{min,max}RowCountForPageSizeCheck, and parquet-mr's bloom-aware
      // ColumnWriteStoreBase ctor (the one Iceberg ends up using because
      // ColumnChunkPageWriteStore implements both PageWriteStore and BloomFilterWriteStore)
      // initialises its internal rowCountForNextSizeCheck to `minRowCountForPageSizeCheck`
      // ALONE -- it does NOT clamp to pageRowCountLimit like the no-bloom ctor. Pinning these
      // to rowGroupRows therefore deferred the first (and, with no estimateNextSizeCheck,
      // every subsequent) page-size check past the end of the file: the entire row group ended
      // up as a single page, no column index entries beyond the row-group min/max, and
      // page-level pruning had nothing to bite into.
      //
      // So: use small page-size-check counts and approximate row-group sizing via a bytes
      // estimate. With a long key (8 B) + ~64-byte file_path + 8 B pos that's ~80 B/row plus
      // encoding overhead; 128 B/row is a comfortable upper bound.
      String pageRowLimit = Integer.toString(Math.min(TARGET_PAGE_ROW_COUNT, rowGroupRows));
      long targetRowGroupBytes = Math.max(1L, (long) rowGroupRows * 128L);
      return Parquet.write(output)
          .schema(schema)
          .createWriterFunc(GenericParquetWriter::create)
          .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Long.toString(targetRowGroupBytes))
          // Page-size check fires every TARGET_PAGE_ROW_COUNT rows so the page row-count limit
          // can actually take effect; max bound is 4x that to absorb estimateNextSizeCheck
          // halving without skipping past a page boundary.
          .set(
              TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT,
              Integer.toString(TARGET_PAGE_ROW_COUNT))
          .set(
              TableProperties.PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT,
              Integer.toString(TARGET_PAGE_ROW_COUNT * 4))
          .set(TableProperties.PARQUET_PAGE_ROW_LIMIT, pageRowLimit)
          // Shrink the byte-based page cap so it doesn't trip before the row-count cap and
          // collapse pages back together (default 1 MB → ~75K rows/page at ~14 B/row).
          .set(TableProperties.PARQUET_PAGE_SIZE_BYTES, "65536")
          // Skip min/max stats for the payload columns -- they are never used for predicate
          // push-down (only the key columns are filtered on) so writing them just bloats the
          // file. The column index for these columns is suppressed for the same reason.
          .set(TableProperties.PARQUET_COLUMN_STATS_ENABLED_PREFIX + FILE_PATH_COLUMN, "false")
          .set(TableProperties.PARQUET_COLUMN_STATS_ENABLED_PREFIX + POS_COLUMN, "false")
          .overwrite()
          .build();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private int compareKeys(Object[] a, Object[] b) {
      for (int i = 0; i < keyFieldCount; i++) {
        Object av = a[i];
        Object bv = b[i];
        if (av == null && bv == null) {
          // go to the next field
          continue;
        } else if (av == null) {
          return -1;
        } else if (bv == null) {
          return 1;
        } else if (av instanceof Comparable) {
          int c = ((Comparable) av).compareTo(bv);
          if (c != 0) {
            return c;
          }
        } else {
          throw new IllegalStateException(
              "Key field " + i + " is not Comparable: " + av.getClass().getName());
        }
      }

      return 0;
    }
  }

  // -----------------------------------------------------------------------
  // Reader
  // -----------------------------------------------------------------------

  /**
   * Lookup reader that drives parquet-mr's {@link ParquetFileReader} directly so it can use the
   * column index for page-level pruning. Each {@link #lookup(Record)} call opens a fresh {@code
   * ParquetFileReader} configured with a {@link FilterPredicate} translated from the key {@link
   * Record}; parquet-mr applies that predicate against both the row-group statistics and the column
   * index, returning a {@link PageReadStore} pruned to the matching row ranges.
   *
   * <p>Decoding goes through parquet-mr's own {@link RecordReader} + {@link GroupRecordConverter}
   * because Iceberg's {@code ParquetValueReader} pipeline does not honour the per-row skip protocol
   * that {@code FilteredPageReadStore} relies on. Since the on-disk schema is fixed ({@code [key
   * columns..., file_path, pos]}) we reach into the resulting {@link Group} positionally; no
   * general-purpose {@code Group} → Iceberg {@code Record} adapter is needed.
   */
  private static final class Reader implements IndexHandler.Reader {
    private final InputFile input;
    private final int keyFieldCount;
    private final List<Types.NestedField> keyFields;

    /** Field ordinal of {@code file_path} in the on-disk schema. */
    private final int filePathOrd;

    /** Field ordinal of {@code pos} in the on-disk schema. */
    private final int posOrd;

    Reader(InputFile input, Schema schema, int keyFieldCount) {
      this.input = input;
      this.keyFieldCount = keyFieldCount;
      this.keyFields = schema.columns().subList(0, keyFieldCount);
      this.filePathOrd = keyFieldCount;
      this.posOrd = keyFieldCount + 1;
    }

    @Override
    public IndexHandler.Hit lookup(Record key) throws IOException {
      if (key == null) {
        throw new IllegalArgumentException("Lookup key cannot be null");
      }

      // Snapshot key field values (also enforces non-null) before building the predicate.
      Object[] keyVals = new Object[keyFieldCount];
      for (int i = 0; i < keyFieldCount; i++) {
        Object v = key.get(i);
        if (v == null) {
          throw new IllegalArgumentException("Key field " + i + " is null");
        }

        keyVals[i] = v;
      }

      FilterPredicate predicate = buildPredicate(keyVals);
      FilterCompat.Filter filterCompat = FilterCompat.get(predicate);

      LOG.warn("Looking up key {} with predicate {}", keyVals, predicate);

      // PlainParquetConfiguration is the Hadoop-free Configuration parquet-mr uses when no
      // org.apache.hadoop.conf.Configuration is supplied. Iceberg uses the same pattern in
      // org.apache.iceberg.parquet.Parquet.ReadBuilder when the input file is not a
      // HadoopInputFile.
      ParquetReadOptions options =
          ParquetReadOptions.builder(new PlainParquetConfiguration())
              .withRecordFilter(filterCompat)
              .useStatsFilter(true) // row-group level min/max pruning
              .useColumnIndexFilter(true) // page level pruning via column index
              .useDictionaryFilter(true)
              .useBloomFilter(true)
              .build();

      try (ParquetFileReader pfr =
          ParquetFileReader.open(new IcebergParquetInputFile(input), options)) {
        MessageType fileSchema = pfr.getFileMetaData().getSchema();
        ColumnIOFactory colIOFactory = new ColumnIOFactory(pfr.getFileMetaData().getCreatedBy());
        MessageColumnIO columnIO = colIOFactory.getColumnIO(fileSchema);
        GroupRecordConverter converter = new GroupRecordConverter(fileSchema);

        // ---- one-time-per-process diagnostic: dump page layout & column-index presence ----
        if (DIAGNOSTIC_DUMPED.compareAndSet(false, true)) {
          dumpPageLayout(pfr);
        }

        PageReadStore pages;
        int blockOrdinal = -1;
        while ((pages = pfr.readNextFilteredRowGroup()) != null) {
          blockOrdinal++;
          long rowCount = pages.getRowCount();
          // Log only the first lookup so we don't spam JMH output.
          if (LOOKUP_LOGGED.compareAndSet(false, true)) {
            LOG.warn(
                "lookup: block#{} rowCount(after column-index pruning)={} (block has {} rows total)",
                blockOrdinal,
                rowCount,
                pfr.getRowGroups().get(blockOrdinal).getRowCount());
          }
          if (rowCount == 0) {
            // Whole row group pruned by stats / column index; advance to the next.
          } else {
            RecordReader<Group> rrdr = columnIO.getRecordReader(pages, converter, filterCompat);
            for (long i = 0; i < rowCount; i++) {
              Group g = rrdr.read();
              if (g == null || rrdr.shouldSkipCurrentRecord()) {
                continue;
              }

              String filePath = g.getString(filePathOrd, 0);
              long pos = g.getLong(posOrd, 0);
              return new HitImpl(filePath, pos);
            }
          }
        }
      }

      return null;
    }

    private static final java.util.concurrent.atomic.AtomicBoolean DIAGNOSTIC_DUMPED =
        new java.util.concurrent.atomic.AtomicBoolean(false);
    private static final java.util.concurrent.atomic.AtomicBoolean LOOKUP_LOGGED =
        new java.util.concurrent.atomic.AtomicBoolean(false);

    private static void dumpPageLayout(ParquetFileReader pfr) {
      try {
        java.util.List<org.apache.parquet.hadoop.metadata.BlockMetaData> blocks =
            pfr.getRowGroups();
        LOG.warn("Parquet file has {} row group(s)", blocks.size());
        for (int b = 0; b < blocks.size(); b++) {
          org.apache.parquet.hadoop.metadata.BlockMetaData block = blocks.get(b);
          LOG.warn(
              "  RG[{}] rows={} totalByteSize={} compressedSize={}",
              b,
              block.getRowCount(),
              block.getTotalByteSize(),
              block.getCompressedSize());
          for (org.apache.parquet.hadoop.metadata.ColumnChunkMetaData c : block.getColumns()) {
            org.apache.parquet.internal.column.columnindex.OffsetIndex oi = pfr.readOffsetIndex(c);
            org.apache.parquet.internal.column.columnindex.ColumnIndex ci = pfr.readColumnIndex(c);
            LOG.warn(
                "    col {} encodings={} pageCount(offsetIndex)={} columnIndexPresent={} totalSize={}",
                c.getPath(),
                c.getEncodings(),
                oi == null ? -1 : oi.getPageCount(),
                ci != null,
                c.getTotalSize());
          }
        }
      } catch (Exception e) {
        LOG.warn("Failed to dump page layout", e);
      }
    }

    /**
     * Translates the lookup key into a parquet-mr {@link FilterPredicate} of equalities, combined
     * via {@link FilterApi#and}. Type dispatch mirrors the on-disk encoding: {@code long}/{@code
     * int} → typed numeric column, {@code string} → {@code binaryColumn} with UTF-8 bytes, {@code
     * uuid} → {@code binaryColumn} with the 16-byte big-endian {@code msb || lsb} layout used by
     * Iceberg's Parquet writer for {@code FIXED_LEN_BYTE_ARRAY(16)}.
     */
    private FilterPredicate buildPredicate(Object[] keyVals) {
      FilterPredicate combined = null;
      for (int i = 0; i < keyFieldCount; i++) {
        Types.NestedField f = keyFields.get(i);
        Object v = keyVals[i];
        FilterPredicate eq;
        switch (f.type().typeId()) {
          case LONG -> eq = FilterApi.eq(FilterApi.longColumn(f.name()), (Long) v);
          case INTEGER -> eq = FilterApi.eq(FilterApi.intColumn(f.name()), (Integer) v);
          case STRING ->
              eq = FilterApi.eq(FilterApi.binaryColumn(f.name()), Binary.fromString(v.toString()));
          case UUID -> {
            UUID u = (UUID) v;
            byte[] bytes = new byte[16];
            ByteBuffer.wrap(bytes)
                .putLong(u.getMostSignificantBits())
                .putLong(u.getLeastSignificantBits());
            eq =
                FilterApi.eq(FilterApi.binaryColumn(f.name()), Binary.fromConstantByteArray(bytes));
          }
          default ->
              throw new IllegalStateException(
                  "Unsupported key field type at position " + i + ": " + f.type());
        }

        combined = (combined == null) ? eq : FilterApi.and(combined, eq);
      }

      // keyFieldCount >= 1 is enforced by the handler ctor, so combined is non-null here.
      return combined;
    }

    @Override
    public void close() {
      // Each lookup opens and closes its own ParquetFileReader.
    }
  }

  // -----------------------------------------------------------------------
  // Iceberg → parquet-mr InputFile adapter
  // -----------------------------------------------------------------------
  //
  // We can't use iceberg-parquet's package-private ParquetIO.file(...) helper, so we adapt the
  // Iceberg InputFile / SeekableInputStream pair to parquet-mr's own InputFile /
  // SeekableInputStream (parquet-mr's version is a separate type with the same name).

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
   * org.apache.parquet.io.SeekableInputStream}. Read methods are inherited from {@link
   * DelegatingSeekableInputStream}; only {@code getPos} / {@code seek} need to be forwarded to the
   * underlying Iceberg stream (the {@code java.io.InputStream} contract is satisfied because {@link
   * org.apache.iceberg.io.SeekableInputStream} extends it).
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
      src.seek(newPos);
    }
  }
}

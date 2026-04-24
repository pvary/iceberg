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
import java.util.List;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.vortex.GenericVortexReader;
import org.apache.iceberg.data.vortex.GenericVortexWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.vortex.VortexFormatModel;

/**
 * Vortex-backed inverted-index file format.
 *
 * <p>The index is stored as a single Vortex file whose schema is the user-supplied key schema
 * concatenated with two payload columns: {@code file_path} (string) and {@code pos} (long). Rows
 * are sorted by the key columns at write time so that range / equality predicates on the keys can
 * be efficiently evaluated by Vortex's pushdown filtering.
 *
 * <p>At lookup time the handler converts the equality on each key column into an Iceberg {@link
 * Expression} and lets the Vortex reader filter the data; the first matching row is returned as a
 * {@link Hit}. Because the writer sorts by key, the matching row -- if any -- is returned with no
 * additional client-side scanning logic.
 */
public class VortexIndexHandler implements IndexHandler {

  /** Field name of the source-file path column. */
  public static final String FILE_PATH_COLUMN = "file_path";

  /** Field name of the row-position column. */
  public static final String POS_COLUMN = "pos";

  private final Schema schema;
  private final int keyFieldCount;

  /**
   * Creates a handler for the given key {@link Schema}. Every {@link Record} supplied to {@link
   * Writer#add(Record, String, long)} and {@link Reader#lookup(Record)} must match it.
   *
   * @param keySchema the schema of the key columns (must contain at least one field and must not
   *     contain fields named {@code file_path} or {@code pos})
   */
  public VortexIndexHandler(Schema keySchema) {
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
    return new Writer(output, schema, keyFieldCount);
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
    private final List<Object[]> keyValues = Lists.newArrayList();
    private final List<String> filePaths = Lists.newArrayList();
    private final LongArrayList positions = new LongArrayList();
    private boolean closed;

    Writer(OutputFile output, Schema schema, int keyFieldCount) {
      this.output = output;
      this.schema = schema;
      this.keyFieldCount = keyFieldCount;
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

    /** Sorts the buffered entries by key and writes the Vortex file. */
    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }

      closed = true;

      int n = keyValues.size();
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
      EncryptedOutputFile encOutput = EncryptedFiles.plainAsEncryptedOutput(output);
      return VortexFormatModel.create(
              Record.class,
              Void.class,
              (icebergSchema, fileSchema, engineSchema) ->
                  GenericVortexWriter.buildWriter(icebergSchema),
              (VortexFormatModel.ReaderFunction<Record>) GenericVortexReader::buildReader)
          .writeBuilder(encOutput)
          .schema(schema)
          .content(FileContent.DATA)
          .overwrite()
          .build();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private int compareKeys(Object[] a, Object[] b) {
      for (int i = 0; i < keyFieldCount; i++) {
        Object av = a[i];
        Object bv = b[i];
        if (av == null && bv == null) {
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
   * Stateless reader: a fresh Vortex scan is opened on every {@link #lookup(Record)} call. The
   * wrapped {@link InputFile} is reused across calls. Filtering is delegated to Vortex via the
   * pushdown predicate built from equality expressions on each key column; the first row returned
   * by the filtered scan is the answer (rows are sorted by the key columns at write time so the
   * matching row, if any, is unique).
   */
  private static final class Reader implements IndexHandler.Reader {
    private final InputFile input;
    private final Schema schema;
    private final int keyFieldCount;
    private final String[] keyColumnNames;

    Reader(InputFile input, Schema schema, int keyFieldCount) {
      this.input = input;
      this.schema = schema;
      this.keyFieldCount = keyFieldCount;
      this.keyColumnNames = new String[keyFieldCount];
      for (int i = 0; i < keyFieldCount; i++) {
        this.keyColumnNames[i] = schema.columns().get(i).name();
      }
    }

    @Override
    public IndexHandler.Hit lookup(Record key) throws IOException {
      if (key == null) {
        throw new IllegalArgumentException("Lookup key cannot be null");
      }

      Object firstVal = requireNonNullKeyField(key, 0);
      Expression filter = Expressions.equal(keyColumnNames[0], firstVal);
      for (int i = 1; i < keyFieldCount; i++) {
        filter =
            Expressions.and(
                filter, Expressions.equal(keyColumnNames[i], requireNonNullKeyField(key, i)));
      }

      try (CloseableIterable<Record> records =
          VortexFormatModel.create(
                  Record.class,
                  Void.class,
                  (icebergSchema, fileSchema, engineSchema) ->
                      GenericVortexWriter.buildWriter(icebergSchema),
                  (VortexFormatModel.ReaderFunction<Record>) GenericVortexReader::buildReader)
              .readBuilder(input)
              .project(schema)
              .filter(filter)
              .build()) {
        try (CloseableIterator<Record> it = records.iterator()) {
          while (it.hasNext()) {
            Record record = it.next();
            // Vortex pushdown filter may be advisory; verify the key columns match exactly before
            // returning a hit.
            if (matchesKey(record, key)) {
              String filePath = (String) record.getField(FILE_PATH_COLUMN);
              long pos = (long) record.getField(POS_COLUMN);
              return new HitImpl(filePath, pos);
            }
          }
        }
      }

      return null;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private boolean matchesKey(Record row, Record key) {
      for (int i = 0; i < keyFieldCount; i++) {
        Object rv = row.get(i);
        Object kv = key.get(i);
        if (rv == null && kv == null) {
          continue;
        }

        if (rv == null || kv == null) {
          return false;
        }

        if (((Comparable) rv).compareTo(kv) != 0) {
          return false;
        }
      }

      return true;
    }

    private Object requireNonNullKeyField(Record key, int position) {
      Object val = key.get(position);
      if (val == null) {
        throw new IllegalArgumentException("Key field " + position + " is null");
      }

      return val;
    }

    @Override
    public void close() {
      // Nothing to close: each lookup opens and closes its own Vortex reader.
    }
  }

  /** Minimal value-object implementation of {@link IndexHandler.Hit}. */
  private record HitImpl(String filePath, long pos) implements IndexHandler.Hit {}
}

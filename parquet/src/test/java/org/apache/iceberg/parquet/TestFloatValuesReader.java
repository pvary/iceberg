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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Tests reading FLOAT columns with {@link FloatValuesReader}, one value at a time and in bulk. */
public class TestFloatValuesReader {
  private static final MessageType FILE_SCHEMA =
      org.apache.parquet.schema.Types.buildMessage()
          .required(PrimitiveTypeName.INT32)
          .id(1)
          .named("id")
          .optional(PrimitiveTypeName.FLOAT)
          .id(2)
          .named("f")
          .named("table");

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), optional(2, "f", Types.FloatType.get()));

  private static final Schema PROMOTED_SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), optional(2, "f", Types.DoubleType.get()));

  private static final float[] SPECIAL_VALUES = {
    0.0f,
    -0.0f,
    1.0f,
    -1.5f,
    Float.MIN_VALUE,
    -Float.MIN_VALUE,
    Float.MIN_NORMAL,
    Float.MAX_VALUE,
    -Float.MAX_VALUE,
    Float.POSITIVE_INFINITY,
    Float.NEGATIVE_INFINITY,
    Float.NaN
  };

  @TempDir private Path temp;

  /** Ways a FLOAT column is encoded by parquet-java writers. */
  private enum FloatEncoding {
    PLAIN(WriterVersion.PARQUET_1_0, false, false, false, Encoding.PLAIN),
    PLAIN_V2(WriterVersion.PARQUET_2_0, false, false, false, Encoding.PLAIN),
    PLAIN_DICTIONARY(WriterVersion.PARQUET_1_0, true, false, false, Encoding.PLAIN_DICTIONARY),
    RLE_DICTIONARY(WriterVersion.PARQUET_2_0, true, false, false, Encoding.RLE_DICTIONARY),
    // the dictionary overflows after the first pages, so pages switch encoding in a row group
    DICTIONARY_FALLBACK(
        WriterVersion.PARQUET_1_0, true, true, false, Encoding.PLAIN_DICTIONARY, Encoding.PLAIN),
    BYTE_STREAM_SPLIT(WriterVersion.PARQUET_1_0, false, false, true, Encoding.BYTE_STREAM_SPLIT),
    BYTE_STREAM_SPLIT_V2(WriterVersion.PARQUET_2_0, false, false, true, Encoding.BYTE_STREAM_SPLIT);

    private final WriterVersion writerVersion;
    private final boolean dictionary;
    private final boolean smallDictionary;
    private final boolean byteStreamSplit;
    private final Set<Encoding> dataEncodings;

    FloatEncoding(
        WriterVersion writerVersion,
        boolean dictionary,
        boolean smallDictionary,
        boolean byteStreamSplit,
        Encoding... dataEncodings) {
      this.writerVersion = writerVersion;
      this.dictionary = dictionary;
      this.smallDictionary = smallDictionary;
      this.byteStreamSplit = byteStreamSplit;
      this.dataEncodings = ImmutableSet.copyOf(dataEncodings);
    }
  }

  @ParameterizedTest
  @EnumSource(FloatEncoding.class)
  public void testReadEncodings(FloatEncoding encoding) throws IOException {
    List<Float> values = floatValues();
    File file = writeFloatFile(values, encoding);
    assertThat(dataEncodings(file)).isEqualTo(encoding.dataEncodings);

    List<Record> floats = read(file, SCHEMA);
    List<Record> doubles = read(file, PROMOTED_SCHEMA);
    assertThat(floats).hasSize(values.size());
    assertThat(doubles).hasSize(values.size());
    for (int i = 0; i < values.size(); i += 1) {
      Float expected = values.get(i);
      if (expected == null) {
        assertThat(floats.get(i).getField("f")).as("value at %d", i).isNull();
        assertThat(doubles.get(i).getField("f")).as("value at %d", i).isNull();
      } else {
        // Parquet writers store NaN in its canonical form
        assertThat(Float.floatToIntBits((Float) floats.get(i).getField("f")))
            .as("value at %d", i)
            .isEqualTo(Float.floatToIntBits(expected));
        assertThat(Double.doubleToLongBits((Double) doubles.get(i).getField("f")))
            .as("value at %d", i)
            .isEqualTo(Double.doubleToLongBits(expected));
      }
    }
  }

  @ParameterizedTest
  @EnumSource(FloatEncoding.class)
  public void testBulkReads(FloatEncoding encoding) throws IOException {
    List<Float> values = floatValues();
    File file = writeFloatFile(values, encoding);

    // vary the limit and the accessor, including one value at a time, and read at an offset
    int[] maxCounts = {1, 3, 8, 100, 1_000};
    float[] floats = new float[1_001];
    int index = 0;
    int reads = 0;
    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(Files.localInput(file)))) {
      ColumnDescriptor desc =
          reader.getFileMetaData().getSchema().getColumnDescription(new String[] {"f"});
      PageReadStore rowGroup;
      while ((rowGroup = reader.readNextRowGroup()) != null) {
        ColumnIterator<?> column = ColumnIterator.newIterator(desc, "");
        column.setPageSource(rowGroup.getPageReader(desc));
        while (column.hasNext()) {
          if (column.currentDefinitionLevel() < desc.getMaxDefinitionLevel()) {
            assertThat(values.get(index)).as("value at %d", index).isNull();
            column.nextNull();
            index += 1;
            continue;
          }

          int maxCount = maxCounts[reads % maxCounts.length];
          boolean single = reads % 3 == 2;
          reads += 1;
          int count;
          if (single) {
            floats[1] = column.nextFloat();
            count = 1;
          } else {
            count = column.nextFloats(floats, 1, maxCount);
            assertThat(count).as("count at %d", index).isBetween(1, maxCount);
          }

          for (int i = 0; i < count; i += 1) {
            Float expected = values.get(index + i);
            assertThat(expected).as("value at %d", index + i).isNotNull();
            assertThat(Float.floatToIntBits(floats[1 + i]))
                .as("value at %d", index + i)
                .isEqualTo(Float.floatToIntBits(expected));
          }

          index += count;
          if (!single && count < maxCount) {
            // a bulk read only stops early before a null value or at the end of the row group
            assertThat(!column.hasNext() || values.get(index) == null)
                .as("read stopped early at %d", index)
                .isTrue();
          }
        }
      }
    }

    assertThat(index).isEqualTo(values.size());
  }

  /**
   * Returns low-cardinality values with repeats, which dictionary-encode as RLE runs, followed by
   * special and random values, which dictionary-encode as bit-packed runs, with nulls interleaved
   * and a long run without nulls, so that bulk reads span pages.
   */
  private static List<Float> floatValues() {
    List<Float> values = Lists.newArrayList();
    for (int i = 0; i < 3_000; i += 1) {
      values.add(i % 97 == 13 ? null : (float) ((i / 25) % 7) / 4);
    }

    Random random = new Random(42);
    for (int i = 0; i < 5_000; i += 1) {
      if (i % 11 == 5) {
        values.add(null);
      } else if (i % 13 == 0) {
        values.add(SPECIAL_VALUES[(i / 13) % SPECIAL_VALUES.length]);
      } else {
        values.add(Float.intBitsToFloat(random.nextInt()));
      }
    }

    for (int i = 0; i < 2_000; i += 1) {
      values.add(random.nextFloat() - 0.5f);
    }

    return values;
  }

  private File writeFloatFile(List<Float> values, FloatEncoding encoding) throws IOException {
    File file = new File(temp.toFile(), "float-" + System.nanoTime() + ".parquet");
    ExampleParquetWriter.Builder builder =
        ExampleParquetWriter.builder(ParquetIO.file(Files.localOutput(file)))
            .withType(FILE_SCHEMA)
            .withWriterVersion(encoding.writerVersion)
            .withDictionaryEncoding(encoding.dictionary)
            .withByteStreamSplitEncoding("f", encoding.byteStreamSplit)
            // many pages, so that readers are re-initialized and pages can switch encodings
            .withPageRowCountLimit(500);
    if (encoding.smallDictionary) {
      builder.withDictionaryPageSize(1024);
    }

    SimpleGroupFactory groupFactory = new SimpleGroupFactory(FILE_SCHEMA);
    try (ParquetWriter<Group> writer = builder.build()) {
      for (int i = 0; i < values.size(); i += 1) {
        Group group = groupFactory.newGroup().append("id", i);
        Float value = values.get(i);
        if (value != null) {
          group.append("f", value);
        }

        writer.write(group);
      }
    }

    return file;
  }

  private static Set<Encoding> dataEncodings(File file) throws IOException {
    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(Files.localInput(file)))) {
      Set<Encoding> encodings = Sets.newHashSet();
      for (BlockMetaData rowGroup : reader.getRowGroups()) {
        encodings.addAll(rowGroup.getColumns().get(1).getEncodingStats().getDataEncodings());
      }

      return encodings;
    }
  }

  private static List<Record> read(File file, Schema schema) throws IOException {
    try (CloseableIterable<Record> reader =
        Parquet.read(Files.localInput(file))
            .project(schema)
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
            .build()) {
      return Lists.newArrayList(reader);
    }
  }
}

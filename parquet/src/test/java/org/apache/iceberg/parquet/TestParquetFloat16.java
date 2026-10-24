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

import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.Files;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Conversions;
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
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

public class TestParquetFloat16 {
  private static final MessageType FILE_SCHEMA =
      org.apache.parquet.schema.Types.buildMessage()
          .required(PrimitiveTypeName.INT32)
          .id(1)
          .named("id")
          .optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
          .length(LogicalTypeAnnotation.Float16LogicalTypeAnnotation.BYTES)
          .as(LogicalTypeAnnotation.float16Type())
          .id(2)
          .named("f16")
          .named("table");

  // a 3-level list with optional elements: definition levels 0 (null list), 1 (empty list),
  // 2 (null element), and 3 (value)
  private static final MessageType LIST_FILE_SCHEMA =
      org.apache.parquet.schema.Types.buildMessage()
          .optionalGroup()
          .as(LogicalTypeAnnotation.listType())
          .repeatedGroup()
          .optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
          .length(LogicalTypeAnnotation.Float16LogicalTypeAnnotation.BYTES)
          .as(LogicalTypeAnnotation.float16Type())
          .named("element")
          .named("list")
          .named("f16_list")
          .named("table");

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), optional(2, "f16", Types.FloatType.get()));

  private static final Schema PROMOTED_SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), optional(2, "f16", Types.DoubleType.get()));

  // half-precision bit patterns of -2.0, 0.5, 1.0, 1.5, 65504.0 (max float16), and null
  private static final List<Short> FLOAT16_VALUES =
      Arrays.asList(
          (short) 0xC000, (short) 0x3800, (short) 0x3C00, (short) 0x3E00, (short) 0x7BFF, null);

  private static final List<Float> FLOAT_VALUES =
      Arrays.asList(-2.0f, 0.5f, 1.0f, 1.5f, 65504.0f, null);

  @TempDir private Path temp;

  @Test
  public void testFloat16ToFloatSpecialValues() {
    assertFloat16((short) 0x0000, 0.0f);
    assertFloat16((short) 0x8000, -0.0f);
    assertFloat16((short) 0x3C00, 1.0f);
    assertFloat16((short) 0xC000, -2.0f);
    assertFloat16((short) 0x3555, 0.333251953125f);
    assertFloat16((short) 0x7BFF, 65504.0f);
    assertFloat16((short) 0xFBFF, -65504.0f);
    assertFloat16((short) 0x0400, 0x1.0p-14f); // smallest normal
    assertFloat16((short) 0x03FF, 1023 * 0x1.0p-24f); // largest subnormal
    assertFloat16((short) 0x0001, 0x1.0p-24f); // smallest subnormal
    assertFloat16((short) 0x8001, -0x1.0p-24f);
    assertFloat16((short) 0x7C00, Float.POSITIVE_INFINITY);
    assertFloat16((short) 0xFC00, Float.NEGATIVE_INFINITY);

    assertThat(ParquetConversions.float16ToFloat((short) 0x7E00)).isNaN();
    assertThat(ParquetConversions.float16ToFloat((short) 0x7C01)).isNaN();
    assertThat(ParquetConversions.float16ToFloat((short) 0xFE00)).isNaN();
  }

  @Test
  public void testFloat16ToFloatAllValues() {
    for (int bits = 0; bits <= 0xFFFF; bits += 1) {
      float expected = referenceFloat16ToFloat(bits);
      float actual = ParquetConversions.float16ToFloat((short) bits);
      if (Float.isNaN(expected)) {
        assertThat(actual).as("float16 bits 0x%04X", bits).isNaN();
      } else {
        assertThat(Float.floatToRawIntBits(actual))
            .as("float16 bits 0x%04X", bits)
            .isEqualTo(Float.floatToRawIntBits(expected));
      }
    }
  }

  @Test
  public void testFloat16FromBinary() {
    assertThat(ParquetConversions.float16ToFloat(float16Binary((short) 0x3E00))).isEqualTo(1.5f);
    assertThat(ParquetConversions.float16ToFloat(float16Binary((short) 0xC000))).isEqualTo(-2.0f);
  }

  @Test
  public void testFloatToFloat16RoundTrip() {
    for (int bits = 0; bits <= 0xFFFF; bits += 1) {
      float value = ParquetConversions.float16ToFloat((short) bits);
      short actual = ParquetConversions.floatToFloat16(value);
      if (Float.isNaN(value)) {
        assertThat(ParquetConversions.float16ToFloat(actual))
            .as("float16 bits 0x%04X", bits)
            .isNaN();
      } else {
        assertThat(actual).as("float16 bits 0x%04X", bits).isEqualTo((short) bits);
      }
    }
  }

  @Test
  public void testFloatToFloat16Rounding() {
    // every pair of adjacent finite float16 values, including subnormals, and the largest finite
    // value with infinity, which is where rounding overflows
    for (int bits = 0; bits < 0x7C00; bits += 1) {
      for (int sign : new int[] {0, 0x8000}) {
        short lowerBits = (short) (sign | bits);
        short upperBits = (short) (sign | (bits + 1));
        float lower = ParquetConversions.float16ToFloat(lowerBits);
        float upper =
            bits + 1 < 0x7C00
                ? ParquetConversions.float16ToFloat(upperBits)
                // the next value if the exponent range were not limited
                : Math.copySign(0x1.0p16f, lower);
        // the midpoint is exact, because float has more than one extra bit of precision
        float midpoint = (lower + upper) / 2;
        short even = (bits & 1) == 0 ? lowerBits : upperBits;

        assertThat(ParquetConversions.floatToFloat16(midpoint))
            .as("midpoint above 0x%04X", lowerBits)
            .isEqualTo(even);
        assertThat(ParquetConversions.floatToFloat16(Math.nextAfter(midpoint, 0.0)))
            .as("below midpoint above 0x%04X", lowerBits)
            .isEqualTo(lowerBits);
        assertThat(ParquetConversions.floatToFloat16(Math.nextAfter(midpoint, upper)))
            .as("above midpoint above 0x%04X", lowerBits)
            .isEqualTo(upperBits);
      }
    }

    assertThat(ParquetConversions.floatToFloat16(Float.MAX_VALUE)).isEqualTo((short) 0x7C00);
    assertThat(ParquetConversions.floatToFloat16(-Float.MAX_VALUE)).isEqualTo((short) 0xFC00);
    assertThat(ParquetConversions.floatToFloat16(Float.MIN_VALUE)).isEqualTo((short) 0x0000);
    assertThat(ParquetConversions.floatToFloat16(-Float.MIN_VALUE)).isEqualTo((short) 0x8000);
    assertThat(ParquetConversions.floatToFloat16(Float.POSITIVE_INFINITY))
        .isEqualTo((short) 0x7C00);
    assertThat(ParquetConversions.floatToFloat16(Float.NEGATIVE_INFINITY))
        .isEqualTo((short) 0xFC00);
    assertThat(ParquetConversions.float16ToFloat(ParquetConversions.floatToFloat16(Float.NaN)))
        .isNaN();
    // a NaN with only low significand bits must not become infinity
    assertThat(
            ParquetConversions.float16ToFloat(
                ParquetConversions.floatToFloat16(Float.intBitsToFloat(0x7F800001))))
        .isNaN();
  }

  @Test
  public void testSchemaConversion() {
    assertThat(ParquetSchemaUtil.convert(FILE_SCHEMA).asStruct()).isEqualTo(SCHEMA.asStruct());
  }

  @Test
  public void testReadFloat16() throws IOException {
    File file = writeFloat16File(FLOAT16_VALUES, WriterVersion.PARQUET_1_0);

    List<Record> records = read(file, SCHEMA);

    assertThat(records).hasSize(FLOAT_VALUES.size());
    for (int i = 0; i < records.size(); i += 1) {
      assertThat(records.get(i).getField("id")).isEqualTo(i);
      assertThat(records.get(i).getField("f16")).isEqualTo(FLOAT_VALUES.get(i));
    }
  }

  @Test
  public void testReadFloat16SpecialValues() throws IOException {
    List<Short> values =
        Arrays.asList(
            (short) 0x8000, (short) 0x0001, (short) 0x7C00, (short) 0xFC00, (short) 0x7E00);
    File file = writeFloat16File(values, WriterVersion.PARQUET_2_0);

    List<Record> records = read(file, SCHEMA);

    assertThat(records).hasSize(values.size());
    assertThat(Float.floatToRawIntBits((Float) records.get(0).getField("f16")))
        .isEqualTo(Float.floatToRawIntBits(-0.0f));
    assertThat(records.get(1).getField("f16")).isEqualTo(0x1.0p-24f);
    assertThat(records.get(2).getField("f16")).isEqualTo(Float.POSITIVE_INFINITY);
    assertThat(records.get(3).getField("f16")).isEqualTo(Float.NEGATIVE_INFINITY);
    assertThat((Float) records.get(4).getField("f16")).isNaN();
  }

  @Test
  public void testReadFloat16AsDouble() throws IOException {
    File file = writeFloat16File(FLOAT16_VALUES, WriterVersion.PARQUET_1_0);

    List<Record> records = read(file, PROMOTED_SCHEMA);

    assertThat(records).hasSize(FLOAT_VALUES.size());
    for (int i = 0; i < records.size(); i += 1) {
      Float expected = FLOAT_VALUES.get(i);
      assertThat(records.get(i).getField("f16"))
          .isEqualTo(expected != null ? expected.doubleValue() : null);
    }
  }

  @Test
  public void testReadFloat16AsFixed() throws IOException {
    // tables imported before float16 support have the column typed as fixed[2]
    Schema fixedSchema =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            optional(2, "f16", Types.FixedType.ofLength(2)));
    File file = writeFloat16File(FLOAT16_VALUES, WriterVersion.PARQUET_1_0);

    List<Record> records = read(file, fixedSchema);

    assertThat(records).hasSize(FLOAT16_VALUES.size());
    for (int i = 0; i < records.size(); i += 1) {
      Short bits = FLOAT16_VALUES.get(i);
      assertThat(records.get(i).getField("f16"))
          .isEqualTo(bits != null ? float16Binary(bits).getBytes() : null);
    }
  }

  @Test
  public void testReadFloat16WithFilter() throws IOException {
    File file = writeFloat16File(FLOAT16_VALUES, WriterVersion.PARQUET_1_0);

    assertThat(read(file, SCHEMA, greaterThan("f16", 65504.0f))).isEmpty();
    assertThat(read(file, SCHEMA, lessThan("f16", -2.0f))).isEmpty();
    assertThat(read(file, SCHEMA, equal("f16", 1.5f))).hasSize(FLOAT_VALUES.size());

    assertThat(read(file, PROMOTED_SCHEMA, greaterThan("f16", 70000.0d))).isEmpty();
    assertThat(read(file, PROMOTED_SCHEMA, lessThanOrEqual("f16", -2.0d)))
        .hasSize(FLOAT_VALUES.size());
  }

  @Test
  public void testMetricsRowGroupFilter() throws IOException {
    File file = writeFloat16File(FLOAT16_VALUES, WriterVersion.PARQUET_1_0);

    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(Files.localInput(file)))) {
      assertThat(reader.getRowGroups()).hasSize(1);
      BlockMetaData rowGroup = reader.getRowGroups().get(0);
      MessageType fileSchema = reader.getFileMetaData().getSchema();

      assertThat(shouldReadStats(fileSchema, rowGroup, SCHEMA, lessThan("f16", -2.0f))).isFalse();
      assertThat(shouldReadStats(fileSchema, rowGroup, SCHEMA, lessThanOrEqual("f16", -2.0f)))
          .isTrue();
      assertThat(shouldReadStats(fileSchema, rowGroup, SCHEMA, greaterThan("f16", 65504.0f)))
          .isFalse();
      assertThat(shouldReadStats(fileSchema, rowGroup, SCHEMA, greaterThanOrEqual("f16", 65504.0f)))
          .isTrue();
      assertThat(shouldReadStats(fileSchema, rowGroup, SCHEMA, equal("f16", 100000.0f))).isFalse();
      assertThat(shouldReadStats(fileSchema, rowGroup, SCHEMA, equal("f16", 1.0f))).isTrue();

      assertThat(
              shouldReadStats(fileSchema, rowGroup, PROMOTED_SCHEMA, greaterThan("f16", 65504.0d)))
          .isFalse();
      assertThat(shouldReadStats(fileSchema, rowGroup, PROMOTED_SCHEMA, lessThan("f16", 0.0d)))
          .isTrue();
    }
  }

  @Test
  public void testDictionaryRowGroupFilter() throws IOException {
    // FIXED_LEN_BYTE_ARRAY columns are only dictionary-encoded by the v2 writer
    File file = writeFloat16File(FLOAT16_VALUES, WriterVersion.PARQUET_2_0);

    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(Files.localInput(file)))) {
      assertThat(reader.getRowGroups()).hasSize(1);
      BlockMetaData rowGroup = reader.getRowGroups().get(0);
      MessageType fileSchema = reader.getFileMetaData().getSchema();

      // 3.0 is within the [min, max] range, so only the dictionary can prove it is absent
      assertThat(shouldReadDictionary(reader, fileSchema, rowGroup, SCHEMA, equal("f16", 3.0f)))
          .isFalse();
      assertThat(shouldReadDictionary(reader, fileSchema, rowGroup, SCHEMA, equal("f16", 1.5f)))
          .isTrue();
      assertThat(
              shouldReadDictionary(
                  reader, fileSchema, rowGroup, PROMOTED_SCHEMA, equal("f16", 3.0d)))
          .isFalse();
      assertThat(
              shouldReadDictionary(
                  reader, fileSchema, rowGroup, PROMOTED_SCHEMA, equal("f16", 0.5d)))
          .isTrue();
    }
  }

  @Test
  public void testFooterMetrics() throws IOException {
    File file = writeFloat16File(FLOAT16_VALUES, WriterVersion.PARQUET_1_0);

    Metrics metrics = ParquetUtil.fileMetrics(Files.localInput(file), MetricsConfig.getDefault());

    assertThat(metrics.valueCounts()).containsEntry(2, (long) FLOAT_VALUES.size());
    assertThat(metrics.nullValueCounts()).containsEntry(2, 1L);
    assertThat(
            (Float) Conversions.fromByteBuffer(Types.FloatType.get(), metrics.lowerBounds().get(2)))
        .isEqualTo(-2.0f);
    assertThat(
            (Float) Conversions.fromByteBuffer(Types.FloatType.get(), metrics.upperBounds().get(2)))
        .isEqualTo(65504.0f);
  }

  @Test
  public void testFooterMetricsWithNaN() throws IOException {
    List<Short> values = Arrays.asList((short) 0x3C00, (short) 0x7E00, (short) 0x4000);
    File file = writeFloat16File(values, WriterVersion.PARQUET_1_0);

    Metrics metrics = ParquetUtil.fileMetrics(Files.localInput(file), MetricsConfig.getDefault());

    // Parquet excludes NaN values from the float16 min/max
    assertThat(metrics.valueCounts()).containsEntry(2, (long) values.size());
    assertThat(
            (Float) Conversions.fromByteBuffer(Types.FloatType.get(), metrics.lowerBounds().get(2)))
        .isEqualTo(1.0f);
    assertThat(
            (Float) Conversions.fromByteBuffer(Types.FloatType.get(), metrics.upperBounds().get(2)))
        .isEqualTo(2.0f);
  }

  @ParameterizedTest
  @EnumSource(Float16Encoding.class)
  public void testReadEncodings(Float16Encoding encoding) throws IOException {
    List<Short> values = allFloat16Values();
    File file = writeFloat16File(values, encoding);
    assertThat(dataEncodings(file)).isEqualTo(encoding.dataEncodings());

    List<Record> floats = read(file, SCHEMA);
    List<Record> doubles = read(file, PROMOTED_SCHEMA);
    List<Record> fixeds =
        read(
            file,
            new Schema(
                required(1, "id", Types.IntegerType.get()),
                optional(2, "f16", Types.FixedType.ofLength(2))));

    assertThat(floats).hasSize(values.size());
    assertThat(doubles).hasSize(values.size());
    assertThat(fixeds).hasSize(values.size());
    for (int i = 0; i < values.size(); i += 1) {
      Short bits = values.get(i);
      if (bits == null) {
        assertThat(floats.get(i).getField("f16")).isNull();
        assertThat(doubles.get(i).getField("f16")).isNull();
        assertThat(fixeds.get(i).getField("f16")).isNull();
      } else {
        float expected = ParquetConversions.float16ToFloat(bits);
        assertThat(Float.floatToRawIntBits((Float) floats.get(i).getField("f16")))
            .as("float16 bits 0x%04X at %d", bits, i)
            .isEqualTo(Float.floatToRawIntBits(expected));
        assertThat(Double.doubleToRawLongBits((Double) doubles.get(i).getField("f16")))
            .as("float16 bits 0x%04X at %d", bits, i)
            .isEqualTo(Double.doubleToRawLongBits(expected));
        assertThat(fixeds.get(i).getField("f16"))
            .as("float16 bits 0x%04X at %d", bits, i)
            .isEqualTo(float16Binary(bits).getBytes());
      }
    }
  }

  @ParameterizedTest
  @EnumSource(Float16Encoding.class)
  public void testColumnIterator(Float16Encoding encoding) throws IOException {
    List<Short> values = allFloat16Values();
    File file = writeFloat16File(values, encoding);

    int index = 0;
    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(Files.localInput(file)))) {
      ColumnDescriptor desc =
          reader.getFileMetaData().getSchema().getColumnDescription(new String[] {"f16"});
      PageReadStore rowGroup;
      while ((rowGroup = reader.readNextRowGroup()) != null) {
        ColumnIterator<?> column = ColumnIterator.newIterator(desc, "");
        column.setPageSource(rowGroup.getPageReader(desc));
        while (column.hasNext()) {
          Short expected = values.get(index);
          if (column.currentDefinitionLevel() < desc.getMaxDefinitionLevel()) {
            assertThat(expected).as("value at %d", index).isNull();
            column.nextNull();
          } else {
            assertThat(expected).as("value at %d", index).isNotNull();
            // alternate between the accessors to switch between them within a page
            switch (index % 3) {
              case 0:
                assertThat(column.nextFloat16()).as("value at %d", index).isEqualTo(expected);
                break;
              case 1:
                assertThat(Float.floatToRawIntBits(column.nextFloat()))
                    .as("value at %d", index)
                    .isEqualTo(
                        Float.floatToRawIntBits(ParquetConversions.float16ToFloat(expected)));
                break;
              default:
                assertThat(column.nextBinary().get2BytesLittleEndian())
                    .as("value at %d", index)
                    .isEqualTo(expected);
            }
          }

          index += 1;
        }
      }
    }

    assertThat(index).isEqualTo(values.size());
  }

  @Test
  public void testNextFloat16FromNonFloat16Column() throws IOException {
    File file = writeFloat16File(FLOAT16_VALUES, WriterVersion.PARQUET_1_0);

    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(Files.localInput(file)))) {
      ColumnDescriptor desc =
          reader.getFileMetaData().getSchema().getColumnDescription(new String[] {"id"});
      ColumnIterator<?> column = ColumnIterator.newIterator(desc, "");
      column.setPageSource(reader.readNextRowGroup().getPageReader(desc));

      assertThatThrownBy(column::nextFloat16)
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageStartingWith("Not a float16 column");
      assertThatThrownBy(() -> column.nextFloat16s(new short[1], 0, 1))
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageStartingWith("Not a float16 column");
      assertThatThrownBy(() -> column.nextFloats(new float[1], 0, 1))
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageStartingWith("Not a float or float16 column");
    }
  }

  @ParameterizedTest
  @EnumSource(Float16Encoding.class)
  public void testBulkReads(Float16Encoding encoding) throws IOException {
    List<Short> values = allFloat16Values();
    // followed by a run without nulls that is longer than a page, so bulk reads span pages
    for (int i = 0; i < 2_000; i += 1) {
      values.add((short) (i * 37));
    }

    File file = writeFloat16File(values, encoding);

    int[] maxCounts = {1, 3, 7, 1_000};
    short[] float16s = new short[1_001];
    float[] floats = new float[1_001];
    int index = 0;
    int reads = 0;
    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(Files.localInput(file)))) {
      ColumnDescriptor desc =
          reader.getFileMetaData().getSchema().getColumnDescription(new String[] {"f16"});
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

          // vary the limit and the accessor, and read at an offset into the array
          int maxCount = maxCounts[reads % maxCounts.length];
          boolean asFloats = reads % 2 == 1;
          int count =
              asFloats
                  ? column.nextFloats(floats, 1, maxCount)
                  : column.nextFloat16s(float16s, 1, maxCount);
          reads += 1;

          assertThat(count).as("count at %d", index).isBetween(1, maxCount);
          for (int i = 0; i < count; i += 1) {
            Short expected = values.get(index + i);
            assertThat(expected).as("value at %d", index + i).isNotNull();
            if (asFloats) {
              assertThat(Float.floatToRawIntBits(floats[1 + i]))
                  .as("value at %d", index + i)
                  .isEqualTo(Float.floatToRawIntBits(ParquetConversions.float16ToFloat(expected)));
            } else {
              assertThat(float16s[1 + i]).as("value at %d", index + i).isEqualTo(expected);
            }
          }

          index += count;
          if (count < maxCount) {
            // a read only stops early before a null value or at the end of the row group
            assertThat(!column.hasNext() || values.get(index) == null)
                .as("read stopped early at %d", index)
                .isTrue();
          }
        }
      }
    }

    assertThat(index).isEqualTo(values.size());
  }

  @ParameterizedTest
  @EnumSource(Float16Encoding.class)
  public void testBulkReadsOfLists(Float16Encoding encoding) throws IOException {
    List<List<Short>> lists = Lists.newArrayList();
    for (int row = 0; row < 3_000; row += 1) {
      lists.add(float16List(row));
    }

    File file = writeFloat16ListFile(lists, encoding);

    int[] maxCounts = {1, 4, 2_000};
    short[] float16s = new short[2_000];
    float[] floats = new float[2_000];
    int row = 0;
    int reads = 0;
    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(Files.localInput(file)))) {
      ColumnDescriptor desc =
          reader
              .getFileMetaData()
              .getSchema()
              .getColumnDescription(new String[] {"f16_list", "list", "element"});
      int maxDefinitionLevel = desc.getMaxDefinitionLevel();
      PageReadStore rowGroup;
      while ((rowGroup = reader.readNextRowGroup()) != null) {
        ColumnIterator<?> column = ColumnIterator.newIterator(desc, "");
        column.setPageSource(rowGroup.getPageReader(desc));
        while (column.hasNext()) {
          List<Short> actual;
          if (column.currentDefinitionLevel() < maxDefinitionLevel - 1) {
            // a null list (0) or an empty list (1)
            actual = column.currentDefinitionLevel() == 0 ? null : Lists.newArrayList();
            column.nextNull();
          } else {
            actual = Lists.newArrayList();
            do {
              if (column.currentDefinitionLevel() < maxDefinitionLevel) {
                column.nextNull();
                actual.add(null);
              } else {
                // a bulk read stops at a null element, at the end of the list, or at maxCount
                int maxCount = maxCounts[reads % maxCounts.length];
                boolean asFloats = reads % 2 == 1;
                int count =
                    asFloats
                        ? column.nextFloats(floats, 0, maxCount)
                        : column.nextFloat16s(float16s, 0, maxCount);
                reads += 1;

                assertThat(count).as("count in row %d", row).isBetween(1, maxCount);
                for (int i = 0; i < count; i += 1) {
                  actual.add(asFloats ? ParquetConversions.floatToFloat16(floats[i]) : float16s[i]);
                }
              }
            } while (column.currentRepetitionLevel() == desc.getMaxRepetitionLevel());
          }

          assertThat(actual).as("row %d", row).isEqualTo(lists.get(row));
          row += 1;
        }
      }
    }

    assertThat(row).isEqualTo(lists.size());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testBulkReadsOfFloatColumn(boolean dictionary) throws IOException {
    MessageType floatSchema =
        org.apache.parquet.schema.Types.buildMessage()
            .optional(PrimitiveTypeName.FLOAT)
            .id(1)
            .named("f")
            .named("table");
    List<Float> values = Lists.newArrayList();
    for (int i = 0; i < 5_000; i += 1) {
      values.add(i % 11 == 5 ? null : (float) (i % 100) / 4);
    }

    File file = newFile();
    SimpleGroupFactory groupFactory = new SimpleGroupFactory(floatSchema);
    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(ParquetIO.file(Files.localOutput(file)))
            .withType(floatSchema)
            .withDictionaryEncoding(dictionary)
            .withPageRowCountLimit(500)
            .build()) {
      for (Float value : values) {
        Group group = groupFactory.newGroup();
        if (value != null) {
          group.append("f", value);
        }

        writer.write(group);
      }
    }

    float[] floats = new float[8];
    int index = 0;
    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(Files.localInput(file)))) {
      ColumnDescriptor desc =
          reader.getFileMetaData().getSchema().getColumnDescription(new String[] {"f"});
      ColumnIterator<?> column = ColumnIterator.newIterator(desc, "");
      column.setPageSource(reader.readNextRowGroup().getPageReader(desc));
      assertThatThrownBy(() -> column.nextFloat16s(new short[1], 0, 1))
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageStartingWith("Not a float16 column");

      while (column.hasNext()) {
        if (column.currentDefinitionLevel() < desc.getMaxDefinitionLevel()) {
          assertThat(values.get(index)).as("value at %d", index).isNull();
          column.nextNull();
          index += 1;
        } else {
          int count = column.nextFloats(floats, 0, floats.length);
          assertThat(count).as("count at %d", index).isBetween(1, floats.length);
          for (int i = 0; i < count; i += 1) {
            assertThat(floats[i]).as("value at %d", index + i).isEqualTo(values.get(index + i));
          }

          index += count;
        }
      }
    }

    assertThat(index).isEqualTo(values.size());
  }

  @ParameterizedTest
  @EnumSource(Float16Encoding.class)
  public void testFixedSizeLists(Float16Encoding encoding) throws IOException {
    int size = 37;
    List<List<Short>> lists = Lists.newArrayList();
    for (int row = 0; row < 1_000; row += 1) {
      List<Short> list = Lists.newArrayListWithCapacity(size);
      for (int i = 0; i < size; i += 1) {
        // finite values, so that they convert to float and back exactly
        list.add((short) ((row * 31 + i * 7) & 0xBBFF));
      }

      lists.add(list);
    }

    File file = writeFloat16ListFile(lists, encoding);

    short[] float16s = new short[100];
    float[] floats = new float[100];
    int row = 0;
    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(Files.localInput(file)))) {
      ColumnDescriptor desc =
          reader
              .getFileMetaData()
              .getSchema()
              .getColumnDescription(new String[] {"f16_list", "list", "element"});
      int maxDefinitionLevel = desc.getMaxDefinitionLevel();
      int maxRepetitionLevel = desc.getMaxRepetitionLevel();
      PageReadStore rowGroup;
      while ((rowGroup = reader.readNextRowGroup()) != null) {
        ColumnIterator<?> column = ColumnIterator.newIterator(desc, "");
        column.setFixedListSize(size);
        column.setPageSource(rowGroup.getPageReader(desc));
        while (column.hasNext()) {
          // the derived levels start a new list
          assertThat(column.currentRepetitionLevel()).as("row %d", row).isZero();
          List<Short> actual = Lists.newArrayList();
          do {
            assertThat(column.currentDefinitionLevel())
                .as("row %d", row)
                .isEqualTo(maxDefinitionLevel);
            // vary how lists are read: whole, in bulk pieces, or one value at a time
            switch (row % 4) {
              case 0 -> {
                // a bulk read stops at the end of the list
                int count = column.nextFloat16s(float16s, 0, float16s.length);
                assertThat(count).as("row %d", row).isEqualTo(size);
                for (int i = 0; i < count; i += 1) {
                  actual.add(float16s[i]);
                }
              }
              case 1 -> {
                int count = column.nextFloats(floats, 0, 10);
                assertThat(count).as("row %d", row).isBetween(1, 10);
                for (int i = 0; i < count; i += 1) {
                  actual.add(ParquetConversions.floatToFloat16(floats[i]));
                }
              }
              case 2 -> actual.add(column.nextFloat16());
              default -> {
                assertThat(column.nextFloat16s(float16s, 0, 1)).as("row %d", row).isOne();
                actual.add(float16s[0]);
              }
            }
          } while (column.currentRepetitionLevel() == maxRepetitionLevel);

          assertThat(actual).as("row %d", row).isEqualTo(lists.get(row));
          row += 1;
        }
      }
    }

    assertThat(row).isEqualTo(lists.size());
  }

  @Test
  public void testFixedSizeListsRejectPagesWithOtherSizes() throws IOException {
    List<List<Short>> lists = Lists.newArrayList();
    for (int row = 0; row < 10; row += 1) {
      // one list is longer, so the page does not hold whole lists of 5 elements
      lists.add(Lists.newArrayList(Collections.nCopies(row == 3 ? 6 : 5, (short) 0x3C00)));
    }

    File file = writeFloat16ListFile(lists, Float16Encoding.PLAIN);

    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(Files.localInput(file)))) {
      ColumnDescriptor desc =
          reader
              .getFileMetaData()
              .getSchema()
              .getColumnDescription(new String[] {"f16_list", "list", "element"});
      ColumnIterator<?> column = ColumnIterator.newIterator(desc, "");
      column.setFixedListSize(5);
      PageReadStore rowGroup = reader.readNextRowGroup();
      assertThatThrownBy(() -> column.setPageSource(rowGroup.getPageReader(desc)))
          .isInstanceOf(ParquetDecodingException.class)
          .hasMessageStartingWith("Cannot read lists of 5 elements")
          .hasMessageEndingWith("page has 51 values");
    }
  }

  @Test
  public void testInvalidFixedListSize() {
    ColumnIterator<?> flatColumn =
        ColumnIterator.newIterator(FILE_SCHEMA.getColumnDescription(new String[] {"f16"}), "");
    assertThatThrownBy(() -> flatColumn.setFixedListSize(4))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot read fixed-size lists from column")
        .hasMessageEndingWith("with max repetition level 0");

    ColumnIterator<?> listColumn =
        ColumnIterator.newIterator(
            LIST_FILE_SCHEMA.getColumnDescription(new String[] {"f16_list", "list", "element"}),
            "");
    assertThatThrownBy(() -> listColumn.setFixedListSize(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid fixed list size: -1");
  }

  /** Ways the float16 column is encoded by parquet-java writers. */
  private enum Float16Encoding {
    // the v1 writer never dictionary-encodes FIXED_LEN_BYTE_ARRAY columns
    PLAIN(WriterVersion.PARQUET_1_0, true, false, Encoding.PLAIN),
    BYTE_STREAM_SPLIT(WriterVersion.PARQUET_1_0, false, false, Encoding.BYTE_STREAM_SPLIT),
    RLE_DICTIONARY(WriterVersion.PARQUET_2_0, true, false, Encoding.RLE_DICTIONARY),
    DELTA_BYTE_ARRAY(WriterVersion.PARQUET_2_0, false, false, Encoding.DELTA_BYTE_ARRAY),
    // the dictionary overflows after the first pages, so pages switch encoding in a row group
    DICTIONARY_FALLBACK(
        WriterVersion.PARQUET_2_0, true, true, Encoding.RLE_DICTIONARY, Encoding.DELTA_BYTE_ARRAY);

    private final WriterVersion writerVersion;
    private final boolean dictionary;
    private final boolean smallDictionary;
    private final Set<Encoding> dataEncodings;

    Float16Encoding(
        WriterVersion writerVersion,
        boolean dictionary,
        boolean smallDictionary,
        Encoding... dataEncodings) {
      this.writerVersion = writerVersion;
      this.dictionary = dictionary;
      this.smallDictionary = smallDictionary;
      this.dataEncodings = ImmutableSet.copyOf(dataEncodings);
    }

    Set<Encoding> dataEncodings() {
      return dataEncodings;
    }
  }

  /**
   * Returns a few pages of low-cardinality values followed by every float16 bit pattern, with nulls
   * interleaved.
   */
  private static List<Short> allFloat16Values() {
    List<Short> values = Lists.newArrayList();
    for (int i = 0; i < 3_000; i += 1) {
      values.add(i % 7 == 3 ? null : FLOAT16_VALUES.get(i % 5));
    }

    for (int bits = 0; bits <= 0xFFFF; bits += 1) {
      values.add(bits % 7 == 3 ? null : (short) bits);
    }

    return values;
  }

  /**
   * Returns the list of a row: null, empty, with null elements, long, or short. Values are finite,
   * so that they convert to float and back exactly.
   */
  private static List<Short> float16List(int row) {
    int length;
    boolean nullElements = false;
    switch (row % 6) {
      case 0:
        return null;
      case 1:
        return Lists.newArrayList();
      case 2:
        length = row % 50 + 1;
        nullElements = true;
        break;
      case 3:
        length = 1_200 + row % 7;
        break;
      default:
        length = row % 17 + 1;
    }

    List<Short> list = Lists.newArrayListWithCapacity(length);
    for (int i = 0; i < length; i += 1) {
      // clears two exponent bits, so the exponent is at most 14 and values are not NaN or infinite
      boolean isNull = nullElements && i % 9 == 4;
      list.add(isNull ? null : (short) ((row * 31 + i * 7) & 0xBBFF));
    }

    return list;
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

  private static void assertFloat16(short bits, float expected) {
    assertThat(Float.floatToRawIntBits(ParquetConversions.float16ToFloat(bits)))
        .as("float16 bits 0x%04X", bits)
        .isEqualTo(Float.floatToRawIntBits(expected));
  }

  /** A direct implementation of the IEEE 754 binary16 definition, used as a test oracle. */
  private static float referenceFloat16ToFloat(int bits) {
    int sign = (bits >>> 15) & 0x1;
    int exponent = (bits >>> 10) & 0x1F;
    int significand = bits & 0x3FF;

    float magnitude;
    if (exponent == 0x1F) {
      magnitude = significand == 0 ? Float.POSITIVE_INFINITY : Float.NaN;
    } else if (exponent == 0) {
      // subnormal: significand * 2^-24
      magnitude = Math.scalb((float) significand, -24);
    } else {
      // normal: (1 + significand / 2^10) * 2^(exponent - 15)
      magnitude = Math.scalb((float) (0x400 | significand), exponent - 25);
    }

    return sign == 0 ? magnitude : -magnitude;
  }

  private static Binary float16Binary(short bits) {
    return Binary.fromConstantByteArray(new byte[] {(byte) bits, (byte) (bits >>> 8)});
  }

  private File writeFloat16File(List<Short> values, WriterVersion writerVersion)
      throws IOException {
    File file = newFile();
    writeFloat16File(
        values,
        ExampleParquetWriter.builder(ParquetIO.file(Files.localOutput(file)))
            .withWriterVersion(writerVersion)
            .withDictionaryEncoding(true));
    return file;
  }

  private File writeFloat16File(List<Short> values, Float16Encoding encoding) throws IOException {
    File file = newFile();
    ExampleParquetWriter.Builder builder =
        ExampleParquetWriter.builder(ParquetIO.file(Files.localOutput(file)))
            .withWriterVersion(encoding.writerVersion)
            .withDictionaryEncoding(encoding.dictionary)
            .withByteStreamSplitEncoding("f16", encoding == Float16Encoding.BYTE_STREAM_SPLIT)
            // many pages, so that readers are re-initialized and pages can switch encodings
            .withPageRowCountLimit(500);
    if (encoding.smallDictionary) {
      builder.withDictionaryPageSize(1024);
    }

    writeFloat16File(values, builder);
    return file;
  }

  private File writeFloat16ListFile(List<List<Short>> lists, Float16Encoding encoding)
      throws IOException {
    File file = newFile();
    ExampleParquetWriter.Builder builder =
        ExampleParquetWriter.builder(ParquetIO.file(Files.localOutput(file)))
            .withType(LIST_FILE_SCHEMA)
            .withWriterVersion(encoding.writerVersion)
            .withDictionaryEncoding(encoding.dictionary)
            .withByteStreamSplitEncoding(
                "f16_list.list.element", encoding == Float16Encoding.BYTE_STREAM_SPLIT)
            .withPageRowCountLimit(100);
    if (encoding.smallDictionary) {
      builder.withDictionaryPageSize(1024);
    }

    SimpleGroupFactory groupFactory = new SimpleGroupFactory(LIST_FILE_SCHEMA);
    try (ParquetWriter<Group> writer = builder.build()) {
      for (List<Short> list : lists) {
        Group group = groupFactory.newGroup();
        if (list != null) {
          Group listGroup = group.addGroup("f16_list");
          for (Short value : list) {
            Group element = listGroup.addGroup("list");
            if (value != null) {
              element.append("element", float16Binary(value));
            }
          }
        }

        writer.write(group);
      }
    }

    return file;
  }

  private File newFile() {
    return new File(temp.toFile(), "float16-" + System.nanoTime() + ".parquet");
  }

  private static void writeFloat16File(List<Short> values, ExampleParquetWriter.Builder builder)
      throws IOException {
    SimpleGroupFactory groupFactory = new SimpleGroupFactory(FILE_SCHEMA);
    try (ParquetWriter<Group> writer = builder.withType(FILE_SCHEMA).build()) {
      for (int i = 0; i < values.size(); i += 1) {
        Group group = groupFactory.newGroup().append("id", i);
        Short value = values.get(i);
        if (value != null) {
          group.append("f16", float16Binary(value));
        }

        writer.write(group);
      }
    }
  }

  private static List<Record> read(File file, Schema schema) throws IOException {
    return read(file, schema, null);
  }

  private static List<Record> read(File file, Schema schema, Expression filter) throws IOException {
    Parquet.ReadBuilder builder =
        Parquet.read(Files.localInput(file))
            .project(schema)
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema));
    if (filter != null) {
      builder.filter(filter);
    }

    try (CloseableIterable<Record> reader = builder.build()) {
      return Lists.newArrayList(reader);
    }
  }

  private static boolean shouldReadStats(
      MessageType fileSchema, BlockMetaData rowGroup, Schema schema, Expression filter) {
    return new ParquetMetricsRowGroupFilter(schema, filter).shouldRead(fileSchema, rowGroup);
  }

  private static boolean shouldReadDictionary(
      ParquetFileReader reader,
      MessageType fileSchema,
      BlockMetaData rowGroup,
      Schema schema,
      Expression filter) {
    return new ParquetDictionaryRowGroupFilter(schema, filter)
        .shouldRead(fileSchema, rowGroup, reader.getDictionaryReader(rowGroup));
  }
}

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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestMumblingBitmap {
  @TempDir private Path temp;

  @Test
  void simpleInsert() {
    MumblingBitmap bitmap = new MumblingBitmap();

    assertThat(bitmap.set(0)).isTrue();
    assertThat(bitmap.set(255)).isTrue();
    assertThat(bitmap.set(65_533)).isTrue();

    assertThat(bitmap.isSet(0)).isTrue();
    assertThat(bitmap.isSet(1)).isFalse();
    assertThat(bitmap.isSet(255)).isTrue();
    assertThat(bitmap.isSet(256)).isFalse();
    assertThat(bitmap.isSet(65_533)).isTrue();
    assertThat(bitmap.cardinality()).isEqualTo(3);
  }

  @Test
  void duplicateInsertDoesNotChangeCardinality() {
    MumblingBitmap bitmap = new MumblingBitmap();

    assertThat(bitmap.set(5)).isTrue();
    assertThat(bitmap.set(6)).isTrue();
    assertThat(bitmap.set(5)).isFalse();
    assertThat(bitmap.set(4)).isTrue();
    assertThat(bitmap.set(6)).isFalse();

    assertThat(bitmap.cardinality()).isEqualTo(3);
    assertThat(bitmap.isSet(4)).isTrue();
    assertThat(bitmap.isSet(5)).isTrue();
    assertThat(bitmap.isSet(6)).isTrue();
    assertThat(bitmap.isSet(7)).isFalse();
  }

  @Test
  void sparseContainerConvertsToDenseOnThirtySecondDistinctValue() {
    MumblingBitmap bitmap = new MumblingBitmap();

    for (int position = 0; position < 31; position += 1) {
      assertThat(bitmap.set(position)).isTrue();
      assertThat(bitmap.isSet(position)).isTrue();
    }

    assertThat(bitmap.isSet(32)).isFalse();
    assertThat(bitmap.cardinality()).isEqualTo(31);
    assertThat(bitmap.size()).isEqualTo(1563 + 31);

    assertThat(bitmap.set(32)).isTrue();

    assertThat(bitmap.isSet(32)).isTrue();
    assertThat(bitmap.cardinality()).isEqualTo(32);
    assertThat(bitmap.size()).isEqualTo(1563 + 32);
    for (int position = 0; position < 31; position += 1) {
      assertThat(bitmap.isSet(position)).isTrue();
    }
  }

  @Test
  void fullContainerCardinalityDistinguishes255From256Values() {
    MumblingBitmap bitmap = new MumblingBitmap();

    for (int position = 0; position < 255; position += 1) {
      assertThat(bitmap.set(position)).isTrue();
    }

    assertThat(bitmap.cardinality()).isEqualTo(255);
    assertThat(bitmap.set(255)).isTrue();
    assertThat(bitmap.cardinality()).isEqualTo(256);
    assertThat(bitmap.set(255)).isFalse();
    assertThat(bitmap.cardinality()).isEqualTo(256);
  }

  @Test
  void randomizedValuesMatchSet() {
    Random random = new Random(44L);
    MumblingBitmap bitmap = new MumblingBitmap();
    Set<Integer> positions = new HashSet<>();

    for (int index = 0; index < 50_000; index += 1) {
      int position = random.nextInt(400_000);
      assertThat(bitmap.set(position)).isEqualTo(positions.add(position));
    }

    assertThat(bitmap.cardinality()).isEqualTo(positions.size());
    for (int position : positions) {
      assertThat(bitmap.isSet(position)).isTrue();
    }
  }

  @Test
  void serializationRoundTrip() {
    Random random = new Random(728L);
    MumblingBitmap bitmap = new MumblingBitmap();
    Set<Integer> positions = new HashSet<>();

    for (int index = 0; index < 50_000; index += 1) {
      int position = random.nextInt(50_000);
      positions.add(position);
      bitmap.set(position);
    }

    byte[] serialized = bitmap.serialize().array();
    assertThat(serialized).hasSize(bitmap.size());

    MumblingBitmap copy = MumblingBitmap.deserialize(serialized);

    assertThat(copy).isEqualTo(bitmap);
    assertThat(copy.cardinality()).isEqualTo(positions.size());
    for (int position : positions) {
      assertThat(copy.isSet(position)).isTrue();
    }
  }

  @Test
  void serializesRandomTwentyPercentRowsFromRange() {
    int rowCount = 400_000;
    int selectedRows = rowCount / 20;
    Random random = new Random(31L);
    MumblingBitmap bitmap = new MumblingBitmap();
    Set<Integer> positions = new HashSet<>();

    while (positions.size() < selectedRows) {
      int position = random.nextInt(rowCount);
      if (positions.add(position)) {
        assertThat(bitmap.set(position)).isTrue();
      }
    }

    assertThat(bitmap.cardinality()).isEqualTo(selectedRows);

    byte[] serialized = bitmap.serialize().array();
    assertThat(serialized).hasSize(bitmap.size());

    MumblingBitmap copy = MumblingBitmap.deserialize(serialized);

    assertThat(copy).isEqualTo(bitmap);
    assertThat(copy.cardinality()).isEqualTo(selectedRows);
    for (int position = 0; position < rowCount; position += 1) {
      assertThat(copy.isSet(position)).isEqualTo(positions.contains(position));
    }
  }

  @Test
  void storesSelectedPositionsInParquetFile() throws IOException {
    int rowCount = 400_000;
    int selectedRows = rowCount / 20;
    Random random = new Random(31L);
    MumblingBitmap bitmap = new MumblingBitmap();
    Set<Integer> positions = new HashSet<>();

    while (positions.size() < selectedRows) {
      int position = random.nextInt(rowCount);
      if (positions.add(position)) {
        assertThat(bitmap.set(position)).isTrue();
      }
    }

    assertThat(bitmap.cardinality()).isEqualTo(selectedRows);

    Schema schema = new Schema(required(1, "deleted_pos", Types.IntegerType.get()));

    File parquetFile = File.createTempFile("mumbling-bitmap", ".parquet", temp.toFile());
    assertThat(parquetFile.delete()).isTrue();

    // Positions are monotonically increasing, so writing them sorted and using DELTA_BINARY_PACKED
    // (Parquet writer v2, dictionary disabled) encodes the small deltas very compactly.
    List<Integer> sortedPositions = Lists.newArrayList(positions);
    Collections.sort(sortedPositions);

    try (FileAppender<Record> appender =
        Parquet.write(Files.localOutput(parquetFile))
            .schema(schema)
            .writerVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
            .set(ParquetOutputFormat.ENABLE_DICTIONARY, "false")
            .createWriterFunc(fileSchema -> GenericParquetWriter.create(schema, fileSchema))
            .build()) {
      for (int position : sortedPositions) {
        Record record = GenericRecord.create(schema);
        record.setField("deleted_pos", position);
        appender.add(record);
      }
    }

    MumblingBitmap copy = new MumblingBitmap();
    try (CloseableIterable<Record> reader =
        Parquet.read(Files.localInput(parquetFile))
            .project(schema)
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
            .build()) {
      for (Record read : reader) {
        copy.set((int) read.getField("deleted_pos"));
      }
    }

    assertThat(copy).isEqualTo(bitmap);
    assertThat(copy.cardinality()).isEqualTo(selectedRows);
    for (int position = 0; position < rowCount; position += 1) {
      assertThat(copy.isSet(position)).isEqualTo(positions.contains(position));
    }
  }

  @Test
  void rejectsPositionsOutsideRowRange() {
    MumblingBitmap bitmap = new MumblingBitmap();

    assertThat(bitmap.set(399_999)).isTrue();
    assertThat(bitmap.isSet(399_999)).isTrue();

    assertThatThrownBy(() -> bitmap.set(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid position: -1");
    assertThatThrownBy(() -> bitmap.isSet(400_000))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid position: 400000");
  }
}

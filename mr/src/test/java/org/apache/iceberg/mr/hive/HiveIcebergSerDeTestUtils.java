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

package org.apache.iceberg.mr.hive;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.iceberg.util.UUIDUtil;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class HiveIcebergSerDeTestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergSerDeTestUtils.class);

  // TODO: Can this be a constant all around the Iceberg tests?
  public static final Schema FULL_SCHEMA = new Schema(
      optional(1, "boolean_type", Types.BooleanType.get()),
      optional(2, "integer_type", Types.IntegerType.get()),
      optional(3, "long_type", Types.LongType.get()),
      optional(4, "float_type", Types.FloatType.get()),
      optional(5, "double_type", Types.DoubleType.get()),
      optional(6, "date_type", Types.DateType.get()),
      // TimeType is not supported
      // required(7, "time_type", Types.TimeType.get()),
      optional(7, "tsTz", Types.TimestampType.withZone()),
      optional(8, "ts", Types.TimestampType.withoutZone()),
      optional(9, "string_type", Types.StringType.get()),
      optional(10, "uuid_type", Types.UUIDType.get()),
      optional(11, "fixed_type", Types.FixedType.ofLength(3)),
      optional(12, "binary_type", Types.BinaryType.get()),
      optional(13, "decimal_type", Types.DecimalType.of(38, 10)));

  private HiveIcebergSerDeTestUtils() {
    // Empty constuctor for the utility class
  }

  public static Record getTestRecord(boolean uuidAsByte) {
    Record record = GenericRecord.create(HiveIcebergSerDeTestUtils.FULL_SCHEMA);
    record.set(0, true);
    record.set(1, 1);
    record.set(2, 2L);
    record.set(3, 3.1f);
    record.set(4, 4.2d);
    record.set(5, LocalDate.of(2020, 1, 21));
    // TimeType is not supported
    // record.set(6, LocalTime.of(11, 33));
    // Nano is not supported
    record.set(6, OffsetDateTime.of(2017, 11, 22, 11, 30, 7, 0, ZoneOffset.ofHours(2)));
    record.set(7, LocalDateTime.of(2019, 2, 22, 9, 44, 54));
    record.set(8, "kilenc");
    if (uuidAsByte) {
      // TODO: Parquet UUID expect byte[], others are expecting UUID
      record.set(9, UUIDUtil.convert(UUID.fromString("1-2-3-4-5")));
    } else {
      record.set(9, UUID.fromString("1-2-3-4-5"));
    }
    record.set(10, new byte[] {0, 1, 2});
    record.set(11, ByteBuffer.wrap(new byte[] {0, 1, 2, 3}));
    record.set(12, new BigDecimal("0.0000000013"));

    return record;
  }

  public static Record getNullTestRecord() {
    Record record = GenericRecord.create(HiveIcebergSerDeTestUtils.FULL_SCHEMA);

    for (int i = 0; i < HiveIcebergSerDeTestUtils.FULL_SCHEMA.columns().size(); i++) {
      record.set(i, null);
    }

    return record;
  }

  public static void assertEquals(Record expected, Record actual) {
    for (int i = 0; i < expected.size(); ++i) {
      if (expected.get(i) instanceof OffsetDateTime) {
        // For OffsetDateTime we just compare the actual instant
        Assert.assertEquals(((OffsetDateTime) expected.get(i)).toInstant(),
            ((OffsetDateTime) actual.get(i)).toInstant());
      } else {
        if (expected.get(i) instanceof byte[]) {
          Assert.assertArrayEquals((byte[]) expected.get(i), (byte[]) actual.get(i));
        } else {
          Assert.assertEquals(expected.get(i), actual.get(i));
        }
      }
    }
  }

  public static List<Record> load(FileIO io, Table table) {
    TableScan scan = table.newScan();
    List<Record> result = new ArrayList<>();
    scan.planFiles().forEach(fileScanTask -> {
      CloseableIterable<Object> readIter = open(io, table.schema(), fileScanTask);
      readIter.forEach(record -> result.add((Record) record));
      try {
        readIter.close();
      } catch (IOException ioException) {
        LOG.error("Error closing file", ioException);
      }
    });
    return result;
  }

  public static void validate(Table table, List<Record> expected, Integer sortBy) {
    // Refresh the table, so we get the new data as well
    table.refresh();
    List<Record> records = HiveIcebergSerDeTestUtils.load(table.io(), table);

    // Sort if needed
    if (sortBy != null) {
      expected.sort(Comparator.comparingLong(record -> ((Long) record.get(sortBy.intValue())).longValue()));
      records.sort(Comparator.comparingLong(record -> ((Long) record.get(sortBy.intValue())).longValue()));
    }
    Assert.assertEquals(expected.size(), records.size());
    for (int i = 0; i < expected.size(); ++i) {
      HiveIcebergSerDeTestUtils.assertEquals(expected.get(i), records.get(i));
    }
  }

  private static CloseableIterable<Object> open(FileIO io, Schema schema, FileScanTask task) {
    InputFile input = io.newInputFile(task.file().path().toString());
    // TODO: join to partition data from the manifest file
    switch (task.file().format()) {
      case AVRO:
        Avro.ReadBuilder avroReadBuilder = Avro.read(input)
            .project(schema)
            .split(task.start(), task.length());
        avroReadBuilder.createReaderFunc(
            (expIcebergSchema, expAvroSchema) ->
                DataReader.create(expIcebergSchema, expAvroSchema,
                    constantsMap(task, IdentityPartitionConverters::convertConstant)));
        return avroReadBuilder.build();
      case PARQUET:
        Parquet.ReadBuilder parquetReadBuilder = Parquet.read(input)
            .project(schema)
            .filter(task.residual())
            .caseSensitive(false)
            .split(task.start(), task.length());
        parquetReadBuilder.createReaderFunc(
            fileSchema -> GenericParquetReaders.buildReader(
                schema,
                fileSchema,
                constantsMap(task, IdentityPartitionConverters::convertConstant)));
        return parquetReadBuilder.build();
      case ORC:
        Map<Integer, ?> idToConstant = constantsMap(task, IdentityPartitionConverters::convertConstant);
        Schema readSchemaWithoutConstantAndMetadataFields = TypeUtil.selectNot(schema,
            Sets.union(idToConstant.keySet(), MetadataColumns.metadataFieldIds()));
        ORC.ReadBuilder orcReadBuilder = ORC.read(input)
            .project(readSchemaWithoutConstantAndMetadataFields)
            .filter(task.residual())
            .caseSensitive(false)
            .split(task.start(), task.length());
        orcReadBuilder.createReaderFunc(
            fileSchema -> GenericOrcReader.buildReader(
                schema, fileSchema, idToConstant));
        return orcReadBuilder.build();
      default:
        throw new UnsupportedOperationException(String.format("Cannot read %s file: %s",
            task.file().format().name(), task.file().path()));
    }
  }

  private static Map<Integer, ?> constantsMap(FileScanTask task, BiFunction<Type, Object, Object> converter) {
    PartitionSpec spec = task.spec();
    Set<Integer> idColumns = spec.identitySourceIds();
    Schema partitionSchema = TypeUtil.select(HiveIcebergSerDeTestUtils.FULL_SCHEMA, idColumns);
    boolean projectsIdentityPartitionColumns = !partitionSchema.columns().isEmpty();
    if (projectsIdentityPartitionColumns) {
      return PartitionUtil.constantsMap(task, converter);
    } else {
      return Collections.emptyMap();
    }
  }
}

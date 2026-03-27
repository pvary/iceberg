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
package org.apache.iceberg.data;

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroFormatModel;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.formats.FormatModel;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.orc.ORCFormatModel;
import org.apache.iceberg.parquet.ParquetFormatModel;

public class GenericFormatModels {
  public static void register() {
    FormatModelRegistry.register(
        AvroFormatModel.create(
            Record.class,
            Void.class,
            (icebergSchema, fileSchema, engineSchema) -> DataWriter.create(fileSchema),
            (icebergSchema, fileSchema, engineSchema, idToConstant) ->
                PlannedDataReader.create(icebergSchema, idToConstant)));

    FormatModelRegistry.register(AvroFormatModel.forPositionDeletes());

    FormatModelRegistry.register(
        ParquetFormatModel.create(
            Record.class,
            Void.class,
            (icebergSchema, fileSchema, engineSchema) ->
                GenericParquetWriter.create(icebergSchema, fileSchema),
            (icebergSchema, fileSchema, engineSchema, idToConstant) ->
                GenericParquetReaders.buildReader(icebergSchema, fileSchema, idToConstant),
            MyCombiner::new,
            (icebergSchema, family) -> {
              NarrowedRecord record = NarrowedRecord.create(icebergSchema, family);
              return new MyNarrower(record);
            }));

    FormatModelRegistry.register(ParquetFormatModel.forPositionDeletes());

    FormatModelRegistry.register(
        ORCFormatModel.create(
            Record.class,
            Void.class,
            (icebergSchema, fileSchema, engineSchema) ->
                GenericOrcWriter.buildWriter(icebergSchema, fileSchema),
            (icebergSchema, fileSchema, engineSchema, idToConstant) ->
                GenericOrcReader.buildReader(icebergSchema, fileSchema, idToConstant)));

    FormatModelRegistry.register(ORCFormatModel.forPositionDeletes());
  }

  private static class MyNarrower implements FormatModel.Narrower<Record> {
    private final NarrowedRecord record;

    MyNarrower(NarrowedRecord record) {
      this.record = record;
    }

    @Override
    public Record narrow(Record wrappedRecord) {
      record.set(wrappedRecord);

      return record;
    }
  }

  private static class MyCombiner implements FormatModel.Combiner<Record> {
    private final CombinedRecord template;
    private final boolean reuseContainers;

    MyCombiner(Schema schema, Integer[][] families, boolean reuseContainers) {
      this.template = CombinedRecord.create(schema, families, reuseContainers);
      this.reuseContainers = reuseContainers;
    }

    @Override
    public Record combine(List<Record> records) {
      CombinedRecord target = reuseContainers ? template : CombinedRecord.clone(template);
      for (int i = 0; i < records.size(); i++) {
        target.setFamily(i, records.get(i));
      }

      return target;
    }

    @Override
    public Record copyInto(Record source, Record target) {
      if (!reuseContainers) {
        return source;
      }

      if (source instanceof GenericRecord src) {
        GenericRecord tgt =
            target instanceof GenericRecord
                ? (GenericRecord) target
                : GenericRecord.create(src.struct());
        System.arraycopy(src.values(), 0, tgt.values(), 0, src.size());
        return tgt;
      }

      return source;
    }
  }

  private GenericFormatModels() {}
}

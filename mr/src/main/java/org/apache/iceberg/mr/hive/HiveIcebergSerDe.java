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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.hive.serde.objectinspector.IcebergObjectInspector;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.mr.mapreduce.IcebergWritable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class HiveIcebergSerDe extends AbstractSerDe {
  private Table table;
  private ObjectInspector inspector;

  @Override
  public void initialize(@Nullable Configuration configuration, Properties serDeProperties) throws SerDeException {
    this.table = Catalogs.loadTable(configuration, serDeProperties);

    try {
      this.inspector = IcebergObjectInspector.create(table.schema());
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Container.class;
  }

  @Override
  public IcebergWritable serialize(Object obj, ObjectInspector outerObjectInspector) throws SerDeException {
    Preconditions.checkArgument(outerObjectInspector.getCategory() == ObjectInspector.Category.STRUCT);

    StructObjectInspector soi = (StructObjectInspector) outerObjectInspector;
    List<Object> writableObj = soi.getStructFieldsDataAsList(obj);
    List<? extends StructField> fields = soi.getAllStructFieldRefs();

    Record record = GenericRecord.create(table.schema());
    for (int i = 0; i < table.schema().columns().size(); i++) {
      StructField field = fields.get(i);
      Object value = writableObj.get(i);

      if (value == null) {
        record.setField(table.schema().findColumnName(i), null);
      } else {
        Type type = table.schema().columns().get(i).type();
        ObjectInspector fieldInspector = field.getFieldObjectInspector();
        switch (type.typeId()) {
          case BOOLEAN:
            boolean boolVal = ((BooleanObjectInspector) fieldInspector).get(value);
            record.set(i, boolVal);
            break;
          case INTEGER:
            int intVal = ((IntObjectInspector) fieldInspector).get(value);
            record.set(i, intVal);
            break;
          case LONG:
            long longVal = ((LongObjectInspector) fieldInspector).get(value);
            record.set(i, longVal);
            break;
          case FLOAT:
            float floatVal = ((FloatObjectInspector) fieldInspector).get(value);
            record.set(i, floatVal);
            break;
          case DOUBLE:
            double doubleVal = ((DoubleObjectInspector) fieldInspector).get(value);
            record.set(i, doubleVal);
            break;
          case DATE:
            Date dateVal = ((DateObjectInspector) fieldInspector).getPrimitiveJavaObject(value);
            record.set(i, dateVal.toLocalDate());
            break;
          case TIMESTAMP:
            // TODO: handle timezone in Hive 3.x where Hive type also has TZ
            java.sql.Timestamp timestampVal = ((TimestampObjectInspector) fieldInspector).getPrimitiveJavaObject(value);
            Types.TimestampType timestampType = (Types.TimestampType) table.schema().columns().get(i).type();
            if (timestampType.shouldAdjustToUTC()) {
              record.set(i, OffsetDateTime.ofInstant(timestampVal.toInstant(), ZoneId.of("UTC")));
            } else {
              record.set(i, timestampVal.toLocalDateTime());
            }
            break;
          case STRING:
            String stringVal = ((StringObjectInspector) fieldInspector).getPrimitiveJavaObject(value);
            record.set(i, stringVal);
            break;
          case UUID:
            String stringUuidVal = ((StringObjectInspector) fieldInspector).getPrimitiveJavaObject(value);
            // TODO: This will not work with Parquet. Parquet UUID expect byte[], others are expecting UUID
            record.set(i, UUID.fromString(stringUuidVal));
            break;
          case FIXED:
            byte[] bytesVal = ((BinaryObjectInspector) fieldInspector).getPrimitiveJavaObject(value);
            record.set(i, bytesVal);
            break;
          case BINARY:
            byte[] binaryBytesVal = ((BinaryObjectInspector) fieldInspector).getPrimitiveJavaObject(value);
            record.set(i, ByteBuffer.wrap(binaryBytesVal));
            break;
          case DECIMAL:
            BigDecimal decimalVal =
                ((HiveDecimalObjectInspector) fieldInspector).getPrimitiveJavaObject(value).bigDecimalValue();
            record.set(i, decimalVal);
            break;
          case STRUCT:
          case LIST:
          case MAP:
          case TIME:
          default:
            throw new SerDeException("Unsupported column type: " + type);
        }
      }
    }
    return new IcebergWritable(record);
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }

  @Override
  public Object deserialize(Writable writable) {
    return ((Container<?>) writable).get();
  }

  @Override
  public ObjectInspector getObjectInspector() {
    return inspector;
  }
}

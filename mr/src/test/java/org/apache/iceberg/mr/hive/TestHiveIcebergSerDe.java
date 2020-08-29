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

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.mr.HiveSerDeConfig;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.mr.hive.serde.objectinspector.IcebergObjectInspector;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.mr.mapreduce.IcebergWritable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestHiveIcebergSerDe {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private static final Configuration conf = new Configuration();

  private TestHelper helper;

  @Before
  public void before() throws Exception {
    helper = new TestHelper(conf,
        new HadoopTables(conf),
        temp.newFolder("ORC").toString(),
        HiveIcebergSerDeTestUtils.FULL_SCHEMA,
        null,
        FileFormat.ORC,
        temp);
  }

  @Test
  public void testInitialize() throws SerDeException {
    Properties properties = createUnpartitionedTable();

    HiveIcebergSerDe serDe = new HiveIcebergSerDe();
    serDe.initialize(conf, properties);

    Assert.assertEquals(IcebergObjectInspector.create(HiveIcebergSerDeTestUtils.FULL_SCHEMA),
        serDe.getObjectInspector());
  }

  @Test
  public void testDeserialize() {
    HiveIcebergSerDe serDe = new HiveIcebergSerDe();

    Record record = RandomGenericData.generate(HiveIcebergSerDeTestUtils.FULL_SCHEMA, 1, 0).get(0);
    Container<Record> container = new Container<>();
    container.set(record);

    Assert.assertEquals(record, serDe.deserialize(container));
  }

  @Test
  public void testFromWritables() throws Exception {
    Properties properties = createUnpartitionedTable();

    HiveIcebergSerDe serDe = new HiveIcebergSerDe();
    serDe.initialize(conf, properties);

    Record record = HiveIcebergSerDeTestUtils.getTestRecord(false);

    // Capitalized `boolean_type` field to check for field case insensitivity.
    List<String> fieldNames = Arrays.asList("Boolean_Type", "integer_type", "long_type", "float_type", "double_type",
        "date_type", "tsTz", "ts", "string_type", "uuid_type", "fixed_type", "binary_type", "decimal_type");

    List<ObjectInspector> ois = Arrays.asList(
        PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
        PrimitiveObjectInspectorFactory.writableIntObjectInspector,
        PrimitiveObjectInspectorFactory.writableLongObjectInspector,
        PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
        PrimitiveObjectInspectorFactory.writableDateObjectInspector,
        PrimitiveObjectInspectorFactory.writableTimestampObjectInspector,
        PrimitiveObjectInspectorFactory.writableTimestampObjectInspector,
        PrimitiveObjectInspectorFactory.writableStringObjectInspector,
        PrimitiveObjectInspectorFactory.writableStringObjectInspector,
        PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
        PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
        PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector
    );

    List<Object> values = Arrays.asList(
        new BooleanWritable(Boolean.TRUE),
        new IntWritable(1),
        new LongWritable(2L),
        new FloatWritable(3.1f),
        new DoubleWritable(4.2d),
        new DateWritable(Date.valueOf("2020-01-21")),
        // TimeType is not supported
        // new Timestamp()
        new TimestampWritable(
            Timestamp.from(OffsetDateTime.of(2017, 11, 22, 11, 30, 7, 0, ZoneOffset.ofHours(2)).toInstant())),
        new TimestampWritable(Timestamp.valueOf(LocalDateTime.of(2019, 2, 22, 9, 44, 54))),
        new Text("kilenc"),
        new Text("1-2-3-4-5"),
        new BytesWritable(new byte[] {0, 1, 2}),
        new BytesWritable(new byte[] {0, 1, 2, 3}),
        new HiveDecimalWritable(HiveDecimal.create("0.0000000013"))
    );
    StandardStructObjectInspector objectInspector =
        ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, ois);
    IcebergWritable afterWritable = serDe.serialize(values, objectInspector);

    HiveIcebergSerDeTestUtils.assertEquals(record, afterWritable.record());
  }

  private Properties createUnpartitionedTable() {
    Table table = helper.createUnpartitionedTable();
    Properties props = new Properties();
    props.put(HiveSerDeConfig.WRITE_FILE_FORMAT, FileFormat.ORC);
    props.put(HiveSerDeConfig.TABLE_LOCATION, table.location());
    props.put("location", table.location());
    props.put(HiveConf.ConfVars.HIVEQUERYID.varname, "TestQuery_ORC");
    return props;
  }
}

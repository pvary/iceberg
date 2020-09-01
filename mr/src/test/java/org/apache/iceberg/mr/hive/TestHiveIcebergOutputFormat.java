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
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.mr.hive.HiveIcebergOutputFormat.IcebergRecordWriter;
import org.apache.iceberg.mr.mapreduce.IcebergWritable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestHiveIcebergOutputFormat {

  private static final Object[] TESTED_FILE_FORMATS = new Object[] {"avro", "orc", "parquet"};

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private static final Configuration conf = new Configuration();

  private TestHelper helper;

  // parametrized variables
  private final FileFormat fileFormat;

  @Parameterized.Parameters(name = "{0}")
  public static Object[] parameters() {
    return TESTED_FILE_FORMATS;
  }

  public TestHiveIcebergOutputFormat(String fileFormat) throws IOException {
    this.fileFormat = FileFormat.valueOf(fileFormat.toUpperCase(Locale.ENGLISH));
  }

  @Before
  public void before() throws Exception {
    helper = new TestHelper(conf,
        new HadoopTables(conf),
        temp.newFolder(this.fileFormat.name()).toString(),
        HiveIcebergSerDeTestUtils.FULL_SCHEMA,
        null,
        fileFormat,
        temp);
  }

  @Test
  public void testGoodRow() throws Exception {
    Table table = helper.createUnpartitionedTable();
    Properties props = new Properties();
    props.put(InputFormatConfig.WRITE_FILE_FORMAT, fileFormat.name());
    props.put(InputFormatConfig.TABLE_LOCATION, table.location());
    props.put("location", table.location());
    props.put(HiveConf.ConfVars.HIVEQUERYID.varname, "TestQuery_" + fileFormat);

    HiveIcebergOutputFormat outputFormat = new HiveIcebergOutputFormat();

    IcebergRecordWriter writer =
        (IcebergRecordWriter) outputFormat.getHiveRecordWriter(new JobConf(conf),
            null, null, false, props, null);

    // Write a row.
    Record record = HiveIcebergSerDeTestUtils.getTestRecord(FileFormat.PARQUET.equals(fileFormat));

    writer.write(new IcebergWritable(record));

    writer.close(false);

    // Reload the table, so we get the new data as well
    Table newTable = Catalogs.loadTable(conf, props);
    List<Record> records = HiveIcebergSerDeTestUtils.load(new HadoopFileIO(conf), newTable);

    Assert.assertEquals(1, records.size());
    HiveIcebergSerDeTestUtils.assertEquals(record, records.get(0));
  }
}

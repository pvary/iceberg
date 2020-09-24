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

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(StandaloneHiveRunner.class)
public class TestHiveRunnerWrite {
  private static final Schema SCHEMA = new Schema(
      required(1, "data", Types.StringType.get()),
      required(2, "id", Types.LongType.get()));

  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("id", 3).build();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @HiveSQL(files = {})
  private HiveShell shell;

  private static final Configuration conf = new Configuration();
  private TestHelper helper;
  private Table table;

  @Before
  public void before() throws Exception {
    helper = new TestHelper(conf,
        new HadoopTables(conf),
        temp.newFolder(FileFormat.PARQUET.name()).toString(),
        SCHEMA,
        null,
        FileFormat.PARQUET,
        temp);

    table = helper.createUnpartitionedTable();

    shell.executeQuery("CREATE TABLE withShell STORED BY '" + HiveIcebergStorageHandler.class.getName() + "' " +
        "LOCATION '" + table.location() + "' " +
        "TBLPROPERTIES ('" + InputFormatConfig.WRITE_FILE_FORMAT + "'='" + FileFormat.PARQUET.name() + "')");
  }

  @After
  public void after() {
    shell.executeQuery("DROP TABLE withShell");
  }

  @Test
  public void testInsert() throws IOException {
    List<Record> records = helper.generateRandomRecords(2, 0L);
    // The expected query is like
    // INSERT INTO withShell VALUES ('farkas', 1), ('kutya', 2)
    StringBuilder query = new StringBuilder().append("INSERT INTO withShell VALUES ");
    records.forEach(record -> query.append("('").append(record.get(0)).append("',").append(record.get(1)).append("),"));
    query.setLength(query.length() - 1);

    shell.executeQuery(query.toString());

    HiveIcebergSerDeTestUtils.validate(table, records, 1);
  }

  @Test
  public void testInsertFromSelect() throws IOException {
    List<Record> records = helper.generateRandomRecords(2, 0L);
    helper.appendToTable(null, records);

    // Just check the initial data
    HiveIcebergSerDeTestUtils.validate(table, records, 1);

    shell.executeQuery("INSERT INTO withShell SELECT * FROM withShell");

    // Check that everything is duplicated as expected
    records.addAll(records);
    HiveIcebergSerDeTestUtils.validate(table, records, 1);
  }

  @Test
  public void testInsertFromSelectWithOrderBy() throws IOException {
    // We expect that there will be Mappers and Reducers here
    List<Record> records = helper.generateRandomRecords(2, 0L);
    helper.appendToTable(null, records);

    // Just check the initial data
    HiveIcebergSerDeTestUtils.validate(table, records, 1);

    shell.executeQuery("INSERT INTO withShell SELECT * FROM withShell ORDER BY id");

    // Check that everything is duplicated as expected
    records.addAll(records);
    HiveIcebergSerDeTestUtils.validate(table, records, 1);
  }

  @Test
  public void testDefaultFileFormat() throws IOException {
    shell.executeQuery("CREATE TABLE withShell2 STORED BY '" + HiveIcebergStorageHandler.class.getName() + "' " +
        "LOCATION '" + table.location() + "'");

    List<Record> records = helper.generateRandomRecords(2, 0L);
    // The expected query is like
    // INSERT INTO withShell VALUES ('farkas', 1), ('kutya', 2)
    StringBuilder query = new StringBuilder().append("INSERT INTO withShell2 VALUES ");
    records.forEach(record -> query.append("('").append(record.get(0)).append("',").append(record.get(1)).append("),"));
    query.setLength(query.length() - 1);

    shell.executeQuery(query.toString());

    HiveIcebergSerDeTestUtils.validate(table, records, 1);
  }

  @Test
  public void testPartitionedWrite() throws IOException {
    TestHelper helper2 = new TestHelper(conf,
        new HadoopTables(conf),
        temp.newFolder(FileFormat.PARQUET.name() + "_partitioned").toString(),
        SCHEMA,
        SPEC,
        FileFormat.PARQUET,
        temp);

    Table table2 = helper2.createTable(SCHEMA, SPEC);

    shell.executeQuery("CREATE TABLE withShell2 STORED BY '" + HiveIcebergStorageHandler.class.getName() + "' " +
        "LOCATION '" + table2.location() + "' " +
        "TBLPROPERTIES ('" + InputFormatConfig.WRITE_FILE_FORMAT + "'='" + FileFormat.PARQUET.name() + "')");

    List<Record> records = helper2.generateRandomRecords(4, 0L);
    // The expected query is like
    // INSERT INTO withShell VALUES ('farkas', 1), ('kutya', 2)
    StringBuilder query = new StringBuilder().append("INSERT INTO withShell2 VALUES ");
    records.forEach(record -> query.append("('").append(record.get(0)).append("',").append(record.get(1)).append("),"));
    query.setLength(query.length() - 1);

    shell.executeQuery(query.toString());

    HiveIcebergSerDeTestUtils.validate(table2, records, 1);
  }
}

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
package org.apache.iceberg.spark.sql;

import static org.apache.iceberg.TableProperties.SPLIT_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IndexSnapshots;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.IndexCatalog;
import org.apache.iceberg.catalog.IndexIdentifier;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.index.IndexType;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSelect extends CatalogTestBase {
  private int scanEventCount = 0;
  private ScanEvent lastScanEvent = null;

  @Parameter(index = 3)
  private String binaryTableName;

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}, binaryTableName = {3}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        SparkCatalogConfig.HIVE.properties(),
        SparkCatalogConfig.HIVE.catalogName() + ".default.binary_table"
      },
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties(),
        SparkCatalogConfig.HADOOP.catalogName() + ".default.binary_table"
      },
      {
        SparkCatalogConfig.SPARK_SESSION.catalogName(),
        SparkCatalogConfig.SPARK_SESSION.implementation(),
        SparkCatalogConfig.SPARK_SESSION.properties(),
        "default.binary_table"
      }
    };
  }

  @BeforeEach
  public void createTables() {
    // register a scan event listener to validate pushdown
    Listeners.register(
        event -> {
          scanEventCount += 1;
          lastScanEvent = event;
        },
        ScanEvent.class);

    sql("CREATE TABLE %s (id bigint, data string, float float) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 'a', 1.0), (2, 'b', 2.0), (3, 'c', float('NaN'))", tableName);

    this.scanEventCount = 0;
    this.lastScanEvent = null;
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %s", binaryTableName);
  }

  @TestTemplate
  public void testSelect() {
    List<Object[]> expected =
        ImmutableList.of(row(1L, "a", 1.0F), row(2L, "b", 2.0F), row(3L, "c", Float.NaN));

    assertEquals(
        "Should return all expected rows",
        expected,
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testSelectWithSpecifiedTargetSplitSize() {
    List<Object[]> expected =
        ImmutableList.of(row(1L, "a", 1.0F), row(2L, "b", 2.0F), row(3L, "c", Float.NaN));

    Table table = validationCatalog.loadTable(tableIdent);
    table.updateProperties().set("read.split.target-size", "1024").commit();
    spark.sql("REFRESH TABLE " + tableName);
    assertEquals(
        "Should return all expected rows",
        expected,
        sql("SELECT * FROM %s ORDER BY id", tableName));

    // Query failed when `SPLIT_SIZE` < 0
    table.updateProperties().set(SPLIT_SIZE, "-1").commit();
    spark.sql("REFRESH TABLE " + tableName);
    assertThatThrownBy(() -> sql("SELECT * FROM %s", tableName))
        .hasMessageContaining("Split size must be > 0: -1")
        .isInstanceOf(IllegalArgumentException.class);

    // Query failed when `SPLIT_SIZE` == 0
    table.updateProperties().set(SPLIT_SIZE, "0").commit();
    spark.sql("REFRESH TABLE " + tableName);
    assertThatThrownBy(() -> sql("SELECT * FROM %s", tableName))
        .hasMessageContaining("Split size must be > 0: 0")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @TestTemplate
  public void testSelectRewrite() {
    List<Object[]> expected = ImmutableList.of(row(3L, "c", Float.NaN));

    assertEquals(
        "Should return all expected rows",
        expected,
        sql("SELECT * FROM %s where float = float('NaN')", tableName));

    assertThat(scanEventCount).as("Should create only one scan").isEqualTo(1);
    assertThat(Spark3Util.describe(lastScanEvent.filter()))
        .as("Should push down expected filter")
        .isEqualTo("(float IS NOT NULL AND is_nan(float))");
  }

  @TestTemplate
  public void selectWithLimit() {
    Object[] first = row(1L, "a", 1.0F);
    Object[] second = row(2L, "b", 2.0F);
    Object[] third = row(3L, "c", Float.NaN);

    // verify that LIMIT is properly applied in case SupportsPushDownLimit.isPartiallyPushed() is
    // ever overridden in SparkScanBuilder
    assertThat(sql("SELECT * FROM %s ORDER BY id LIMIT 1", tableName)).containsExactly(first);
    assertThat(sql("SELECT * FROM %s ORDER BY id LIMIT 2", tableName))
        .containsExactly(first, second);
    assertThat(sql("SELECT * FROM %s ORDER BY id LIMIT 3", tableName))
        .containsExactly(first, second, third);
  }

  @TestTemplate
  public void testProjection() {
    List<Object[]> expected = ImmutableList.of(row(1L), row(2L), row(3L));
    assertEquals(
        "Should return all expected rows",
        expected,
        sql("SELECT id FROM %s ORDER BY id", tableName));

    assertThat(scanEventCount).as("Should create only one scan").isEqualTo(1);
    assertThat(lastScanEvent.filter())
        .as("Should not push down a filter")
        .isEqualTo(Expressions.alwaysTrue());
    assertThat(lastScanEvent.projection().asStruct())
        .as("Should project only the id column")
        .isEqualTo(validationCatalog.loadTable(tableIdent).schema().select("id").asStruct());
  }

  @TestTemplate
  public void testExpressionPushdown() {
    List<Object[]> expected = ImmutableList.of(row("b"));

    assertEquals(
        "Should return all expected rows",
        expected,
        sql("SELECT data FROM %s WHERE id = 2", tableName));

    assertThat(scanEventCount).as("Should create only one scan").isEqualTo(1);
    assertThat(Spark3Util.describe(lastScanEvent.filter()))
        .as("Should push down expected filter")
        .isEqualTo("(id IS NOT NULL AND id = 2)");
    assertThat(lastScanEvent.projection().asStruct())
        .as("Should project only id and data columns")
        .isEqualTo(
            validationCatalog.loadTable(tableIdent).schema().select("id", "data").asStruct());
  }

  @TestTemplate
  public void testMetadataTables() {
    assertEquals(
        "Snapshot metadata table",
        ImmutableList.of(row(ANY, ANY, null, "append", ANY, ANY)),
        sql("SELECT * FROM %s.snapshots", tableName));
  }

  @TestTemplate
  public void testSnapshotInTableName() {
    // get the snapshot ID of the last write and get the current row set as expected
    long snapshotId = validationCatalog.loadTable(tableIdent).currentSnapshot().snapshotId();
    List<Object[]> expected = sql("SELECT * FROM %s ORDER by id", tableName);

    // create a second snapshot
    sql("INSERT INTO %s VALUES (4, 'd', 4.0), (5, 'e', 5.0)", tableName);

    String prefix = "snapshot_id_";
    // read the table at the snapshot
    List<Object[]> actual = sql("SELECT * FROM %s.%s ORDER by id", tableName, prefix + snapshotId);
    assertEquals("Snapshot at specific ID, prefix " + prefix, expected, actual);

    // read the table using DataFrameReader option
    Dataset<Row> df =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.SNAPSHOT_ID, snapshotId)
            .load(tableName)
            .orderBy("id");
    List<Object[]> fromDF = rowsToJava(df.collectAsList());
    assertEquals("Snapshot at specific ID " + snapshotId, expected, fromDF);
  }

  @TestTemplate
  public void testTimestampInTableName() {
    // get a timestamp just after the last write and get the current row set as expected
    long snapshotTs = validationCatalog.loadTable(tableIdent).currentSnapshot().timestampMillis();
    long timestamp = waitUntilAfter(snapshotTs + 2);
    List<Object[]> expected = sql("SELECT * FROM %s ORDER by id", tableName);

    // create a second snapshot
    sql("INSERT INTO %s VALUES (4, 'd', 4.0), (5, 'e', 5.0)", tableName);

    String prefix = "at_timestamp_";
    // read the table at the snapshot
    List<Object[]> actual = sql("SELECT * FROM %s.%s ORDER by id", tableName, prefix + timestamp);
    assertEquals("Snapshot at timestamp, prefix " + prefix, expected, actual);

    // read the table using DataFrameReader option
    Dataset<Row> df =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.AS_OF_TIMESTAMP, timestamp)
            .load(tableName)
            .orderBy("id");
    List<Object[]> fromDF = rowsToJava(df.collectAsList());
    assertEquals("Snapshot at timestamp " + timestamp, expected, fromDF);
  }

  @TestTemplate
  public void testVersionAsOf() {
    // get the snapshot ID of the last write and get the current row set as expected
    long snapshotId = validationCatalog.loadTable(tableIdent).currentSnapshot().snapshotId();
    List<Object[]> expected = sql("SELECT * FROM %s ORDER BY id", tableName);

    // create a second snapshot
    sql("INSERT INTO %s VALUES (4, 'd', 4.0), (5, 'e', 5.0)", tableName);

    // read the table at the snapshot
    List<Object[]> actual1 =
        sql("SELECT * FROM %s VERSION AS OF %s ORDER BY id", tableName, snapshotId);
    assertEquals("Snapshot at specific ID", expected, actual1);

    // read the table at the snapshot
    // HIVE time travel syntax
    List<Object[]> actual2 =
        sql("SELECT * FROM %s FOR SYSTEM_VERSION AS OF %s ORDER BY id", tableName, snapshotId);
    assertEquals("Snapshot at specific ID", expected, actual2);

    // read the table using DataFrameReader option: versionAsOf
    Dataset<Row> df =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.VERSION_AS_OF, snapshotId)
            .load(tableName)
            .orderBy("id");
    List<Object[]> fromDF = rowsToJava(df.collectAsList());
    assertEquals("Snapshot at specific ID " + snapshotId, expected, fromDF);
  }

  @TestTemplate
  public void testTagReference() {
    Table table = validationCatalog.loadTable(tableIdent);
    long snapshotId = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createTag("test_tag", snapshotId).commit();
    List<Object[]> expected = sql("SELECT * FROM %s ORDER by id", tableName);

    // create a second snapshot, read the table at the tag
    sql("INSERT INTO %s VALUES (4, 'd', 4.0), (5, 'e', 5.0)", tableName);
    List<Object[]> actual1 =
        sql("SELECT * FROM %s VERSION AS OF 'test_tag' ORDER by id", tableName);
    assertEquals("Snapshot at specific tag reference name", expected, actual1);

    // read the table at the tag
    // HIVE time travel syntax
    List<Object[]> actual2 =
        sql("SELECT * FROM %s FOR SYSTEM_VERSION AS OF 'test_tag' ORDER by id", tableName);
    assertEquals("Snapshot at specific tag reference name", expected, actual2);

    // Spark session catalog does not support extended table names
    if (!"spark_catalog".equals(catalogName)) {
      // read the table using the "tag_" prefix in the table name
      List<Object[]> actual3 = sql("SELECT * FROM %s.tag_test_tag ORDER by id", tableName);
      assertEquals("Snapshot at specific tag reference name, prefix", expected, actual3);
    }

    // read the table using DataFrameReader option: tag
    Dataset<Row> df =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.TAG, "test_tag")
            .load(tableName)
            .orderBy("id");
    List<Object[]> fromDF = rowsToJava(df.collectAsList());
    assertEquals("Snapshot at specific tag reference name", expected, fromDF);
  }

  @TestTemplate
  public void testUseSnapshotIdForTagReferenceAsOf() {
    Table table = validationCatalog.loadTable(tableIdent);
    long snapshotId1 = table.currentSnapshot().snapshotId();

    // create a second snapshot, read the table at the snapshot
    List<Object[]> actual = sql("SELECT * FROM %s ORDER by id", tableName);
    sql("INSERT INTO %s VALUES (4, 'd', 4.0), (5, 'e', 5.0)", tableName);

    table.refresh();
    long snapshotId2 = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createTag(Long.toString(snapshotId1), snapshotId2).commit();

    // currently Spark version travel ignores the type of the AS OF
    // this means if a tag name matches a snapshot ID, it will always choose snapshotID to travel
    // to.
    List<Object[]> travelWithStringResult =
        sql("SELECT * FROM %s VERSION AS OF '%s' ORDER by id", tableName, snapshotId1);
    assertEquals("Snapshot at specific tag reference name", actual, travelWithStringResult);

    List<Object[]> travelWithLongResult =
        sql("SELECT * FROM %s VERSION AS OF %s ORDER by id", tableName, snapshotId1);
    assertEquals("Snapshot at specific tag reference name", actual, travelWithLongResult);
  }

  @TestTemplate
  public void testBranchReference() {
    Table table = validationCatalog.loadTable(tableIdent);
    long snapshotId = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createBranch("test_branch", snapshotId).commit();
    List<Object[]> expected = sql("SELECT * FROM %s ORDER by id", tableName);

    // create a second snapshot, read the table at the branch
    sql("INSERT INTO %s VALUES (4, 'd', 4.0), (5, 'e', 5.0)", tableName);
    List<Object[]> actual1 =
        sql("SELECT * FROM %s VERSION AS OF 'test_branch' ORDER by id", tableName);
    assertEquals("Snapshot at specific branch reference name", expected, actual1);

    // read the table at the branch
    // HIVE time travel syntax
    List<Object[]> actual2 =
        sql("SELECT * FROM %s FOR SYSTEM_VERSION AS OF 'test_branch' ORDER by id", tableName);
    assertEquals("Snapshot at specific branch reference name", expected, actual2);

    // Spark session catalog does not support extended table names
    if (!"spark_catalog".equals(catalogName)) {
      // read the table using the "branch_" prefix in the table name
      List<Object[]> actual3 = sql("SELECT * FROM %s.branch_test_branch ORDER by id", tableName);
      assertEquals("Snapshot at specific branch reference name, prefix", expected, actual3);
    }

    // read the table using DataFrameReader option: branch
    Dataset<Row> df =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.BRANCH, "test_branch")
            .load(tableName)
            .orderBy("id");
    List<Object[]> fromDF = rowsToJava(df.collectAsList());
    assertEquals("Snapshot at specific branch reference name", expected, fromDF);
  }

  @TestTemplate
  public void readAndWriteWithBranchAfterSchemaChange() {
    Table table = validationCatalog.loadTable(tableIdent);
    String branchName = "test_branch";
    table.manageSnapshots().createBranch(branchName, table.currentSnapshot().snapshotId()).commit();

    List<Object[]> expected =
        Arrays.asList(row(1L, "a", 1.0f), row(2L, "b", 2.0f), row(3L, "c", Float.NaN));
    assertThat(sql("SELECT * FROM %s", tableName)).containsExactlyInAnyOrderElementsOf(expected);

    // change schema on the table and add more data
    sql("ALTER TABLE %s DROP COLUMN float", tableName);
    sql("ALTER TABLE %s ADD COLUMN new_col date", tableName);
    sql(
        "INSERT INTO %s VALUES (4, 'd', date('2024-04-04')), (5, 'e', date('2024-05-05'))",
        tableName);

    // time-travel query using snapshot id should return the snapshot's schema
    long branchSnapshotId = table.refs().get(branchName).snapshotId();
    assertThat(sql("SELECT * FROM %s VERSION AS OF %s ORDER by id", tableName, branchSnapshotId))
        .containsExactlyElementsOf(expected);

    // querying the head of the branch should return the table's schema
    assertThat(sql("SELECT * FROM %s VERSION AS OF '%s' ORDER by id", tableName, branchName))
        .containsExactly(row(1L, "a", null), row(2L, "b", null), row(3L, "c", null));

    if (!"spark_catalog".equals(catalogName)) {
      // querying the head of the branch using 'branch_' should return the table's schema
      assertThat(sql("SELECT * FROM %s.branch_%s ORDER by id", tableName, branchName))
          .containsExactly(row(1L, "a", null), row(2L, "b", null), row(3L, "c", null));
    }

    // writing to a branch uses the table's schema
    sql(
        "INSERT INTO %s.branch_%s VALUES (6L, 'f', cast('2023-06-06' as date)), (7L, 'g', cast('2023-07-07' as date))",
        tableName, branchName);

    // querying the head of the branch returns the table's schema
    assertThat(sql("SELECT * FROM %s VERSION AS OF '%s' ORDER by id", tableName, branchName))
        .containsExactlyInAnyOrder(
            row(1L, "a", null),
            row(2L, "b", null),
            row(3L, "c", null),
            row(6L, "f", java.sql.Date.valueOf("2023-06-06")),
            row(7L, "g", java.sql.Date.valueOf("2023-07-07")));

    // using DataFrameReader with the 'branch' option should return the table's schema
    Dataset<Row> df =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.BRANCH, branchName)
            .load(tableName)
            .orderBy("id");
    assertThat(rowsToJava(df.collectAsList()))
        .containsExactlyInAnyOrder(
            row(1L, "a", null),
            row(2L, "b", null),
            row(3L, "c", null),
            row(6L, "f", java.sql.Date.valueOf("2023-06-06")),
            row(7L, "g", java.sql.Date.valueOf("2023-07-07")));
  }

  @TestTemplate
  public void testUnknownReferenceAsOf() {
    assertThatThrownBy(() -> sql("SELECT * FROM %s VERSION AS OF 'test_unknown'", tableName))
        .hasMessageContaining("Cannot find matching snapshot ID or reference name for version")
        .isInstanceOf(ValidationException.class);
  }

  @TestTemplate
  public void testTimestampAsOf() {
    long snapshotTs = validationCatalog.loadTable(tableIdent).currentSnapshot().timestampMillis();
    long timestamp = waitUntilAfter(snapshotTs + 1000);
    waitUntilAfter(timestamp + 1000);
    // AS OF expects the timestamp if given in long format will be of seconds precision
    long timestampInSeconds = TimeUnit.MILLISECONDS.toSeconds(timestamp);
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String formattedDate = sdf.format(new Date(timestamp));

    List<Object[]> expected = sql("SELECT * FROM %s ORDER BY id", tableName);

    // create a second snapshot
    sql("INSERT INTO %s VALUES (4, 'd', 4.0), (5, 'e', 5.0)", tableName);

    // read the table at the timestamp in long format i.e 1656507980463.
    List<Object[]> actualWithLongFormat =
        sql("SELECT * FROM %s TIMESTAMP AS OF %s ORDER BY id", tableName, timestampInSeconds);
    assertEquals("Snapshot at timestamp", expected, actualWithLongFormat);

    // read the table at the timestamp in date format i.e 2022-06-29 18:40:37
    List<Object[]> actualWithDateFormat =
        sql("SELECT * FROM %s TIMESTAMP AS OF '%s' ORDER BY id", tableName, formattedDate);
    assertEquals("Snapshot at timestamp", expected, actualWithDateFormat);

    // HIVE time travel syntax
    // read the table at the timestamp in long format i.e 1656507980463.
    List<Object[]> actualWithLongFormatInHiveSyntax =
        sql("SELECT * FROM %s FOR SYSTEM_TIME AS OF %s ORDER BY id", tableName, timestampInSeconds);
    assertEquals("Snapshot at specific ID", expected, actualWithLongFormatInHiveSyntax);

    // read the table at the timestamp in date format i.e 2022-06-29 18:40:37
    List<Object[]> actualWithDateFormatInHiveSyntax =
        sql("SELECT * FROM %s FOR SYSTEM_TIME AS OF '%s' ORDER BY id", tableName, formattedDate);
    assertEquals("Snapshot at specific ID", expected, actualWithDateFormatInHiveSyntax);

    // read the table using DataFrameReader option
    Dataset<Row> df =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.TIMESTAMP_AS_OF, formattedDate)
            .load(tableName)
            .orderBy("id");
    List<Object[]> fromDF = rowsToJava(df.collectAsList());
    assertEquals("Snapshot at timestamp " + timestamp, expected, fromDF);
  }

  @TestTemplate
  public void testInvalidTimeTravelBasedOnBothAsOfAndTableIdentifier() {
    // get the snapshot ID of the last write
    long snapshotId = validationCatalog.loadTable(tableIdent).currentSnapshot().snapshotId();
    // get a timestamp just after the last write
    long timestamp =
        validationCatalog.loadTable(tableIdent).currentSnapshot().timestampMillis() + 2;

    String timestampPrefix = "at_timestamp_";
    String snapshotPrefix = "snapshot_id_";

    // create a second snapshot
    sql("INSERT INTO %s VALUES (4, 'd', 4.0), (5, 'e', 5.0)", tableName);

    // using snapshot in table identifier and VERSION AS OF
    assertThatThrownBy(
            () -> {
              sql(
                  "SELECT * FROM %s.%s VERSION AS OF %s",
                  tableName, snapshotPrefix + snapshotId, snapshotId);
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot do time-travel based on both table identifier and AS OF");

    // using snapshot in table identifier and TIMESTAMP AS OF
    assertThatThrownBy(
            () -> {
              sql(
                  "SELECT * FROM %s.%s VERSION AS OF %s",
                  tableName, timestampPrefix + timestamp, snapshotId);
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot do time-travel based on both table identifier and AS OF");

    // using timestamp in table identifier and VERSION AS OF
    assertThatThrownBy(
            () -> {
              sql(
                  "SELECT * FROM %s.%s TIMESTAMP AS OF %s",
                  tableName, snapshotPrefix + snapshotId, timestamp);
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot do time-travel based on both table identifier and AS OF");

    // using timestamp in table identifier and TIMESTAMP AS OF
    assertThatThrownBy(
            () -> {
              sql(
                  "SELECT * FROM %s.%s TIMESTAMP AS OF %s",
                  tableName, timestampPrefix + timestamp, timestamp);
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot do time-travel based on both table identifier and AS OF");
  }

  @TestTemplate
  public void testInvalidTimeTravelAgainstBranchIdentifierWithAsOf() {
    long snapshotId = validationCatalog.loadTable(tableIdent).currentSnapshot().snapshotId();
    validationCatalog.loadTable(tableIdent).manageSnapshots().createBranch("b1").commit();

    // create a second snapshot
    sql("INSERT INTO %s VALUES (4, 'd', 4.0), (5, 'e', 5.0)", tableName);

    // using branch_b1 in the table identifier and VERSION AS OF
    assertThatThrownBy(
            () -> sql("SELECT * FROM %s.branch_b1 VERSION AS OF %s", tableName, snapshotId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot do time-travel based on both table identifier and AS OF");

    // using branch_b1 in the table identifier and TIMESTAMP AS OF
    assertThatThrownBy(() -> sql("SELECT * FROM %s.branch_b1 TIMESTAMP AS OF now()", tableName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot do time-travel based on both table identifier and AS OF");
  }

  @TestTemplate
  public void testSpecifySnapshotAndTimestamp() {
    // get the snapshot ID of the last write
    long snapshotId = validationCatalog.loadTable(tableIdent).currentSnapshot().snapshotId();
    // get a timestamp just after the last write
    long timestamp =
        validationCatalog.loadTable(tableIdent).currentSnapshot().timestampMillis() + 2;

    // create a second snapshot
    sql("INSERT INTO %s VALUES (4, 'd', 4.0), (5, 'e', 5.0)", tableName);

    assertThatThrownBy(
            () -> {
              spark
                  .read()
                  .format("iceberg")
                  .option(SparkReadOptions.SNAPSHOT_ID, snapshotId)
                  .option(SparkReadOptions.AS_OF_TIMESTAMP, timestamp)
                  .load(tableName)
                  .collectAsList();
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            String.format(
                "Can specify only one of snapshot-id (%s), as-of-timestamp (%s)",
                snapshotId, timestamp));
  }

  @TestTemplate
  public void testBinaryInFilter() {
    sql("CREATE TABLE %s (id bigint, binary binary) USING iceberg", binaryTableName);
    sql("INSERT INTO %s VALUES (1, X''), (2, X'1111'), (3, X'11')", binaryTableName);
    List<Object[]> expected = ImmutableList.of(row(2L, new byte[] {0x11, 0x11}));

    assertEquals(
        "Should return all expected rows",
        expected,
        sql("SELECT id, binary FROM %s where binary > X'11'", binaryTableName));
  }

  @TestTemplate
  public void testComplexTypeFilter() {
    String complexTypeTableName = tableName("complex_table");
    sql(
        "CREATE TABLE %s (id INT, complex STRUCT<c1:INT,c2:STRING>) USING iceberg",
        complexTypeTableName);
    sql(
        "INSERT INTO TABLE %s VALUES (1, named_struct(\"c1\", 3, \"c2\", \"v1\"))",
        complexTypeTableName);
    sql(
        "INSERT INTO TABLE %s VALUES (2, named_struct(\"c1\", 2, \"c2\", \"v2\"))",
        complexTypeTableName);

    List<Object[]> result =
        sql(
            "SELECT id FROM %s WHERE complex = named_struct(\"c1\", 3, \"c2\", \"v1\")",
            complexTypeTableName);

    assertEquals("Should return all expected rows", ImmutableList.of(row(1)), result);
    sql("DROP TABLE IF EXISTS %s", complexTypeTableName);
  }

  @TestTemplate
  public void testRequiredNestedFieldInOptionalStructFilter() {
    String nestedStructTable = tableName("nested_struct_table");
    sql(
        "CREATE TABLE %s (id INT NOT NULL, address STRUCT<street: STRING NOT NULL>) "
            + "USING iceberg",
        nestedStructTable);
    sql("INSERT INTO %s VALUES (0, NULL)", nestedStructTable);
    sql("INSERT INTO %s VALUES (1, STRUCT('123 Main St'))", nestedStructTable);

    List<Object[]> result =
        sql("SELECT id FROM %s WHERE address.street IS NULL", nestedStructTable);

    assertEquals("Should return all expected rows", ImmutableList.of(row(0)), result);
    sql("DROP TABLE IF EXISTS %s", nestedStructTable);
  }

  @TestTemplate
  public void simpleTypesInFilter() {
    String tableName = tableName("simple_types_table");
    sql(
        "CREATE TABLE IF NOT EXISTS %s (id bigint, boolean boolean, integer integer, long long, "
            + "float float, double double, string string, date date, timestamp timestamp) USING iceberg",
        tableName);
    sql(
        "INSERT INTO %s VALUES (1, true, 1, 1L, 1.1, 1.3, '1.5', to_date('2021-01-01'), to_timestamp('2021-01-01T00:00:00')), "
            + "(2, false, 2, 2L, 2.2, 2.4, '2.6', to_date('2022-02-02'), to_timestamp('2022-02-02T00:00:00')), "
            + "(3, true, 3, 3L, 3.3, 3.6, '3.9', to_date('2023-03-03'), to_timestamp('2023-03-03T00:00:00'))",
        tableName);
    assertThat(sql("SELECT id FROM %s where id > 1", tableName))
        .containsExactlyInAnyOrder(row(2L), row(3L));
    assertThat(sql("SELECT id, boolean FROM %s where boolean = true", tableName))
        .containsExactlyInAnyOrder(row(1L, true), row(3L, true));
    assertThat(sql("SELECT long FROM %s where long > 1", tableName))
        .containsExactlyInAnyOrder(row(2L), row(3L));
    assertThat(sql("SELECT float FROM %s where float > 1.1f", tableName))
        .containsExactlyInAnyOrder(row(2.2f), row(3.3f));
    assertThat(sql("SELECT double FROM %s where double > 1.3", tableName))
        .containsExactlyInAnyOrder(row(2.4d), row(3.6d));
    assertThat(sql("SELECT string FROM %s where string > '1.5'", tableName))
        .containsExactlyInAnyOrder(row("2.6"), row("3.9"));
    java.sql.Date dateOne = java.sql.Date.valueOf("2022-02-02");
    java.sql.Date dateTwo = java.sql.Date.valueOf("2023-03-03");
    assertThat(sql("SELECT date FROM %s where date > to_date('2021-01-01')", tableName))
        .containsExactlyInAnyOrder(row(dateOne), row(dateTwo));
    assertThat(
            sql("SELECT timestamp FROM %s where timestamp > to_timestamp('2021-01-01')", tableName))
        .containsExactlyInAnyOrder(
            row(new Timestamp(dateOne.getTime())), row(new Timestamp(dateTwo.getTime())));

    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void variantTypeInFilter() {
    assumeThat(validationCatalog)
        .as("Variant is not supported in Hive catalog")
        .isNotInstanceOf(HiveCatalog.class);

    String tableName = tableName("variant_table");
    sql(
        "CREATE TABLE %s (id BIGINT, v1 VARIANT, v2 VARIANT) USING iceberg TBLPROPERTIES ('format-version'='3')",
        tableName);

    String v1r1 = "{\"a\":5}";
    String v1r2 = "{\"a\":10}";
    String v2r1 = "{\"x\":15}";
    String v2r2 = "{\"x\":20}";

    sql("INSERT INTO %s SELECT 1, parse_json('%s'), parse_json('%s')", tableName, v1r1, v2r1);
    sql("INSERT INTO %s SELECT 2, parse_json('%s'), parse_json('%s')", tableName, v1r2, v2r2);

    assertThat(
            sql(
                "SELECT id, try_variant_get(v1, '$.a', 'int') FROM %s WHERE try_variant_get(v1, '$.a', 'int') > 5",
                tableName))
        .containsExactly(row(2L, 10));
    assertThat(
            sql(
                "SELECT id, try_variant_get(v2, '$.x', 'int') FROM %s WHERE try_variant_get(v2, '$.x', 'int') < 100",
                tableName))
        .containsExactlyInAnyOrder(row(1L, 15), row(2L, 20));
  }

  @TestTemplate
  public void testSelectQueriesIndexSnapshot() {
    // Register a dedicated Spark catalog backed by an in-memory Iceberg catalog that implements
    // IndexCatalog so the read path can route the query to an index snapshot.
    String indexCatalogName = "index_cat_" + catalogName;
    spark.conf().set("spark.sql.catalog." + indexCatalogName, SparkCatalog.class.getName());
    spark
        .conf()
        .set(
            "spark.sql.catalog." + indexCatalogName + ".catalog-impl",
            InMemoryCatalog.class.getName());
    spark.conf().set("spark.sql.catalog." + indexCatalogName + ".cache-enabled", "false");

    String namespace = "db";
    String baseTable = indexCatalogName + "." + namespace + ".indexed";
    String indexDataTable = indexCatalogName + "." + namespace + ".index_data";

    try {
      sql("CREATE NAMESPACE IF NOT EXISTS %s.%s", indexCatalogName, namespace);

      // base table holding the original data; "category" (column id 3) is not covered by the index
      sql("CREATE TABLE %s (id bigint, data string, category string) USING iceberg", baseTable);
      sql("INSERT INTO %s VALUES (1, 'a', 'p'), (2, 'b', 'q'), (3, 'c', 'r')", baseTable);

      // separate table whose data files back the index snapshot (distinguishable rows). It only
      // contains the indexed columns (id, data).
      sql("CREATE TABLE %s (id bigint, data string) USING iceberg", indexDataTable);
      sql("INSERT INTO %s VALUES (4, 'x'), (5, 'y'), (6, 'z')", indexDataTable);

      Catalog icebergCatalog = Spark3Util.loadIcebergCatalog(spark, indexCatalogName);
      IndexCatalog indexCatalog = (IndexCatalog) icebergCatalog;

      TableIdentifier baseIdent = TableIdentifier.of(Namespace.of(namespace), "indexed");
      TableIdentifier indexDataIdent = TableIdentifier.of(Namespace.of(namespace), "index_data");

      Table base = icebergCatalog.loadTable(baseIdent);
      Table indexData = icebergCatalog.loadTable(indexDataIdent);

      long baseSnapshotId = base.currentSnapshot().snapshotId();
      String manifestList = indexData.currentSnapshot().manifestListLocation();

      // the index covers only the id (1) and data (2) columns
      IndexIdentifier indexIdent = IndexIdentifier.of(baseIdent, "data_index");
      indexCatalog
          .buildIndex(indexIdent)
          .withTableUuid(base.uuid())
          .withType(IndexType.BTREE)
          .withIndexColumnIds(2)
          .withOptimizedColumnIds(1)
          .create();

      indexCatalog
          .loadIndex(indexIdent)
          .addIndexSnapshot()
          .withTableSnapshotId(baseSnapshotId)
          .withIndexSnapshotId(1L)
          .withSnapshotProperty(IndexSnapshots.MANIFEST_LIST, manifestList)
          .commit();

      // querying only indexed columns is served from the index snapshot's data
      assertEquals(
          "Query using only indexed columns should be served from the index snapshot",
          ImmutableList.of(row(4L, "x"), row(5L, "y"), row(6L, "z")),
          sql("SELECT id, data FROM %s ORDER BY id", baseTable));

      // querying the unindexed "category" column falls back to the original table
      assertEquals(
          "Query using an unindexed column should be served from the original table",
          ImmutableList.of(row(1L, "a", "p"), row(2L, "b", "q"), row(3L, "c", "r")),
          sql("SELECT id, data, category FROM %s ORDER BY id", baseTable));
    } finally {
      sql("DROP TABLE IF EXISTS %s", baseTable);
      sql("DROP TABLE IF EXISTS %s", indexDataTable);
    }
  }

  @TestTemplate
  public void testSelectQueriesWithImmediateDataFileRead() throws IOException {
    // Register a dedicated Spark catalog backed by an in-memory Iceberg catalog that implements
    // IndexCatalog so the read path can route the query through an index snapshot.
    String indexCatalogName = "immediate_read_cat_" + catalogName;
    spark.conf().set("spark.sql.catalog." + indexCatalogName, SparkCatalog.class.getName());
    spark
        .conf()
        .set(
            "spark.sql.catalog." + indexCatalogName + ".catalog-impl",
            InMemoryCatalog.class.getName());
    spark.conf().set("spark.sql.catalog." + indexCatalogName + ".cache-enabled", "false");

    String namespace = "db";
    String baseTable = indexCatalogName + "." + namespace + ".indexed";

    try {
      sql("CREATE NAMESPACE IF NOT EXISTS %s.%s", indexCatalogName, namespace);

      // base table holding the original data; "category" (column id 3) is not covered by the index
      sql("CREATE TABLE %s (id bigint, data string, category string) USING iceberg", baseTable);
      sql("INSERT INTO %s VALUES (1, 'a', 'p'), (2, 'b', 'q'), (3, 'c', 'r')", baseTable);

      Catalog icebergCatalog = Spark3Util.loadIcebergCatalog(spark, indexCatalogName);
      IndexCatalog indexCatalog = (IndexCatalog) icebergCatalog;

      TableIdentifier baseIdent = TableIdentifier.of(Namespace.of(namespace), "indexed");
      Table base = icebergCatalog.loadTable(baseIdent);
      long baseSnapshotId = base.currentSnapshot().snapshotId();

      // build an index file that stores the optimized "id" column and maps each indexed row to its
      // position in the base data files via the _index_file_path and _index_pos metadata columns
      Schema indexSchema =
          new Schema(
              base.schema().findField("id"),
              MetadataColumns.INDEX_FILE_PATH,
              MetadataColumns.INDEX_ROW_POSITION);
      List<Record> indexRecords = Lists.newArrayList();
      try (CloseableIterable<FileScanTask> tasks = base.newScan().planFiles()) {
        for (FileScanTask task : tasks) {
          String dataFileLocation = task.file().location();
          long position = 0;
          try (CloseableIterable<Record> baseRows =
              Parquet.read(base.io().newInputFile(dataFileLocation))
                  .project(base.schema())
                  .createReaderFunc(
                      fileSchema -> GenericParquetReaders.buildReader(base.schema(), fileSchema))
                  .build()) {
            for (Record baseRow : baseRows) {
              Record record = GenericRecord.create(indexSchema);
              record.setField("id", baseRow.getField("id"));
              record.setField(MetadataColumns.INDEX_FILE_PATH.name(), dataFileLocation);
              record.setField(MetadataColumns.INDEX_ROW_POSITION.name(), position);
              indexRecords.add(record);
              position++;
            }
          }
        }
      }

      // an unpartitioned holder table provides a manifest list that references the index file
      TableIdentifier holderIdent = TableIdentifier.of(Namespace.of(namespace), "index_holder");
      Table holder =
          icebergCatalog.createTable(
              holderIdent,
              new Schema(Types.NestedField.required(1, "x", Types.StringType.get())),
              PartitionSpec.unpartitioned());
      DataFile indexDataFile =
          writeIndexFile(
              holder.io(),
              holder.location() + "/data/index-00000.parquet",
              indexSchema,
              indexRecords);
      holder.newAppend().appendFile(indexDataFile).commit();
      String manifestList = holder.currentSnapshot().manifestListLocation();

      // the index optimizes "id" (1) for filtering and records the data file location and row
      // position for each indexed row, so the read path can skip to the matching base rows;
      // "category" (3) is neither optimized nor indexed
      IndexIdentifier indexIdent = IndexIdentifier.of(baseIdent, "id_index");
      indexCatalog
          .buildIndex(indexIdent)
          .withTableUuid(base.uuid())
          .withType(IndexType.BTREE)
          .withIndexColumnIds(
              MetadataColumns.INDEX_FILE_PATH.fieldId(),
              MetadataColumns.INDEX_ROW_POSITION.fieldId())
          .withOptimizedColumnIds(1)
          .create();

      long indexSnapshotId = 1L;
      indexCatalog
          .loadIndex(indexIdent)
          .addIndexSnapshot()
          .withTableSnapshotId(baseSnapshotId)
          .withIndexSnapshotId(indexSnapshotId)
          .withSnapshotProperty(IndexSnapshots.MANIFEST_LIST, manifestList)
          .commit();

      this.scanEventCount = 0;
      this.lastScanEvent = null;

      // filtering on the optimized "id" column while projecting the unindexed "category" column:
      // the index locates the matching row, which is then read immediately from the base data file
      assertEquals(
          "Filtering on an optimized column while projecting an unindexed column should read the "
              + "row from the base data file through the index",
          ImmutableList.of(row(2L, "b", "q")),
          sql("SELECT id, data, category FROM %s WHERE id = 2 ORDER BY id", baseTable));

      assertThat(scanEventCount).as("Should create only one scan").isEqualTo(1);
      assertThat(lastScanEvent.snapshotId())
          .as("Scan should be routed through the index snapshot")
          .isEqualTo(indexSnapshotId);
      assertThat(lastScanEvent.snapshotId())
          .as("Scan should not use the base table snapshot")
          .isNotEqualTo(baseSnapshotId);

      sql("DROP TABLE IF EXISTS %s", indexCatalogName + "." + namespace + ".index_holder");
    } finally {
      sql("DROP TABLE IF EXISTS %s", baseTable);
      sql("DROP TABLE IF EXISTS %s", indexCatalogName + "." + namespace + ".index_holder");
    }
  }

  private static DataFile writeIndexFile(
      FileIO io, String location, Schema indexSchema, List<Record> records) throws IOException {
    OutputFile outputFile = io.newOutputFile(location);
    try (FileAppender<Record> appender =
        Parquet.write(outputFile)
            .schema(indexSchema)
            .createWriterFunc(GenericParquetWriter::create)
            .overwrite()
            .build()) {
      appender.addAll(records);
    }

    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(location)
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(io.newInputFile(location).getLength())
        .withRecordCount(records.size())
        .build();
  }
}

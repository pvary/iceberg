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
package org.apache.iceberg.flink.maintenance.operator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.parquet.format.NanoSeconds;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.flink.maintenance.operator.RewriteUtil.executeRewrite;
import static org.apache.iceberg.flink.maintenance.operator.RewriteUtil.planDataFileRewrite;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

class TestDataFileRewriteExecutorPerf {
  private static final Logger LOG = LoggerFactory.getLogger(TestDataFileRewriteExecutorPerf.class);

  private static final Path WAREHOUSE = Path.of("/Users/varypeter/tmp/warehouse");
  private static final int NUM_TESTS = 10;
  private static final int NUM_FILES = 10;
  private static final int NUM_ROWS = 20_000;
  private static final long SEED = 0;
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of("database", "tablename");
  private static final int SNAPSHOT_COUNT = 1;
  private static final Schema TEST_SCHEMA =
          new Schema(
                  required(1, "longCol", Types.LongType.get()),
                  required(2, "intCol", Types.IntegerType.get()),
                  required(3, "floatCol", Types.FloatType.get()),
                  optional(4, "doubleCol", Types.DoubleType.get()),
                  optional(5, "decimalCol", Types.DecimalType.of(20, 5)),
                  optional(6, "dateCol", Types.DateType.get()),
                  optional(7, "timestampCol", Types.TimestampType.withZone()),
                  optional(8, "stringCol1", Types.StringType.get()),
                  required(9, "stringCol2", Types.StringType.get()),
                  required(10, "stringCol3", Types.StringType.get()),
                  required(11, "stringCol4", Types.StringType.get()));

  private long startingSnapshot;

  @BeforeEach
  void storeTableState() {
    try {
      TableLoader loader = tableLoader();
      loader.open();
      Table table = loader.loadTable();
      if (table.history().size() > SNAPSHOT_COUNT) {
        table.manageSnapshots().rollbackTo(table.history().get(SNAPSHOT_COUNT).snapshotId()).commit();
      }
    } catch (Exception e) {

    }
  }

  @Test
  void testPerf() throws Exception {
    List<DataFileRewritePlanner.PlannedGroup> planned = planDataFileRewrite(tableLoader(), 10_000_000_000L);
    assertThat(planned).hasSize(1);

    long[] results = new long[NUM_TESTS];
    for (int i = 0 ; i<NUM_TESTS; ++i) {
      long start = System.nanoTime();
      List<DataFileRewriteExecutor.ExecutedGroup> actual = executeRewrite(planned);
      results[i] = (System.nanoTime() - start) / 1_000_000L;
      assertThat(actual).hasSize(1);
    }
    LOG.info("TIME TAKEN: " + Arrays.toString(results));
  }

  @Test
  void regenerateTable() throws IOException {
    if (WAREHOUSE.toFile().exists()) {
      Files.walk(WAREHOUSE).sorted(Comparator.reverseOrder()).map(java.nio.file.Path::toFile).forEach(File::delete);
    }

    Files.createDirectory(WAREHOUSE);

    Table table = catalogLoader().loadCatalog()
            .createTable(
            TABLE_IDENTIFIER,
            TEST_SCHEMA,
            PartitionSpec.unpartitioned(),
            null,
            ImmutableMap.of());

    GenericAppenderHelper helper = new GenericAppenderHelper(table, FileFormat.PARQUET, WAREHOUSE);
    DataFile[] dataFiles = new DataFile[NUM_FILES];
    for(int i = 0; i < NUM_FILES; ++i) {
      dataFiles[i] = helper.writeFile(RandomGenericData.generate(TEST_SCHEMA, NUM_ROWS, SEED));
    }

    helper.appendToTable(dataFiles);
  }

  private CatalogLoader catalogLoader() {
    return CatalogLoader.hadoop(
                    "hadoop",
                    new Configuration(),
                    ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, WAREHOUSE.toUri().toString()));

  }
  private TableLoader tableLoader() {
    return TableLoader.fromCatalog(catalogLoader(), TABLE_IDENTIFIER);
  }
}

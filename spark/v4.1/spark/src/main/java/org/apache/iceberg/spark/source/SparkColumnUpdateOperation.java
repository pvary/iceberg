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
package org.apache.iceberg.spark.source;

import java.util.List;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.DeltaWriteBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperation;
import org.apache.spark.sql.connector.write.RowLevelOperationInfo;
import org.apache.spark.sql.connector.write.SupportsColumnUpdate;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

class SparkColumnUpdateOperation implements RowLevelOperation, SupportsColumnUpdate {
  private final SparkSession spark;
  private final Table table;
  private final String branch;
  private final IsolationLevel isolationLevel;

  // lazy vars
  private ScanBuilder lazyScanBuilder;
  private Scan configuredScan;
  private DeltaWriteBuilder lazyWriteBuilder;

  SparkColumnUpdateOperation(
      SparkSession spark,
      Table table,
      String branch,
      RowLevelOperationInfo info,
      IsolationLevel isolationLevel) {
    // super(spark, table, branch, info, isolationLevel);
    this.spark = spark;
    this.table = table;
    this.branch = branch;
    this.isolationLevel = isolationLevel;
  }

  @Override
  public Command command() {
    return Command.UPDATE;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    if (lazyScanBuilder == null) {
      lazyScanBuilder =
          new SparkScanBuilder(spark, table, branch, options) {
            @Override
            public Scan build() {
              Scan scan = super.buildColumnUpdateScan();
              SparkColumnUpdateOperation.this.configuredScan = scan;
              return scan;
            }
          };
    }

    return lazyScanBuilder;
  }

  @Override
  public DeltaWriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    if (lazyWriteBuilder == null) {
      lazyWriteBuilder =
          new SparkColumnUpdateWriteBuilder(
              spark, configuredScan, table, branch, info, isolationLevel);
    }

    return lazyWriteBuilder;
  }

  @Override
  public NamedReference[] rowId() {
    return new NamedReference[] {
      SparkMetadataColumns.FILE_PATH.asRef(), SparkMetadataColumns.ROW_POSITION.asRef()
    };
  }

  @Override
  public NamedReference[] requiredMetadataAttributes() {
    List<NamedReference> metadataAttributes = Lists.newArrayList();
    metadataAttributes.add(SparkMetadataColumns.partition(table).asRef());

    return metadataAttributes.toArray(NamedReference[]::new);
  }
}

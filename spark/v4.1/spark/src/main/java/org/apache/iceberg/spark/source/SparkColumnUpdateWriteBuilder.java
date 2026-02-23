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
import java.util.Set;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.spark.SparkWriteConf;
import org.apache.iceberg.spark.SparkWriteRequirements;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.write.DeltaWrite;
import org.apache.spark.sql.connector.write.DeltaWriteBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperation;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkColumnUpdateWriteBuilder implements DeltaWriteBuilder {
  private final SparkSession spark;
  private final Table table;
  private final SparkWriteConf writeConf;
  private final LogicalWriteInfo writeInfo;
  private final StructType dsSchema;
  private SparkCopyOnWriteScan copyOnWriteScan = null;
  private IsolationLevel isolationLevel = null;

  SparkColumnUpdateWriteBuilder(
      SparkSession spark,
      Scan scan,
      Table table,
      String branch,
      LogicalWriteInfo info,
      IsolationLevel isolationLevel) {
    this.spark = spark;
    this.copyOnWriteScan = (SparkCopyOnWriteScan) scan;
    this.table = table;
    this.writeConf = new SparkWriteConf(spark, table, branch, info.options());
    this.writeInfo = info;
    this.dsSchema = info.schema();
    this.isolationLevel = isolationLevel;
  }

  @Override
  public DeltaWrite build() {
    Schema readSchema = validateOrMergeWriteSchema(table, dsSchema, writeConf);

    List<Integer> updatedFieldIds =
        readSchema.columns().stream().map(Types.NestedField::fieldId).toList();

    SparkUtil.validatePartitionTransforms(table.spec());

    return new SparkColumnUpdateWrite(
        spark,
        copyOnWriteScan,
        table,
        writeConf,
        writeInfo,
        spark.sparkContext().applicationId(),
        readSchema,
        dsSchema,
        writeRequirements(),
        updatedFieldIds);
  }

  // TODO gaborkaszab: check if this is needed
  private SparkWriteRequirements writeRequirements() {
    /* if (overwriteFiles) {
      return writeConf.copyOnWriteRequirements(copyOnWriteCommand);
    } else {
      return writeConf.writeRequirements();
    }*/
    return writeConf.copyOnWriteRequirements(RowLevelOperation.Command.UPDATE);
  }

  // TODO gaborkaszab: check if this is needed
  private static Schema validateOrMergeWriteSchema(
      Table table, StructType dsSchema, SparkWriteConf writeConf) {
    Schema writeSchema;
    boolean caseSensitive = writeConf.caseSensitive();
    if (writeConf.mergeSchema()) {
      // convert the dataset schema and assign fresh ids for new fields
      Schema newSchema =
          SparkSchemaUtil.convertWithFreshIds(table.schema(), dsSchema, caseSensitive);

      // update the table to get final id assignments and validate the changes
      UpdateSchema update =
          table.updateSchema().caseSensitive(caseSensitive).unionByNameWith(newSchema);
      Schema mergedSchema = update.apply();

      // reconvert the dsSchema without assignment to use the ids assigned by UpdateSchema
      writeSchema = SparkSchemaUtil.convert(mergedSchema, dsSchema, caseSensitive);

      TypeUtil.validateWriteSchema(
          mergedSchema, writeSchema, writeConf.checkNullability(), writeConf.checkOrdering());

      // if the validation passed, update the table schema
      update.commit();
    } else {
      writeSchema = SparkSchemaUtil.convert(table.schema(), dsSchema, caseSensitive);
      TypeUtil.validateWriteSchema(
          table.schema(), writeSchema, writeConf.checkNullability(), writeConf.checkOrdering());
    }

    return writeSchema;
  }

  // TODO gaborkaszab: filter the metadata cols too. What we need is the cols being updated and
  // FILE_PATH.
  private StructType filterSchemaForColumnUpdate(StructType schema, Set<String> updatedColumns) {
    // First try to get updated columns from thread-local context

    java.util.List<StructField> filteredFields = new java.util.ArrayList<>();
    for (StructField field : schema.fields()) {
      String fieldName = field.name();
      // Always keep metadata columns
      if (MetadataColumns.isMetadataColumn(fieldName)) {
        filteredFields.add(field);
      } else if (updatedColumns.contains(fieldName)) {
        // If we know the updated columns, keep them
        filteredFields.add(field);
      }
    }

    // If we ended up with only metadata columns and no data columns,
    // this means no updated columns were found - fall back to original schema
    boolean hasDataColumns =
        filteredFields.stream().anyMatch(f -> !MetadataColumns.isMetadataColumn(f.name()));
    if (!hasDataColumns) {
      return schema;
    }

    return new StructType(filteredFields.toArray(new StructField[0]));
  }
}

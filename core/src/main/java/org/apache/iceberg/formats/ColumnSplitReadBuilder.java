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
package org.apache.iceberg.formats;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.Types;

class ColumnSplitReadBuilder<X, T> implements ReadBuilder<X, T> {
  private final Map<ReadBuilder<X, ?>, List<Integer>> readBuilders;
  private final BiFunction<Schema, Integer[][], FormatModel.Combiner<X>> combinerBuilder;
  private Schema schema;

  ColumnSplitReadBuilder(
      Map<ReadBuilder<X, ?>, List<Integer>> readBuilders,
      BiFunction<Schema, Integer[][], FormatModel.Combiner<X>> newCombinerBuilder) {
    this.readBuilders = readBuilders;
    this.combinerBuilder = newCombinerBuilder;
  }

  // TODO gaborkaszab: test split reading when there are deletes too
  @Override
  public ReadBuilder<X, T> split(long newStart, long newLength) {
    readBuilders.entrySet().stream()
        .filter(entry -> entry.getValue().contains(MetadataColumns.ROW_POSITION.fieldId()))
        .findAny()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Cannot split read by row position because no read builder includes the row position column"))
        .getKey()
        .split(newStart, newLength);
    return this;
  }

  @Override
  public ReadBuilder<X, T> project(Schema newSchema) {
    // Filter out field IDs that are not in projected schema and remove ReadBuilders with no
    // remaining fields
    Iterator<Map.Entry<ReadBuilder<X, ?>, List<Integer>>> iterator =
        readBuilders.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<ReadBuilder<X, ?>, List<Integer>> entry = iterator.next();
      List<Types.NestedField> projectedFields =
          entry.getValue().stream()
              .map(newSchema::findField)
              .filter(Objects::nonNull)
              .collect(Collectors.toList());

      if (projectedFields.isEmpty()) {
        iterator.remove();
      } else {
        entry.setValue(
            projectedFields.stream().map(Types.NestedField::fieldId).collect(Collectors.toList()));
        entry.getKey().project(new Schema(projectedFields));
      }
    }

    this.schema = newSchema;
    return this;
  }

  @Override
  public ReadBuilder<X, T> engineProjection(T engineSchema) {
    // Engine projection is not supported for combined reads
    return this;
  }

  @Override
  public ReadBuilder<X, T> caseSensitive(boolean caseSensitive) {
    readBuilders.keySet().forEach(r -> r.caseSensitive(caseSensitive));
    return this;
  }

  @Override
  public ReadBuilder<X, T> filter(Expression newFilter) {
    readBuilders.keySet().forEach(r -> r.filter(newFilter));
    return this;
  }

  @Override
  public ReadBuilder<X, T> set(String key, String value) {
    readBuilders.keySet().forEach(r -> r.set(key, value));
    return this;
  }

  @Override
  public ReadBuilder<X, T> reuseContainers() {
    readBuilders.keySet().forEach(ReadBuilder::reuseContainers);
    return this;
  }

  @Override
  public ReadBuilder<X, T> recordsPerBatch(int rowsPerBatch) {
    readBuilders.keySet().forEach(r -> r.recordsPerBatch(rowsPerBatch));
    return this;
  }

  @Override
  public ReadBuilder<X, T> idToConstant(Map<Integer, ?> idToConstant) {
    readBuilders.keySet().forEach(r -> r.idToConstant(idToConstant));
    return this;
  }

  @Override
  public ReadBuilder<X, T> withNameMapping(NameMapping newNameMapping) {
    readBuilders.keySet().forEach(r -> r.withNameMapping(newNameMapping));
    return this;
  }

  @Override
  public CloseableIterable<X> build() {
    FormatModel.Combiner<X> combiner =
        combinerBuilder.apply(
            schema,
            readBuilders.values().stream()
                .map(list -> list.toArray(new Integer[0]))
                .toArray(Integer[][]::new));
    return FormatModel.combiner(
        readBuilders.keySet().stream().map(ReadBuilder::build).collect(Collectors.toList()),
        combiner,
        false);
  }
}

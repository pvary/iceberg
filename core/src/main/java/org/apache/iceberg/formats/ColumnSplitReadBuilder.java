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

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mapping.NameMapping;

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

  @Override
  public ReadBuilder<X, T> split(long newStart, long newLength) {
    // throw new UnsupportedOperationException("Not supported");
    // TODO gaborkaszab: currently this doesn't do anything
    return this;
  }

  @Override
  public ReadBuilder<X, T> project(Schema newSchema) {
    // TODO gaborkaszab: verify all fields in newSchema are covered?
    readBuilders.forEach(
        (builder, columnIndices) ->
            builder.project(
                new Schema(
                    columnIndices.stream()
                        .map(newSchema::findField)
                        .collect(Collectors.toList()))));
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
    readBuilders.keySet().forEach(builder -> builder.caseSensitive(caseSensitive));
    return this;
  }

  @Override
  public ReadBuilder<X, T> filter(Expression newFilter) {
    readBuilders.keySet().forEach(builder -> builder.filter(newFilter));
    return this;
  }

  @Override
  public ReadBuilder<X, T> set(String key, String value) {
    readBuilders.keySet().forEach(builder -> builder.set(key, value));
    return this;
  }

  @Override
  public ReadBuilder<X, T> reuseContainers() {
    readBuilders.keySet().forEach(ReadBuilder::reuseContainers);
    return this;
  }

  @Override
  public ReadBuilder<X, T> recordsPerBatch(int rowsPerBatch) {
    readBuilders.keySet().forEach(builder -> builder.recordsPerBatch(rowsPerBatch));
    return this;
  }

  @Override
  public ReadBuilder<X, T> idToConstant(Map<Integer, ?> idToConstant) {
    readBuilders.keySet().forEach(builder -> builder.idToConstant(idToConstant));
    return this;
  }

  @Override
  public ReadBuilder<X, T> withNameMapping(NameMapping newNameMapping) {
    readBuilders.keySet().forEach(builder -> builder.withNameMapping(newNameMapping));
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

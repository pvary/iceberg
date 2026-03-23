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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Pair;

class ColumnSplitWriteBuilder<X, T> implements ModelWriteBuilder<X, T> {
  private final Map<ModelWriteBuilder<X, T>, List<Integer>> writeBuilders;
  private final BiFunction<Schema, Integer[], FormatModel.Narrower<X>> narrowerBuilder;
  private Schema schema;
  private boolean multiThreaded = false;

  ColumnSplitWriteBuilder(
      Map<ModelWriteBuilder<X, T>, List<Integer>> writeBuilders,
      BiFunction<Schema, Integer[], FormatModel.Narrower<X>> narrowerBuilder) {
    this.writeBuilders = writeBuilders;
    this.narrowerBuilder = narrowerBuilder;
  }

  @Override
  public ColumnSplitWriteBuilder<X, T> schema(Schema newSchema) {
    writeBuilders.forEach(
        (builder, columnIds) ->
            builder.schema(
                new Schema(
                    columnIds.stream().map(newSchema::findField).collect(Collectors.toList()))));
    this.schema = newSchema;
    return this;
  }

  @Override
  public ColumnSplitWriteBuilder<X, T> engineSchema(T newEngineSchema) {
    // TODO: we might want to narrow the engine schema as well
    writeBuilders.keySet().forEach(builder -> builder.engineSchema(newEngineSchema));
    return this;
  }

  @Override
  public ColumnSplitWriteBuilder<X, T> set(String property, String value) {
    if (FormatModel.MULTI_THREADED.equals(property)) {
      this.multiThreaded = Boolean.parseBoolean(value);
    }

    writeBuilders.keySet().forEach(builder -> builder.set(property, value));
    return this;
  }

  @Override
  public ColumnSplitWriteBuilder<X, T> meta(String property, String value) {
    writeBuilders.keySet().forEach(builder -> builder.meta(property, value));
    return this;
  }

  @Override
  public ColumnSplitWriteBuilder<X, T> content(FileContent content) {
    writeBuilders.keySet().forEach(builder -> builder.content(content));
    return this;
  }

  @Override
  public ColumnSplitWriteBuilder<X, T> metricsConfig(MetricsConfig newMetricsConfig) {
    writeBuilders.keySet().forEach(builder -> builder.metricsConfig(newMetricsConfig));
    return this;
  }

  @Override
  public ColumnSplitWriteBuilder<X, T> overwrite() {
    writeBuilders.keySet().forEach(ModelWriteBuilder::overwrite);
    return this;
  }

  @Override
  public ColumnSplitWriteBuilder<X, T> withFileEncryptionKey(ByteBuffer encryptionKey) {
    writeBuilders.keySet().forEach(builder -> builder.withFileEncryptionKey(encryptionKey));
    return this;
  }

  @Override
  public ColumnSplitWriteBuilder<X, T> withAADPrefix(ByteBuffer aadPrefix) {
    writeBuilders.keySet().forEach(builder -> builder.withAADPrefix(aadPrefix));
    return this;
  }

  @Override
  public FileAppender<X> build() throws IOException {
    List<Pair<FileAppender<X>, FormatModel.Narrower<X>>> appenders =
        Lists.newArrayListWithCapacity(writeBuilders.size());
    for (Map.Entry<ModelWriteBuilder<X, T>, List<Integer>> entry : writeBuilders.entrySet()) {
      Integer[] columnIds = entry.getValue().toArray(new Integer[0]);
      appenders.add(Pair.of(entry.getKey().build(), narrowerBuilder.apply(schema, columnIds)));
    }

    return FormatModel.narrower(appenders, multiThreaded);
  }
}

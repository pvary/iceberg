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
package org.apache.iceberg.io.datafile;

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Base implementation for the {@link DataWriterBuilder} which handles the common attributes for the
 * builders. FileFormat implementations should extend this for creating their own data writer
 * builders. Uses an embedded {@link AppenderBuilder} to actually write the records.
 *
 * @param <T> the type of the builder for chaining
 */
public class DataWriterBuilderBase<T extends DataWriterBuilderBase<T>>
    implements DataWriterBuilder<T> {
  private final AppenderBuilder<?> appenderBuilder;
  private final FileFormat format;
  private PartitionSpec spec;
  private StructLike partition;
  private EncryptionKeyMetadata keyMetadata = null;
  private SortOrder sortOrder;

  protected DataWriterBuilderBase(AppenderBuilder<?> appenderBuilder, FileFormat format) {
    this.appenderBuilder = appenderBuilder;
    this.format = format;
  }

  @Override
  public T forTable(Table table) {
    schema(table.schema());
    withSpec(table.spec());
    setAll(table.properties());
    metricsConfig(org.apache.iceberg.MetricsConfig.forTable(table));
    return (T) this;
  }

  @Override
  public T schema(Schema newSchema) {
    appenderBuilder.schema(newSchema);
    return (T) this;
  }

  @Override
  public T set(String property, String value) {
    appenderBuilder.set(property, value);
    return (T) this;
  }

  @Override
  public T setAll(Map<String, String> properties) {
    appenderBuilder.setAll(properties);
    return (T) this;
  }

  @Override
  public T meta(String property, String value) {
    appenderBuilder.meta(property, value);
    return (T) this;
  }

  @Override
  public T meta(Map<String, String> properties) {
    appenderBuilder.meta(properties);
    return (T) this;
  }

  @Override
  public T overwrite() {
    return overwrite(true);
  }

  @Override
  public T overwrite(boolean enabled) {
    appenderBuilder.overwrite(enabled);
    return (T) this;
  }

  @Override
  public T metricsConfig(MetricsConfig newMetricsConfig) {
    appenderBuilder.metricsConfig(newMetricsConfig);
    return (T) this;
  }

  @Override
  public T withSpec(PartitionSpec newSpec) {
    this.spec = newSpec;
    return (T) this;
  }

  @Override
  public T withPartition(StructLike newPartition) {
    this.partition = newPartition;
    return (T) this;
  }

  @Override
  public T withKeyMetadata(EncryptionKeyMetadata metadata) {
    this.keyMetadata = metadata;
    return (T) this;
  }

  @Override
  public T withSortOrder(SortOrder newSortOrder) {
    this.sortOrder = newSortOrder;
    return (T) this;
  }

  @Override
  public String location() {
    return appenderBuilder().location();
  }

  @Override
  public Schema schema() {
    return appenderBuilder().schema();
  }

  protected AppenderBuilder<?> appenderBuilder() {
    return appenderBuilder;
  }

  protected FileFormat format() {
    return format;
  }

  protected PartitionSpec spec() {
    return spec;
  }

  protected StructLike partition() {
    return partition;
  }

  protected EncryptionKeyMetadata keyMetadata() {
    return keyMetadata;
  }

  protected SortOrder sortOrder() {
    return sortOrder;
  }

  @Override
  public <D> DataWriter<D> build() throws IOException {
    Preconditions.checkArgument(spec() != null, "Cannot create data writer without spec");
    Preconditions.checkArgument(
        spec().isUnpartitioned() || partition() != null,
        "Partition must not be null when creating data writer for partitioned spec");

    return new DataWriter<>(
        appenderBuilder().build(),
        format(),
        appenderBuilder().location(),
        spec(),
        partition(),
        keyMetadata(),
        sortOrder());
  }
}

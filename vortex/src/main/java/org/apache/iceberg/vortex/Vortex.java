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
package org.apache.iceberg.vortex;

import dev.vortex.api.DType;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.InternalData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.vortex.GenericVortexReader;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.datafile.DataFileServiceRegistry;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Entrypoint to working with Vortex {@link FileFormat formatted} content files. */
public final class Vortex {
  private Vortex() {}

  public static void register() {
    // Register generic Vortex reader.
    DataFileServiceRegistry.registerReader(
        FileFormat.VORTEX,
        Record.class.getName(),
        inputFile -> read(inputFile).readerFunction(GenericVortexReader::buildReader));
  }

  public static ReadBuilder read(InputFile inputFile) {
    return new ReadBuilder(inputFile);
  }

  public interface ReaderFunction<R> {
    VortexRowReader<R> read(Schema schema, DType fileSchema, Map<Integer, ?> idToConstant);
  }

  public static final class ReadBuilder
      implements InternalData.ReadBuilder,
          org.apache.iceberg.io.datafile.ReadBuilder<ReadBuilder, Object> {
    private final InputFile inputFile;
    private Schema schema;
    private ReaderFunction<?> readerFunction;
    private Map<Integer, ?> idToConstant;

    ReadBuilder(InputFile inputFile) {
      this.inputFile = inputFile;
    }

    @Override
    public ReadBuilder project(Schema projectedSchema) {
      this.schema = projectedSchema;
      return this;
    }

    @Override
    public ReadBuilder set(String key, String value) {
      // No-op.
      return this;
    }

    @Override
    public ReadBuilder split(long newStart, long newLength) {
      // TODO(aduffy): support splitting, need to expose split size configuration over the Rust FFI.
      throw new UnsupportedOperationException("Splitting is not supported yet.");
    }

    @Override
    public ReadBuilder reuseContainers() {
      // No-op.
      return this;
    }

    @Override
    public ReadBuilder reuseContainers(boolean newReuseContainers) {
      // This is a no-op.
      return this;
    }

    @Override
    public ReadBuilder idToConstant(Map<Integer, ?> newIdConstant) {
      this.idToConstant = newIdConstant;
      return this;
    }

    @Override
    public ReadBuilder withNameMapping(NameMapping newNameMapping) {
      // TODO(aduffy): is this for field renames? Figure out how to patch this through.
      return this;
    }

    @Override
    public InternalData.ReadBuilder setRootType(Class<? extends StructLike> rootClass) {
      throw new UnsupportedOperationException("Custom types are not supported for Vortex.");
    }

    @Override
    public InternalData.ReadBuilder setCustomType(
        int fieldId, Class<? extends StructLike> structClass) {
      throw new UnsupportedOperationException("Custom types are not supported for Vortex");
    }

    public <D> ReadBuilder readerFunction(ReaderFunction<D> newReaderFunc) {
      Preconditions.checkState(
          readerFunction == null, "Cannot set multiple read builder functions");
      this.readerFunction = newReaderFunc;
      return this;
    }

    @Override
    public <D> CloseableIterable<D> build() {
      Preconditions.checkState(schema != null, "schema must be configured");
      Preconditions.checkState(readerFunction != null, "readerFunction must be configured");
      return new VortexIterable<>(
          inputFile,
          fileSchema -> (VortexRowReader<D>) readerFunction.read(schema, fileSchema, idToConstant));
    }
  }
}

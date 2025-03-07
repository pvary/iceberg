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

import dev.vortex.api.Array;
import dev.vortex.api.ArrayIterator;
import dev.vortex.api.DType;
import dev.vortex.api.File;
import dev.vortex.api.Files;
import dev.vortex.api.ScanOptions;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VortexIterable<T> extends CloseableGroup implements CloseableIterable<T> {
  private static final Logger LOG = LoggerFactory.getLogger(VortexIterable.class);

  private final InputFile inputFile;
  private final Optional<Expression> filterPredicate;
  private final long[] rowRange;
  private final Function<DType, VortexRowReader<T>> rowReaderFunc;
  private final Function<DType, VortexBatchReader<T>> batchReaderFunction;
  private final List<String> projection;

  VortexIterable(
      InputFile inputFile,
      Schema icebergSchema,
      Optional<Expression> filterPredicate,
      long[] rowRange,
      Function<DType, VortexRowReader<T>> readerFunction,
      Function<DType, VortexBatchReader<T>> batchReaderFunction) {
    this.inputFile = inputFile;
    // We have the file schema, we need to assign Iceberg IDs to the entire file schema
    this.projection = Lists.transform(icebergSchema.columns(), Types.NestedField::name);
    this.filterPredicate = filterPredicate;
    this.rowRange = rowRange;
    this.rowReaderFunc = readerFunction;
    this.batchReaderFunction = batchReaderFunction;
  }

  @Override
  public CloseableIterator<T> iterator() {
    File vortexFile = newVortexFile(inputFile);

    // Return the filtered scan, and then the projection, etc.
    Optional<dev.vortex.api.Expression> scanPredicate =
        filterPredicate.map(
            icebergExpression -> {
              Schema fileSchema = VortexSchemas.convert(vortexFile.getDType());
              return ConvertFilterToVortex.convert(fileSchema, icebergExpression);
            });

    Optional<long[]> optRange = Optional.ofNullable(this.rowRange);

    ArrayIterator batchStream =
        vortexFile.newScan(
            ScanOptions.builder()
                .addAllColumns(projection)
                .predicate(scanPredicate)
                .rowRange(optRange)
                .build());
    Preconditions.checkNotNull(batchStream, "batchStream");

    DType dtype = batchStream.getDataType();
    CloseableIterator<Array> wrappedIterator =
        new CloseableIterator<Array>() {
          @Override
          public void close() {
            batchStream.close();
            vortexFile.close();
          }

          @Override
          public boolean hasNext() {
            return batchStream.hasNext();
          }

          @Override
          public Array next() {
            return batchStream.next();
          }
        };

    if (rowReaderFunc != null) {
      VortexRowReader<T> rowFunction = rowReaderFunc.apply(dtype);
      return new VortexRowIterator<>(wrappedIterator, rowFunction);
    } else {
      VortexBatchReader<T> batchTransform = batchReaderFunction.apply(dtype);
      return CloseableIterator.transform(wrappedIterator, batchTransform::read);
    }
  }

  private static File newVortexFile(InputFile inputFile) {
    LOG.debug("opening Vortex file: {}", inputFile);

    URI uriLocation = URI.create(VortexFileUtil.resolveUri(inputFile.location()));
    Map<String, String> properties = VortexFileUtil.resolveInputProperties(inputFile);
    return Files.open(uriLocation, properties);
  }

  static class VortexRowIterator<T> extends CloseableGroup implements CloseableIterator<T> {
    private final CloseableIterator<Array> stream;
    private final VortexRowReader<T> rowReader;

    private Array currentBatch = null;
    private int batchIndex = 0;
    private int batchLen = 0;

    VortexRowIterator(CloseableIterator<Array> stream, VortexRowReader<T> rowReader) {
      this.stream = stream;
      addCloseable(stream);
      this.rowReader = rowReader;
      if (stream.hasNext()) {
        currentBatch = stream.next();
        batchLen = (int) currentBatch.getLen();
      }
    }

    @Override
    public void close() throws IOException {
      // Do not close the ArrayStream, it is closed by the parent.
      currentBatch.close();
      currentBatch = null;
    }

    @Override
    public boolean hasNext() {
      // See if we need to fill a new batch first.
      if (currentBatch == null || batchIndex == batchLen) {
        advance();
      }

      return currentBatch != null;
    }

    @Override
    public T next() {
      T nextRow = rowReader.read(currentBatch, batchIndex);
      batchIndex++;
      return nextRow;
    }

    private void advance() {
      if (stream.hasNext()) {
        currentBatch = stream.next();
        batchIndex = 0;
        batchLen = (int) currentBatch.getLen();
      } else {
        currentBatch = null;
        batchLen = 0;
      }
    }
  }
}

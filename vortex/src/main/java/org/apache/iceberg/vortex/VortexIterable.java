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
import dev.vortex.api.ArrayStream;
import dev.vortex.api.DType;
import dev.vortex.api.ScanOptions;
import dev.vortex.impl.NativeFile;
import java.io.IOException;
import java.util.function.Function;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;

public class VortexIterable<T> extends CloseableGroup implements CloseableIterable<T> {
  private final InputFile inputFile;
  private final Function<DType, VortexRowReader<T>> rowReaderFunc;

  VortexIterable(InputFile inputFile, Function<DType, VortexRowReader<T>> readerFunction) {
    this.inputFile = inputFile;
    this.rowReaderFunc = readerFunction;
  }

  @Override
  public CloseableIterator<T> iterator() {
    NativeFile vortexFile = NativeFile.open(inputFile.location());
    addCloseable(vortexFile);

    DType fileType = vortexFile.getDType();
    addCloseable(fileType);

    ArrayStream batchStream = vortexFile.newScan(ScanOptions.of());
    return new ArrayStreamIterator<>(batchStream, rowReaderFunc.apply(fileType));
  }

  static class ArrayStreamIterator<T> implements CloseableIterator<T> {
    private final ArrayStream stream;
    private final VortexRowReader<T> rowReader;

    private Array currentBatch = null;
    private int batchIndex = 0;
    private int batchLen = 0;

    ArrayStreamIterator(ArrayStream stream, VortexRowReader<T> rowReader) {
      this.stream = stream;
      this.rowReader = rowReader;
      if (stream.next()) {
        currentBatch = stream.getCurrent();
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
      if (stream.next()) {
        currentBatch = stream.getCurrent();
        batchIndex = 0;
        batchLen = (int) currentBatch.getLen();
      } else {
        currentBatch = null;
        batchLen = 0;
      }
    }
  }
}

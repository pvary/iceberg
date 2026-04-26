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
package org.apache.iceberg.index;

import java.io.IOException;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

/**
 * Format-agnostic API for an inverted-index file that maps a key {@link Record} to the {@code
 * (filePath, pos)} location of the row that originally produced it.
 *
 * <p>Implementations are bound to a key {@link org.apache.iceberg.Schema} at construction time
 * (e.g. {@code MinimalPerfectHashFunctionIndexHandler.create(keySchema)}); the returned handler is
 * then used as a factory for {@link Writer} / {@link Reader} instances bound to a concrete {@link
 * OutputFile} / {@link InputFile}. Every {@link Record} passed to the writer or reader must match
 * that key schema.
 */
public interface IndexHandler {

  /** Opens a {@link Writer} that will materialize the index into {@code output} on close. */
  Writer writer(OutputFile output);

  /** Opens a {@link Reader} backed by {@code input}. The reader holds the input stream open. */
  Reader reader(InputFile input) throws IOException;

  /**
   * Hint, in bytes, for the storage adapter's first wire read size (e.g. ADLS {@code
   * adls.read.block-size-bytes}). When non-{@code null}, callers should configure their {@link
   * org.apache.iceberg.io.FileIO FileIO} so a single bounded {@code RangeReadable.readFully} issued
   * by this handler's {@link Reader} fits in one HTTP round-trip without dragging in the adapter's
   * much larger default block (4 MB on ADLS).
   *
   * <p>Returning {@code null} (the default) means the implementation has no opinion and the
   * adapter's defaults should be used.
   */
  default Integer recommendedReadBlockSize() {
    return null;
  }

  /**
   * Buffers entries and writes them as an index file when {@link #close()} is called. Each {@code
   * key} added must be unique within a single writer instance.
   */
  interface Writer extends AutoCloseable {
    /**
     * Adds a single entry to the index.
     *
     * @param key the lookup key, copied as-is
     * @param filePath the source file path the entry was produced from
     * @param pos the row position within {@code filePath}
     */
    void add(Record key, String filePath, long pos);
  }

  /** Resolves a key to its {@link Hit} (or {@code null} if the key is not present). */
  interface Reader extends AutoCloseable {
    /**
     * Looks up {@code key} in the index.
     *
     * @return the matching {@link Hit}, or {@code null} if {@code key} is not present
     * @throws IOException if reading the underlying file fails
     */
    Hit lookup(Record key) throws IOException;
  }

  /** Result of a successful {@link Reader#lookup(Record)}. */
  interface Hit {
    /** Source file path the matched row was produced from. */
    String filePath();

    /** Row position within {@link #filePath()}. */
    long pos();
  }

  record HitImpl(String filePath, long pos) implements IndexHandler.Hit {}
}

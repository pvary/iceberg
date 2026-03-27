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
import java.util.function.BiFunction;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.InputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface that provides a unified abstraction for converting between data file formats and
 * input/output data representations.
 *
 * <p>{@link FormatModel} serves as a bridge between storage formats ({@link FileFormat}) and
 * expected input/output data structures, optimizing performance through direct conversion without
 * intermediate representations. File format implementations handle the low-level parsing details
 * while the object model determines the in-memory representation used for the parsed data.
 * Together, these provide a consistent API for consuming data files while optimizing for specific
 * processing engines.
 *
 * <p>Iceberg provides some built-in object models and processing engines can implement custom
 * object models to integrate with Iceberg's file reading and writing capabilities.
 *
 * @param <D> output type used for reading data, and input type for writing data and deletes
 * @param <S> the type of the schema for the input/output data
 */
public interface FormatModel<D, S> {
  Logger LOG = LoggerFactory.getLogger(FormatModel.class);

  /** Property key to enable multi-threaded reading and writing for column splits. */
  String MULTI_THREADED = "multi-threaded";

  /** Property key for the number of elements per batch for column split reading and writing. */
  String BATCH_SIZE = "batch-size";

  /**
   * Property key for the number of batches each queue can hold for column split reading and
   * writing.
   */
  String QUEUE_CAPACITY = "queue-capacity";

  int DEFAULT_BATCH_SIZE = 1024;
  int DEFAULT_QUEUE_CAPACITY = 16;

  /** The file format which is read/written by the object model. */
  FileFormat format();

  /**
   * Return the row type class for the object model implementation processed by this factory.
   *
   * <p>The model types act as a contract specifying the expected data structures for both reading
   * (converting file formats into output objects) and writing (converting input objects into file
   * formats). This ensures proper integration between Iceberg's storage layer and processing
   * engines.
   *
   * <p>Processing engines can define their own object models by implementing this interface and
   * using their own model name. They can register these models with Iceberg by using the {@link
   * FormatModelRegistry}. This allows custom data representations to be seamlessly integrated with
   * Iceberg's file format handlers.
   *
   * @return the type of the data structures handled by this model implementation
   */
  Class<? extends D> type();

  /**
   * Return the schema type class for the object model implementation processed by this factory.
   *
   * @return the type of the schema for the data structures handled by this model implementation
   */
  Class<S> schemaType();

  /**
   * Creates a writer builder for data files.
   *
   * <p>The returned {@link ModelWriteBuilder} configures and creates a writer that converts input
   * objects into the file format supported by this factory.
   *
   * @param outputFile destination for the written data
   * @return configured writer builder
   */
  ModelWriteBuilder<D, S> writeBuilder(EncryptedOutputFile outputFile);

  /**
   * Creates a file reader builder for the specified input file.
   *
   * <p>The returned {@link ReadBuilder} configures and creates a reader that converts data from the
   * file format into output objects supported by this factory.
   *
   * @param inputFile source file to read from
   * @return configured reader builder for the specified input
   */
  ReadBuilder<D, S> readBuilder(InputFile inputFile);

  interface Combiner<E> {
    E combine(List<E> elements);

    /**
     * Shallow-copies the values from {@code source} into {@code target}, returning {@code target}.
     * Used by multi-threaded readers to snapshot reused containers into pre-allocated batch slots
     * on the producer thread, before the underlying reader overwrites them.
     *
     * <p>When {@code reuseContainers} is false, the default returns {@code source} as-is (each
     * element is already a distinct object, no copy needed).
     *
     * @param source the element to copy from (may be a reused container)
     * @param target a pre-allocated element to copy into, or {@code null} on first use
     * @return the element to store in the batch
     */
    default E copyInto(E source, E target) {
      return source;
    }
  }

  @FunctionalInterface
  interface CombinerBuilderFunction<E> {
    Combiner<E> build(Schema schema, Integer[][] families, boolean reuseContainers);
  }

  interface Narrower<E> {
    E narrow(E elements);
  }

  default CombinerBuilderFunction<D> combinerBuilder() {
    throw new UnsupportedOperationException("Not implemented");
  }

  default BiFunction<Schema, Integer[], Narrower<D>> narrowerBuilder() {
    throw new UnsupportedOperationException("Not implemented");
  }
}

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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.vortex.GenericVortexReader;
import org.apache.iceberg.data.vortex.GenericVortexWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.InputFileResolver;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies reads of a data file that continues into files which are not planned by the scan: the
 * reader must open them through the caller-supplied {@link InputFileResolver}.
 */
public class TestVortexAdditionalFiles {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  private static final int ROWS = 10;
  private static final int ADDITIONAL_FILES = 5;

  @TempDir private Path temp;

  @Test
  public void readsRowsFromAdditionalFiles() throws IOException {
    InputFile file = writeRows();
    writeAdditionalFiles(file, ADDITIONAL_FILES);

    List<String> resolved = Lists.newArrayList();
    InputFileResolver resolver =
        (location, length, keyMetadata) -> {
          resolved.add(location);
          return Files.localInput(location);
        };

    List<Integer> ids = readIds(file, resolver, ADDITIONAL_FILES);

    assertThat(resolved)
        .as("Every file the data file continues into must be opened through the resolver")
        .containsExactly(
            file.location() + ".1",
            file.location() + ".2",
            file.location() + ".3",
            file.location() + ".4",
            file.location() + ".5");
    assertThat(ids)
        .as("The scan must return the rows of the data file and of every file it continues into")
        .hasSize(ROWS * (ADDITIONAL_FILES + 1));
    for (int id = 0; id < ROWS; id += 1) {
      int expectedId = id;
      assertThat(ids.stream().filter(value -> value == expectedId).count())
          .isEqualTo(ADDITIONAL_FILES + 1);
    }
  }

  @Test
  public void readsOnlyTheDataFileByDefault() throws IOException {
    InputFile file = writeRows();
    writeAdditionalFiles(file, ADDITIONAL_FILES);

    assertThat(readIds(file, null, null))
        .as("Additional files must not be read unless the reader is configured to")
        .hasSize(ROWS);
  }

  @Test
  public void failsWhenTheResolverIsMissing() throws IOException {
    InputFile file = writeRows();
    writeAdditionalFiles(file, ADDITIONAL_FILES);

    assertThatThrownBy(() -> readIds(file, null, ADDITIONAL_FILES))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("requires a file resolver");
  }

  private List<Integer> readIds(InputFile file, InputFileResolver resolver, Integer additionalFiles)
      throws IOException {
    var builder = formatModel().readBuilder(file).project(SCHEMA);
    if (resolver != null) {
      builder = builder.fileResolver(resolver);
    }

    if (additionalFiles != null) {
      builder =
          builder.set(VortexFormatModel.READ_ADDITIONAL_FILES, String.valueOf(additionalFiles));
    }

    List<Integer> ids = Lists.newArrayList();
    try (CloseableIterable<Record> reader = builder.build()) {
      for (Record record : reader) {
        ids.add((Integer) record.getField("id"));
      }
    }

    return ids;
  }

  private InputFile writeRows() throws IOException {
    OutputFile outputFile =
        Files.localOutput(temp.resolve("data-" + System.nanoTime() + ".vortex").toFile());
    List<Record> records = Lists.newArrayListWithCapacity(ROWS);
    for (int i = 0; i < ROWS; i++) {
      GenericRecord record = GenericRecord.create(SCHEMA);
      record.setField("id", i);
      record.setField("data", "val-" + i);
      records.add(record);
    }

    try (FileAppender<Record> appender =
        formatModel()
            .writeBuilder(EncryptedFiles.plainAsEncryptedOutput(outputFile))
            .schema(SCHEMA)
            .content(FileContent.DATA)
            .build()) {
      appender.addAll(records);
    }

    return outputFile.toInputFile();
  }

  /** Copies the data file to the locations the reader derives from it. */
  private void writeAdditionalFiles(InputFile file, int count) throws IOException {
    for (int index = 1; index <= count; index += 1) {
      java.nio.file.Files.copy(
          Path.of(file.location()),
          Path.of(file.location() + "." + index),
          StandardCopyOption.REPLACE_EXISTING);
    }
  }

  private static VortexFormatModel<Record, StructType, VortexRowReader<?>> formatModel() {
    return VortexFormatModel.create(
        Record.class,
        StructType.class,
        (icebergSchema, fileSchema, engineSchema) -> GenericVortexWriter.buildWriter(icebergSchema),
        (VortexFormatModel.ReaderFunction<Record>) GenericVortexReader::buildReader);
  }
}

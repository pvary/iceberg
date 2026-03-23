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
package org.apache.iceberg.data;

import static org.apache.iceberg.MetadataColumns.DELETE_FILE_PATH;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_POS;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.formats.FileWriterBuilder;
import org.apache.iceberg.formats.FormatModel;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

public class TestGenericFormatModels {
  private static final List<Record> TEST_RECORDS =
      RandomGenericData.generate(TestBase.SCHEMA, 10, 1L);

  private static final List<Record> LARGE_TEST_RECORDS =
      RandomGenericData.generate(TestBase.SCHEMA, 5000, 2L);

  private static final FileFormat[] FILE_FORMATS =
      new FileFormat[] {FileFormat.AVRO, FileFormat.PARQUET, FileFormat.ORC};

  private static final boolean[] MULTI_THREADED = new boolean[] {false, true};

  @TempDir protected Path temp;

  private InMemoryFileIO fileIO;
  private EncryptedOutputFile encryptedFile;

  @BeforeEach
  public void before() {
    this.fileIO = new InMemoryFileIO();
    this.encryptedFile =
        EncryptedFiles.encryptedOutput(
            fileIO.newOutputFile("test-file"), EncryptionKeyMetadata.EMPTY);
  }

  @AfterEach
  public void after() throws IOException {
    if (fileIO.fileExists(encryptedFile.encryptingOutputFile().location())) {
      fileIO.deleteFile(encryptedFile.encryptingOutputFile());
    }
    this.encryptedFile = null;
    if (fileIO != null) {
      fileIO.close();
    }
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  public void testDataWriterRoundTrip(FileFormat fileFormat) throws IOException {
    FileWriterBuilder<DataWriter<Record>, Schema> writerBuilder =
        FormatModelRegistry.dataWriteBuilder(fileFormat, Record.class, encryptedFile);

    DataFile dataFile;
    DataWriter<Record> writer =
        writerBuilder.schema(TestBase.SCHEMA).spec(PartitionSpec.unpartitioned()).build();
    try (writer) {
      for (Record record : TEST_RECORDS) {
        writer.write(record);
      }
    }

    dataFile = writer.toDataFile();

    assertThat(dataFile).isNotNull();
    assertThat(dataFile.recordCount()).isEqualTo(TEST_RECORDS.size());
    assertThat(dataFile.format()).isEqualTo(fileFormat);

    // Verify the file content by reading it back
    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    List<Record> readRecords;
    try (CloseableIterable<Record> reader =
        FormatModelRegistry.readBuilder(fileFormat, Record.class, inputFile)
            .project(TestBase.SCHEMA)
            .reuseContainers()
            .build()) {
      readRecords = ImmutableList.copyOf(CloseableIterable.transform(reader, Record::copy));
    }

    DataTestHelpers.assertEquals(TestBase.SCHEMA.asStruct(), TEST_RECORDS, readRecords);
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  public void testEqualityDeleteWriterRoundTrip(FileFormat fileFormat) throws IOException {
    FileWriterBuilder<EqualityDeleteWriter<Record>, Schema> writerBuilder =
        FormatModelRegistry.equalityDeleteWriteBuilder(fileFormat, Record.class, encryptedFile);

    DeleteFile deleteFile;
    EqualityDeleteWriter<Record> writer =
        writerBuilder
            .schema(TestBase.SCHEMA)
            .spec(PartitionSpec.unpartitioned())
            .equalityFieldIds(3)
            .build();
    try (writer) {
      for (Record record : TEST_RECORDS) {
        writer.write(record);
      }
    }

    deleteFile = writer.toDeleteFile();

    assertThat(deleteFile).isNotNull();
    assertThat(deleteFile.recordCount()).isEqualTo(TEST_RECORDS.size());
    assertThat(deleteFile.format()).isEqualTo(fileFormat);
    assertThat(deleteFile.equalityFieldIds()).containsExactly(3);

    // Verify the file content by reading it back
    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    List<Record> readRecords;
    try (CloseableIterable<Record> reader =
        FormatModelRegistry.readBuilder(fileFormat, Record.class, inputFile)
            .project(TestBase.SCHEMA)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    DataTestHelpers.assertEquals(TestBase.SCHEMA.asStruct(), TEST_RECORDS, readRecords);
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  public void testPositionDeleteWriterRoundTrip(FileFormat fileFormat) throws IOException {
    Schema positionDeleteSchema = new Schema(DELETE_FILE_PATH, DELETE_FILE_POS);

    FileWriterBuilder<PositionDeleteWriter<Record>, ?> writerBuilder =
        FormatModelRegistry.positionDeleteWriteBuilder(fileFormat, encryptedFile);

    PositionDelete<Record> delete1 = PositionDelete.create();
    delete1.set("data-file-1.parquet", 0L);

    PositionDelete<Record> delete2 = PositionDelete.create();
    delete2.set("data-file-1.parquet", 1L);

    List<PositionDelete<Record>> positionDeletes = ImmutableList.of(delete1, delete2);

    DeleteFile deleteFile;
    PositionDeleteWriter<Record> writer = writerBuilder.spec(PartitionSpec.unpartitioned()).build();
    try (writer) {
      for (PositionDelete<Record> delete : positionDeletes) {
        writer.write(delete);
      }
    }

    deleteFile = writer.toDeleteFile();

    assertThat(deleteFile).isNotNull();
    assertThat(deleteFile.recordCount()).isEqualTo(2);
    assertThat(deleteFile.format()).isEqualTo(fileFormat);

    // Verify the file content by reading it back
    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    List<Record> readRecords;
    try (CloseableIterable<Record> reader =
        FormatModelRegistry.readBuilder(fileFormat, Record.class, inputFile)
            .project(positionDeleteSchema)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    List<Record> expected =
        ImmutableList.of(
            GenericRecord.create(positionDeleteSchema)
                .copy(DELETE_FILE_PATH.name(), "data-file-1.parquet", DELETE_FILE_POS.name(), 0L),
            GenericRecord.create(positionDeleteSchema)
                .copy(DELETE_FILE_PATH.name(), "data-file-1.parquet", DELETE_FILE_POS.name(), 1L));

    DataTestHelpers.assertEquals(positionDeleteSchema.asStruct(), expected, readRecords);
  }

  /**
   * Write generic records using column-split writers (narrowed) across two Parquet files, then read
   * them back using combined readers. The schema has two columns (id=3, data=4) which are split so
   * that each file stores one column. The combined reader must reassemble the full records.
   */
  @ParameterizedTest
  @FieldSource("MULTI_THREADED")
  public void testColumnSplitDataWriterRoundTrip(boolean multiThreaded) throws IOException {
    FileFormat fileFormat = FileFormat.PARQUET;
    List<Record> records = LARGE_TEST_RECORDS;

    // Create two separate output files for the two column families
    EncryptedOutputFile outputFile1 =
        EncryptedFiles.encryptedOutput(
            fileIO.newOutputFile("column-split-file-1"), EncryptionKeyMetadata.EMPTY);
    EncryptedOutputFile outputFile2 =
        EncryptedFiles.encryptedOutput(
            fileIO.newOutputFile("column-split-file-2"), EncryptionKeyMetadata.EMPTY);

    // Split columns: file1 gets field 3 (id), file2 gets field 4 (data)
    Map<EncryptedOutputFile, List<Integer>> writeColumnSplits = new LinkedHashMap<>();
    writeColumnSplits.put(outputFile1, ImmutableList.of(3));
    writeColumnSplits.put(outputFile2, ImmutableList.of(4));

    FileWriterBuilder<DataWriter<Record>, Schema> writerBuilder =
        FormatModelRegistry.dataWriteBuilder(fileFormat, Record.class, writeColumnSplits);

    DataFile dataFile;
    DataWriter<Record> writer =
        writerBuilder
            .schema(TestBase.SCHEMA)
            .spec(PartitionSpec.unpartitioned())
            .set(FormatModel.MULTI_THREADED, Boolean.toString(multiThreaded))
            .build();
    try (writer) {
      for (Record record : records) {
        writer.write(record);
      }
    }

    dataFile = writer.toDataFile();

    assertThat(dataFile).isNotNull();
    assertThat(dataFile.recordCount()).isEqualTo(records.size());
    assertThat(dataFile.format()).isEqualTo(fileFormat);

    // Verify that each split file contains only its expected column
    InputFile inputFile1 = outputFile1.encryptingOutputFile().toInputFile();
    InputFile inputFile2 = outputFile2.encryptingOutputFile().toInputFile();

    // Use Parquet file metrics to verify only expected columns are stored in each file
    Metrics file1Metrics = ParquetUtil.fileMetrics(inputFile1, MetricsConfig.getDefault());
    assertThat(file1Metrics.columnSizes())
        .as("File 1 should only contain field 3 (id)")
        .containsOnlyKeys(3);

    Metrics file2Metrics = ParquetUtil.fileMetrics(inputFile2, MetricsConfig.getDefault());
    assertThat(file2Metrics.columnSizes())
        .as("File 2 should only contain field 4 (data)")
        .containsOnlyKeys(4);

    // Verify the actual values in each file by reading the expected column
    Schema idOnlySchema = new Schema(TestBase.SCHEMA.findField(3));
    Schema dataOnlySchema = new Schema(TestBase.SCHEMA.findField(4));

    // File 1 should contain only the "id" column (field 3) with correct values
    List<Record> file1IdRecords;
    try (CloseableIterable<Record> reader =
        FormatModelRegistry.readBuilder(fileFormat, Record.class, inputFile1)
            .project(idOnlySchema)
            .build()) {
      file1IdRecords = ImmutableList.copyOf(CloseableIterable.transform(reader, Record::copy));
    }

    assertThat(file1IdRecords).hasSize(records.size());
    for (int i = 0; i < records.size(); i++) {
      assertThat(file1IdRecords.get(i).getField("id"))
          .as("File 1 should contain the id values")
          .isEqualTo(records.get(i).getField("id"));
    }

    // File 2 should contain only the "data" column (field 4) with correct values
    List<Record> file2DataRecords;
    try (CloseableIterable<Record> reader =
        FormatModelRegistry.readBuilder(fileFormat, Record.class, inputFile2)
            .project(dataOnlySchema)
            .build()) {
      file2DataRecords = ImmutableList.copyOf(CloseableIterable.transform(reader, Record::copy));
    }

    assertThat(file2DataRecords).hasSize(records.size());
    for (int i = 0; i < records.size(); i++) {
      assertThat(file2DataRecords.get(i).getField("data"))
          .as("File 2 should contain the data values")
          .isEqualTo(records.get(i).getField("data"));
    }

    // Read back using column-split reader (combined) and verify full round-trip
    Map<InputFile, List<Integer>> readColumnSplits = new LinkedHashMap<>();
    readColumnSplits.put(inputFile1, ImmutableList.of(3));
    readColumnSplits.put(inputFile2, ImmutableList.of(4));

    List<Record> readRecords;
    try (CloseableIterable<Record> reader =
        FormatModelRegistry.readBuilder(fileFormat, Record.class, readColumnSplits)
            .project(TestBase.SCHEMA)
            .set(FormatModel.MULTI_THREADED, Boolean.toString(multiThreaded))
            .build()) {
      readRecords = ImmutableList.copyOf(CloseableIterable.transform(reader, Record::copy));
    }

    DataTestHelpers.assertEquals(TestBase.SCHEMA.asStruct(), records, readRecords);

    // Cleanup the additional files
    fileIO.deleteFile(outputFile1.encryptingOutputFile());
    fileIO.deleteFile(outputFile2.encryptingOutputFile());
  }

  /** Closing a column-split writer without writing any records should not hang or throw. */
  @ParameterizedTest
  @FieldSource("MULTI_THREADED")
  public void testColumnSplitEmptyWriterClose(boolean multiThreaded) throws IOException {
    FileFormat fileFormat = FileFormat.PARQUET;

    EncryptedOutputFile outputFile1 =
        EncryptedFiles.encryptedOutput(
            fileIO.newOutputFile("empty-split-file-1"), EncryptionKeyMetadata.EMPTY);
    EncryptedOutputFile outputFile2 =
        EncryptedFiles.encryptedOutput(
            fileIO.newOutputFile("empty-split-file-2"), EncryptionKeyMetadata.EMPTY);

    Map<EncryptedOutputFile, List<Integer>> writeColumnSplits = new LinkedHashMap<>();
    writeColumnSplits.put(outputFile1, ImmutableList.of(3));
    writeColumnSplits.put(outputFile2, ImmutableList.of(4));

    FileWriterBuilder<DataWriter<Record>, Schema> writerBuilder =
        FormatModelRegistry.dataWriteBuilder(fileFormat, Record.class, writeColumnSplits);

    DataWriter<Record> writer =
        writerBuilder
            .schema(TestBase.SCHEMA)
            .spec(PartitionSpec.unpartitioned())
            .set(FormatModel.MULTI_THREADED, Boolean.toString(multiThreaded))
            .build();
    writer.close();

    DataFile dataFile = writer.toDataFile();
    assertThat(dataFile).isNotNull();
    assertThat(dataFile.recordCount()).isEqualTo(0);

    if (fileIO.fileExists(outputFile1.encryptingOutputFile().location())) {
      fileIO.deleteFile(outputFile1.encryptingOutputFile());
    }

    if (fileIO.fileExists(outputFile2.encryptingOutputFile().location())) {
      fileIO.deleteFile(outputFile2.encryptingOutputFile());
    }
  }
}

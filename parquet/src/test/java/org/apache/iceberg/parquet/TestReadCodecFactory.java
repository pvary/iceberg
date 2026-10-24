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
package org.apache.iceberg.parquet;

import static org.apache.iceberg.parquet.ParquetWritingTestUtils.createTempFile;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.luben.zstd.Zstd;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputDecompressor;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopCodecs;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestReadCodecFactory {
  private static final int PAGE_SIZE = 256 * 1024;

  @TempDir private Path temp;

  private final CompressionCodecFactory parquetCodecs = HadoopCodecs.newFactory(0);
  private final CompressionCodecFactory codecs = new ReadCodecFactory(HadoopCodecs.newFactory(0));

  @Test
  public void testDecompressPageFromParquetCompressor() throws IOException {
    byte[] page = page(PAGE_SIZE, 1);
    byte[] compressed = compress(page);

    assertThat(decompress(BytesInput.from(compressed), page.length)).isEqualTo(page);
    assertThat(
            toArray(
                parquetCodecs
                    .getDecompressor(CompressionCodecName.ZSTD)
                    .decompress(BytesInput.from(compressed), page.length)))
        .isEqualTo(page);
  }

  @Test
  public void testDecompressConcatenatedFrames() throws IOException {
    byte[] first = page(PAGE_SIZE, 1);
    byte[] second = page(PAGE_SIZE / 2, 2);

    assertThat(decompress(BytesInput.concat(zstd(first), zstd(second)), PAGE_SIZE * 3 / 2))
        .isEqualTo(concat(first, second));
  }

  @Test
  public void testDecompressPageInBufferSlice() throws IOException {
    byte[] page = page(PAGE_SIZE, 1);
    byte[] compressed = compress(page);
    byte[] padded = new byte[compressed.length + 20];
    System.arraycopy(compressed, 0, padded, 10, compressed.length);

    ByteBuffer slice = ByteBuffer.wrap(padded, 10, compressed.length).slice();
    assertThat(decompress(BytesInput.from(slice), page.length)).isEqualTo(page);

    ByteBuffer positioned = ByteBuffer.wrap(padded);
    positioned.position(10);
    positioned.limit(10 + compressed.length);
    assertThat(decompress(BytesInput.from(positioned), page.length)).isEqualTo(page);
  }

  @Test
  public void testDecompressPageInMultipleBuffers() throws IOException {
    byte[] page = page(PAGE_SIZE, 1);
    byte[] compressed = compress(page);
    int split = compressed.length / 3;

    BytesInput input =
        BytesInput.from(
            ByteBuffer.wrap(compressed, 0, split).slice(),
            ByteBuffer.wrap(compressed, split, compressed.length - split).slice());
    assertThat(decompress(input, page.length)).isEqualTo(page);
  }

  @Test
  public void testDecompressPageInDirectBuffer() throws IOException {
    byte[] page = page(PAGE_SIZE, 1);
    byte[] compressed = compress(page);
    ByteBuffer direct = ByteBuffer.allocateDirect(compressed.length);
    direct.put(compressed).flip();

    assertThat(decompress(BytesInput.from(direct), page.length)).isEqualTo(page);
  }

  @Test
  public void testDecompressEmptyPage() throws IOException {
    assertThat(decompress(BytesInput.from(compress(new byte[0])), 0)).isEmpty();
  }

  @Test
  public void testDecompressDirectBuffersWithParquetDecompressor() throws IOException {
    byte[] page = page(PAGE_SIZE, 1);
    byte[] compressed = compress(page);
    ByteBuffer input = ByteBuffer.allocateDirect(compressed.length);
    input.put(compressed).flip();
    ByteBuffer output = ByteBuffer.allocateDirect(page.length);

    codecs
        .getDecompressor(CompressionCodecName.ZSTD)
        .decompress(input, compressed.length, output, page.length);

    output.flip();
    assertThat(ByteBuffers.toByteArray(output)).isEqualTo(page);
  }

  @Test
  public void testWrongDecompressedSize() {
    byte[] page = page(PAGE_SIZE, 1);
    byte[] compressed = compress(page);

    assertThatThrownBy(() -> decompress(BytesInput.from(compressed), page.length + 1))
        .isInstanceOf(IOException.class)
        .hasMessage(
            "Cannot decompress zstd page: expected %d bytes, but got %d",
            page.length + 1, page.length);

    assertThatThrownBy(() -> decompress(BytesInput.from(compressed), page.length - 1))
        .isInstanceOf(IOException.class)
        .hasMessageStartingWith("Cannot decompress zstd page: ");
  }

  @Test
  public void testCorruptPage() {
    byte[] page = page(PAGE_SIZE, 1);
    byte[] compressed = compress(page);

    byte[] truncated = Arrays.copyOf(compressed, compressed.length / 2);
    assertThatThrownBy(() -> decompress(BytesInput.from(truncated), page.length))
        .isInstanceOf(IOException.class)
        .hasMessageStartingWith("Cannot decompress zstd page: ");

    byte[] notZstd = page(1024, 2);
    assertThatThrownBy(() -> decompress(BytesInput.from(notZstd), page.length))
        .isInstanceOf(IOException.class)
        .hasMessageStartingWith("Cannot decompress zstd page: ");
  }

  @Test
  public void testOtherCodecsUseWrappedFactory() {
    CompressionCodecFactory wrapped = HadoopCodecs.newFactory(0);
    CompressionCodecFactory factory = new ReadCodecFactory(wrapped);

    for (CompressionCodecName codec :
        ImmutableList.of(
            CompressionCodecName.UNCOMPRESSED,
            CompressionCodecName.SNAPPY,
            CompressionCodecName.GZIP)) {
      assertThat(factory.getDecompressor(codec)).isSameAs(wrapped.getDecompressor(codec));
      assertThat(factory.getCompressor(codec)).isSameAs(wrapped.getCompressor(codec));
    }

    assertThat(factory.getDecompressor(CompressionCodecName.ZSTD))
        .isNotSameAs(wrapped.getDecompressor(CompressionCodecName.ZSTD));
    assertThat(factory.getCompressor(CompressionCodecName.ZSTD))
        .isSameAs(wrapped.getCompressor(CompressionCodecName.ZSTD));
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1", "v2"})
  public void testReadZstdFile(String pageVersion) throws IOException {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "category", Types.StringType.get()),
            optional(3, "values", Types.ListType.ofOptional(4, Types.FloatType.get())));

    Random random = new Random(7);
    GenericRecord template = GenericRecord.create(schema);
    List<Record> records = Lists.newArrayList();
    for (long id = 0; id < 20_000; id += 1) {
      List<Float> values = Lists.newArrayList();
      for (int i = 0; i < 8; i += 1) {
        values.add(random.nextFloat());
      }

      // the category column is dictionary encoded
      records.add(
          template.copy(
              ImmutableMap.of("id", id, "category", "category-" + (id % 10), "values", values)));
    }

    File file = createTempFile(temp);
    try (FileAppender<Record> appender =
        Parquet.write(Files.localOutput(file))
            .schema(schema)
            .set(TableProperties.PARQUET_COMPRESSION, "zstd")
            .set(TableProperties.PARQUET_PAGE_VERSION, pageVersion)
            .set(TableProperties.PARQUET_PAGE_SIZE_BYTES, "65536")
            .createWriterFunc(GenericParquetWriter::create)
            .build()) {
      appender.addAll(records);
    }

    try (CloseableIterable<Record> reader =
        Parquet.read(Files.localInput(file))
            .project(schema)
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
            .build()) {
      assertThat(ImmutableList.copyOf(reader)).isEqualTo(records);
    }
  }

  private byte[] decompress(BytesInput input, int decompressedSize) throws IOException {
    BytesInputDecompressor decompressor = codecs.getDecompressor(CompressionCodecName.ZSTD);
    return toArray(decompressor.decompress(input, decompressedSize));
  }

  /** Compresses a page the way Parquet writers do, with a zstd output stream. */
  private byte[] compress(byte[] page) {
    try {
      // the compressor reuses its output buffer, so copy the compressed bytes
      return toArray(
              parquetCodecs
                  .getCompressor(CompressionCodecName.ZSTD)
                  .compress(BytesInput.from(page)))
          .clone();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static BytesInput zstd(byte[] page) {
    return BytesInput.from(Zstd.compress(page));
  }

  private static byte[] toArray(BytesInput input) {
    return ByteBuffers.toByteArray(
        input.toByteBuffer(HeapByteBufferAllocator.getInstance(), buffer -> {}));
  }

  private static byte[] concat(byte[] first, byte[] second) {
    byte[] result = Arrays.copyOf(first, first.length + second.length);
    System.arraycopy(second, 0, result, first.length, second.length);
    return result;
  }

  /** Returns compressible bytes, like encoded values with a narrow range. */
  private static byte[] page(int size, int seed) {
    Random random = new Random(seed);
    byte[] page = new byte[size];
    for (int i = 0; i < size; i += 1) {
      page[i] = (byte) (random.nextGaussian() * 20);
    }

    return page;
  }
}

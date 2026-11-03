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
package org.apache.iceberg.azure.adlsv2;

import com.azure.core.util.Context;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.models.DataLakeFileOpenInputStreamResult;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.FileRange;
import com.azure.storage.file.datalake.options.DataLakeFileInputStreamOptions;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ADLSInputStream extends SeekableInputStream implements RangeReadable {
  private static final Logger LOG = LoggerFactory.getLogger(ADLSInputStream.class);

  // max skip size, if skip is larger, then the stream will be reopened at new position
  private static final int SKIP_SIZE = 1024 * 1024;

  // downloads the chunks of vectored reads, the parallelism of every read limits its requests
  private static final ExecutorService VECTORED_READ_POOL =
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("iceberg-adls-vectored-read-%d")
              .build());

  private final StackTraceElement[] createStack;
  private final String location;
  private final DataLakeFileClient fileClient;
  private Long fileSize;
  private final AzureProperties azureProperties;

  private InputStream stream;
  private long pos;
  private long next;
  private boolean closed;

  private final Counter readBytes;
  private final Counter readOperations;

  ADLSInputStream(
      String location,
      DataLakeFileClient fileClient,
      Long fileSize,
      AzureProperties azureProperties,
      MetricsContext metrics) {
    this.location = location;
    this.fileClient = fileClient;
    this.fileSize = fileSize;
    this.azureProperties = azureProperties;

    this.readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, Unit.BYTES);
    this.readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);

    this.createStack = Thread.currentThread().getStackTrace();

    openStream();
  }

  private void openStream() {
    DataLakeFileOpenInputStreamResult result = openRange(new FileRange(pos));
    this.fileSize = result.getProperties().getFileSize();
    this.stream = result.getInputStream();
  }

  private DataLakeFileInputStreamOptions getInputOptions(FileRange range) {
    DataLakeFileInputStreamOptions options = new DataLakeFileInputStreamOptions();
    azureProperties.adlsReadBlockSize().ifPresent(options::setBlockSize);
    options.setRange(range);
    return options;
  }

  @Override
  public long getPos() {
    return next;
  }

  @Override
  public void seek(long newPos) {
    Preconditions.checkState(!closed, "Cannot seek: already closed");
    Preconditions.checkArgument(newPos >= 0, "Cannot seek: position %s is negative", newPos);

    // this allows a seek beyond the end of the stream but the next read will fail
    this.next = newPos;
  }

  @Override
  public int read() throws IOException {
    Preconditions.checkState(!closed, "Cannot read: already closed");
    positionStream();

    int bytesRead = stream.read();
    if (bytesRead == -1) {
      return -1;
    }

    pos += 1;
    next += 1;
    readBytes.increment();
    readOperations.increment();

    return bytesRead;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Preconditions.checkState(!closed, "Cannot read: already closed");
    positionStream();

    int bytesRead = stream.read(b, off, len);
    if (bytesRead == -1) {
      return -1;
    }

    pos += bytesRead;
    next += bytesRead;
    readBytes.increment(bytesRead);
    readOperations.increment();

    return bytesRead;
  }

  private void positionStream() throws IOException {
    if ((stream != null) && (next == pos)) {
      // already at specified position
      return;
    }

    if ((stream != null) && (next > pos)) {
      // seeking forwards
      long skip = next - pos;
      if (skip <= Math.max(stream.available(), SKIP_SIZE)) {
        // already buffered or seek is small enough
        try {
          ByteStreams.skipFully(stream, skip);
          this.pos = next;
          return;
        } catch (IOException ignored) {
          // will retry by re-opening the stream
        }
      }
    }

    // close the stream and open at desired position
    this.pos = next;
    openStream();
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    Preconditions.checkPositionIndexes(offset, offset + length, buffer.length);

    FileRange range = new FileRange(position, position + length);

    try (InputStream inputStream = openRange(range).getInputStream()) {
      IOUtil.readFully(inputStream, buffer, offset, length);
      if (length > 0) {
        readBytes.increment(length);
        readOperations.increment();
      }
    }
  }

  @Override
  public int readTail(byte[] buffer, int offset, int length) throws IOException {
    Preconditions.checkPositionIndexes(offset, offset + length, buffer.length);

    if (this.fileSize == null) {
      this.fileSize = fileClient.getProperties().getFileSize();
    }
    long readStart = Math.max(0, fileSize - length);

    try (InputStream inputStream = openRange(new FileRange(readStart)).getInputStream()) {
      int bytesRead = IOUtil.readRemaining(inputStream, buffer, offset, length);
      if (bytesRead > 0) {
        readBytes.increment(bytesRead);
        readOperations.increment();
      }
      return bytesRead;
    }
  }

  /**
   * Reads the ranges with concurrent requests when {@link
   * AzureProperties#ADLS_READ_VECTORED_PARALLELISM} is greater than 1, otherwise one after the
   * other.
   *
   * <p>A concurrent read splits the ranges into chunks of {@link
   * AzureProperties#ADLS_READ_VECTORED_CHUNK_SIZE} bytes, and downloads up to parallelism chunks at
   * the same time, every chunk with a single request into its part of the buffer of its range. The
   * chunks are downloaded in the order of their offsets, so the first ranges complete first.
   */
  @Override
  public void readVectored(
      List<org.apache.iceberg.io.FileRange> ranges, IntFunction<ByteBuffer> allocate)
      throws IOException {
    int parallelism = azureProperties.adlsReadVectoredParallelism();
    if (parallelism <= 1) {
      RangeReadable.super.readVectored(ranges, allocate);
      return;
    }

    long chunkSize = azureProperties.adlsReadVectoredChunkSize();
    Queue<VectoredChunk> chunks = new ConcurrentLinkedQueue<>();
    for (org.apache.iceberg.io.FileRange range : RangeReadable.sortRanges(ranges)) {
      ByteBuffer buffer = allocate.apply(range.length());
      if (range.length() == 0) {
        range.byteBuffer().complete(buffer);
        continue;
      }

      AtomicInteger remaining =
          new AtomicInteger((int) ((range.length() + chunkSize - 1) / chunkSize));
      for (long start = 0; start < range.length(); start += chunkSize) {
        int length = (int) Math.min(chunkSize, range.length() - start);
        chunks.add(new VectoredChunk(range, buffer, remaining, (int) start, length));
      }
    }

    int workers = Math.min(parallelism, chunks.size());
    for (int worker = 0; worker < workers; worker += 1) {
      VECTORED_READ_POOL.execute(() -> downloadChunks(chunks));
    }
  }

  private void downloadChunks(Queue<VectoredChunk> chunks) {
    for (VectoredChunk chunk = chunks.poll(); chunk != null; chunk = chunks.poll()) {
      download(chunk);
    }
  }

  private void download(VectoredChunk chunk) {
    CompletableFuture<ByteBuffer> future = chunk.range.byteBuffer();
    if (future.isDone()) {
      // another chunk of the range failed
      return;
    }

    try {
      ByteBuffer target = chunk.buffer.duplicate();
      target.position(chunk.start);
      target.limit(chunk.start + chunk.length);
      ByteBuffer part = target.slice();
      FileRange range = new FileRange(chunk.range.offset() + chunk.start, (long) chunk.length);
      fileClient.readWithResponse(
          new ByteBufferOutputStream(part), range, null, null, false, null, Context.NONE);
      if (part.hasRemaining()) {
        throw new EOFException(
            String.format("Reached the end of %s before the end of range %s", location, range));
      }

      readBytes.increment(chunk.length);
      readOperations.increment();
      if (chunk.remaining.decrementAndGet() == 0) {
        future.complete(chunk.buffer);
      }
    } catch (IOException | RuntimeException e) {
      future.completeExceptionally(
          isFileNotFoundException(e)
              ? new NotFoundException(e, "Location does not exist: %s", location)
              : e);
    }
  }

  private DataLakeFileOpenInputStreamResult openRange(FileRange range) {
    try {
      return fileClient.openInputStream(getInputOptions(range));
    } catch (RuntimeException e) {
      throwNotFoundIfNotPresent(e, location);
      LOG.error(
          "Failed to open input stream for file {}, range {}", fileClient.getFilePath(), range, e);
      throw e;
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    this.closed = true;
    if (stream != null) {
      stream.close();
    }
  }

  @SuppressWarnings({"checkstyle:NoFinalizer", "Finalize", "deprecation"})
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (!closed) {
      close(); // releasing resources is more important than printing the warning
      String trace = Joiner.on("\n\t").join(Arrays.copyOfRange(createStack, 1, createStack.length));
      LOG.warn("Unclosed input stream created by:\n\t{}", trace);
    }
  }

  private static void throwNotFoundIfNotPresent(Throwable throwable, String location) {
    if (isFileNotFoundException(throwable)) {
      throw new NotFoundException(throwable, "Location does not exist: %s", location);
    }
  }

  private static boolean isFileNotFoundException(Throwable exception) {
    if (exception instanceof BlobStorageException blobStorageException) {
      return BlobErrorCode.BLOB_NOT_FOUND.equals(blobStorageException.getErrorCode());
    }
    if (exception instanceof DataLakeStorageException dataLakeStorageException) {
      return "PathNotFound".equals(dataLakeStorageException.getErrorCode());
    }
    return false;
  }

  /** A part of a range of a vectored read, which is downloaded with a single request. */
  private static class VectoredChunk {
    private final org.apache.iceberg.io.FileRange range;
    private final ByteBuffer buffer;
    // the number of chunks of the range that are not downloaded yet
    private final AtomicInteger remaining;
    private final int start;
    private final int length;

    private VectoredChunk(
        org.apache.iceberg.io.FileRange range,
        ByteBuffer buffer,
        AtomicInteger remaining,
        int start,
        int length) {
      this.range = range;
      this.buffer = buffer;
      this.remaining = remaining;
      this.start = start;
      this.length = length;
    }
  }

  /** Writes into a buffer, and fails when the buffer has no room left. */
  private static class ByteBufferOutputStream extends OutputStream {
    private final ByteBuffer buffer;

    private ByteBufferOutputStream(ByteBuffer buffer) {
      this.buffer = buffer;
    }

    @Override
    public void write(int value) {
      buffer.put((byte) value);
    }

    @Override
    public void write(byte[] bytes, int offset, int length) {
      buffer.put(bytes, offset, length);
    }
  }
}

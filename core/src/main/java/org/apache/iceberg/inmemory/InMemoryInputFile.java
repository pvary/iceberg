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
package org.apache.iceberg.inmemory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.UUID;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class InMemoryInputFile implements InputFile {

  private final String location;
  private final byte[] contents;

  public InMemoryInputFile(byte[] contents) {
    this("memory:" + UUID.randomUUID(), contents);
  }

  public InMemoryInputFile(String location, byte[] contents) {
    this(location, contents, true);
  }

  /**
   * Returns an {@link InMemoryInputFile} that adopts {@code contents} <em>by reference</em> without
   * making a defensive copy.
   *
   * <p>Use this when the caller owns the buffer for the lifetime of the returned file and
   * guarantees it will not be mutated after the call -- for example, a freshly-allocated buffer
   * filled by a single ranged read on a hot lookup path. Prefer the public constructors when those
   * invariants do not hold.
   *
   * @param contents byte buffer to expose; <em>not</em> copied
   */
  public static InMemoryInputFile wrap(byte[] contents) {
    return wrap("memory:" + UUID.randomUUID(), contents);
  }

  /** See {@link #wrap(byte[])}. */
  public static InMemoryInputFile wrap(String location, byte[] contents) {
    return new InMemoryInputFile(location, contents, false);
  }

  private InMemoryInputFile(String location, byte[] contents, boolean copy) {
    Preconditions.checkNotNull(location, "location is null");
    Preconditions.checkNotNull(contents, "contents is null");
    this.location = location;
    this.contents = copy ? contents.clone() : contents;
  }

  @Override
  public long getLength() {
    return contents.length;
  }

  @Override
  public SeekableInputStream newStream() {
    return new InMemorySeekableInputStream(contents);
  }

  @Override
  public String location() {
    return location;
  }

  @Override
  public boolean exists() {
    return true;
  }

  private static class InMemorySeekableInputStream extends SeekableInputStream {

    private final long length;
    private final ByteArrayInputStream delegate;
    private boolean closed = false;

    InMemorySeekableInputStream(byte[] contents) {
      this.length = contents.length;
      this.delegate = new ByteArrayInputStream(contents);
    }

    @Override
    public long getPos() throws IOException {
      checkOpen();
      return length - delegate.available();
    }

    @Override
    public void seek(long newPos) throws IOException {
      checkOpen();
      delegate.reset(); // resets to a marked position
      Preconditions.checkState(
          delegate.skip(newPos) == newPos,
          "Invalid position %s within stream of length %s",
          newPos,
          length);
    }

    @Override
    public int read() {
      checkOpen();
      return delegate.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
      checkOpen();
      return delegate.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) {
      checkOpen();
      return delegate.read(b, off, len);
    }

    @Override
    public long skip(long n) {
      checkOpen();
      return delegate.skip(n);
    }

    @Override
    public int available() {
      checkOpen();
      return delegate.available();
    }

    @Override
    public boolean markSupported() {
      return false;
    }

    @Override
    public void mark(int readAheadLimit) {
      // The delegate's mark is used to implement seek
      throw new UnsupportedOperationException();
    }

    @Override
    public void reset() {
      checkOpen();
      delegate.reset();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
      closed = true;
    }

    private void checkOpen() {
      // ByteArrayInputStream can be used even after close, so for test purposes disallow such use
      // explicitly
      Preconditions.checkState(!closed, "Stream is closed");
    }
  }
}

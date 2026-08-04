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
package org.apache.iceberg.io;

import java.nio.ByteBuffer;

/**
 * Resolves a file location to an {@link InputFile}.
 *
 * <p>File formats whose files reference other files need to open inputs that the caller did not
 * plan for. Instead of handing the format the table's {@link FileIO}, which also allows writing and
 * deleting, callers provide a resolver that only exposes read access. Implementations should reuse
 * the caller's {@link FileIO} so that reads are tracked by the same metrics context and use the
 * same credentials as the planned files.
 */
@FunctionalInterface
public interface InputFileResolver {
  /** Length passed when the caller does not know the size of the referenced file. */
  long LENGTH_UNKNOWN = -1L;

  /**
   * Returns an {@link InputFile} for the given location.
   *
   * @param location the location of the file to read
   * @param length the length of the file in bytes, or {@link #LENGTH_UNKNOWN} when not known.
   *     Passing the length avoids a metadata request when the reader asks for it.
   * @param keyMetadata encryption key metadata for the file, or null when it is not encrypted
   * @return an input file for the location
   */
  InputFile resolve(String location, long length, ByteBuffer keyMetadata);
}

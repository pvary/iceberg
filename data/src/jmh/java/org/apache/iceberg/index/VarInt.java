/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */
package org.apache.iceberg.index;

import java.io.IOException;
import java.io.OutputStream;

/** Minimal varint / zigzag-varint helpers shared by the binary {@link MetadataHandler}s. */
final class VarInt {
  private VarInt() {}

  static void writeUVarInt(OutputStream out, int value) throws IOException {
    int v = value;
    while ((v & ~0x7F) != 0) {
      out.write((v & 0x7F) | 0x80);
      v >>>= 7;
    }
    out.write(v & 0x7F);
  }

  static void writeUVarLong(OutputStream out, long value) throws IOException {
    long v = value;
    while ((v & ~0x7FL) != 0L) {
      out.write((int) ((v & 0x7FL) | 0x80L));
      v >>>= 7;
    }
    out.write((int) (v & 0x7FL));
  }

  static void writeZigZagVarLong(OutputStream out, long value) throws IOException {
    writeUVarLong(out, (value << 1) ^ (value >> 63));
  }
}


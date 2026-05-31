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

import java.util.Collection;

/**
 * Helpers for extracting the longest common prefix shared by a collection of file paths. Used by
 * the {@link MetadataHandler} implementations to factor the prefix out of every row and store it
 * once in the file header / file-level metadata.
 */
final class FilePathPrefix {

  /**
   * File-level metadata key used by the columnar handlers ({@link ParquetMetadataHandler}, {@link
   * AvroMetadataHandler}) to store the extracted prefix.
   */
  static final String META_KEY = "index.file_path.common_prefix";

  private FilePathPrefix() {}

  /** Returns the longest common UTF-16 prefix of the supplied strings, or {@code ""} if empty. */
  static String longestCommonPrefix(Collection<String> paths) {
    if (paths.isEmpty()) {
      return "";
    }
    String prefix = null;
    for (String s : paths) {
      if (prefix == null) {
        prefix = s;
        continue;
      }
      int max = Math.min(prefix.length(), s.length());
      int i = 0;
      while (i < max && prefix.charAt(i) == s.charAt(i)) {
        i++;
      }
      if (i < prefix.length()) {
        prefix = prefix.substring(0, i);
      }
      if (prefix.isEmpty()) {
        return "";
      }
    }
    return prefix;
  }
}

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
package org.apache.iceberg;

import java.util.List;
import org.apache.iceberg.util.ScanTaskUtil;

/** A scan task over a range of bytes in a single data file. */
public interface FileScanTask extends ContentScanTask<DataFile>, SplittableScanTask<FileScanTask> {
  /**
   * A list of {@link DeleteFile delete files} to apply when reading the task's data file.
   *
   * @return a list of delete files to apply
   */
  List<DeleteFile> deletes();

  /** Return the schema for this file scan task. */
  default Schema schema() {
    throw new UnsupportedOperationException("Does not support schema getter");
  }

  /**
   * Whether the data file must be read immediately when processing this task.
   *
   * <p>When a skipping index is available for a scan, tasks may be produced from the index instead
   * of the original data files. Such tasks signal that the reader must open the data file
   * referenced by the index and return the row at the position identified by the index (for example
   * using the {@code _index_file_path} and {@code _index_row_position} columns) rather than
   * scanning the file sequentially.
   *
   * @return true if the data file must be read immediately, false to scan the file normally
   */
  default boolean immediateDataFileRead() {
    return false;
  }

  @Override
  default long sizeBytes() {
    return length() + ScanTaskUtil.contentSizeInBytes(deletes());
  }

  @Override
  default int filesCount() {
    return 1 + deletes().size();
  }

  @Override
  default boolean isFileScanTask() {
    return true;
  }

  @Override
  default FileScanTask asFileScanTask() {
    return this;
  }
}

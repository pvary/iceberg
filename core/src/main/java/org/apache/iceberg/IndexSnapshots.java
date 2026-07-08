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

import java.util.Map;
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Utilities for adapting an {@link IndexSnapshot} into a scannable {@link Snapshot}. */
public class IndexSnapshots {

  /** Property name pointing to the manifest list file that backs an index snapshot. */
  public static final String MANIFEST_LIST = "manifestList";

  private IndexSnapshots() {}

  /**
   * Build a {@link Snapshot} that reads the index data referenced by the given index snapshot.
   *
   * <p>The index snapshot's {@value #MANIFEST_LIST} property is used as the manifest list location
   * for the returned snapshot, which can be scanned in place of the base table snapshot.
   *
   * @param indexSnapshot an index snapshot
   * @return a snapshot backed by the index snapshot's manifest list
   */
  public static Snapshot toSnapshot(IndexSnapshot indexSnapshot) {
    Preconditions.checkArgument(indexSnapshot != null, "Invalid index snapshot: null");
    Map<String, String> properties = indexSnapshot.properties();
    String manifestList = properties != null ? properties.get(MANIFEST_LIST) : null;
    Preconditions.checkArgument(
        manifestList != null,
        "Cannot create snapshot for index snapshot %s: missing '%s' property",
        indexSnapshot.indexSnapshotId(),
        MANIFEST_LIST);

    return new BaseSnapshot(
        0 /* sequenceNumber */,
        indexSnapshot.indexSnapshotId(),
        null /* parentId */,
        System.currentTimeMillis(),
        DataOperations.REPLACE,
        properties,
        null /* schemaId */,
        manifestList,
        null /* firstRowId */,
        null /* addedRows */,
        null /* keyId */);
  }
}

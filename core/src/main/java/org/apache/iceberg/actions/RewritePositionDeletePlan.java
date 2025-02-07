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
package org.apache.iceberg.actions;

import java.util.Map;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.CloseableIterable;

/**
 * Result of the positional delete file rewrite planning. We could use
 * FileRewritePlan<RewritePositionDeleteFiles.FileGroupInfo, PositionDeletesScanTask, DeleteFile,
 * RewritePositionDeletesGroup> everywhere instead of creating a new class, but after I had to
 * create the RewriteFilePlan to handle outputSpecId, this seems more elegant.
 */
public class RewritePositionDeletePlan
    extends FileRewritePlan<
        FileRewritePlanInfo,
        RewritePositionDeleteFiles.FileGroupInfo,
        PositionDeletesScanTask,
        DeleteFile,
        RewritePositionDeletesGroup> {
  public RewritePositionDeletePlan(
      CloseableIterable<RewritePositionDeletesGroup> groups,
      int totalGroupCount,
      Map<StructLike, Integer> groupsInPartition,
      long writeMaxFileSize) {
    super(groups, new FileRewritePlanInfo(totalGroupCount, groupsInPartition, writeMaxFileSize));
  }
}

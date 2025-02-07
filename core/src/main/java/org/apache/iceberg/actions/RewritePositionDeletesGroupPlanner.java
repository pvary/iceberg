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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.RewriteJobOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RewritePositionDeleteFiles.FileGroupInfo;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Groups specified files in the {@link Table} by {@link RewriteFileGroup}s. These will be grouped
 * by partitions. Extends the {@link SizeBasedFileRewritePlanner} with {@link
 * RewritePositionDeleteFiles#REWRITE_JOB_ORDER} handling.
 */
public class RewritePositionDeletesGroupPlanner
    implements FileRewritePlanner<
        FileRewritePlanInfo,
        FileGroupInfo,
        PositionDeletesScanTask,
        DeleteFile,
        RewritePositionDeletesGroup> {
  // The SizeBasedFileRewritePlanner will be needed in the long run, but omit for the shake of the
  // discussion
  //        extends SizeBasedFileRewritePlanner<FileGroupInfo, PositionDeletesScanTask, DeleteFile,
  // RewritePositionDeletesGroup> {
  private static final Logger LOG =
      LoggerFactory.getLogger(RewritePositionDeletesGroupPlanner.class);

  private final Expression filter;
  private final boolean caseSensitive;
  private RewriteJobOrder rewriteJobOrder;

  public RewritePositionDeletesGroupPlanner(Table table) {
    this(table, Expressions.alwaysTrue(), false);
  }

  /**
   * Creates the planner for the given table.
   *
   * @param table to plan for
   * @param filter used to remove files from the plan
   * @param caseSensitive property used for scanning
   */
  public RewritePositionDeletesGroupPlanner(Table table, Expression filter, boolean caseSensitive) {
    super(table);
    this.caseSensitive = caseSensitive;
    this.filter = filter;
  }

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.<String>builder()
        .addAll(super.validOptions())
        .add(RewritePositionDeleteFiles.REWRITE_JOB_ORDER);
  }

  @Override
  public void init(Map<String, String> options) {
    super.init(options);
    this.rewriteJobOrder =
        RewriteJobOrder.fromName(
            PropertyUtil.propertyAsString(
                options,
                RewritePositionDeleteFiles.REWRITE_JOB_ORDER,
                RewritePositionDeleteFiles.REWRITE_JOB_ORDER_DEFAULT));
  }

  @Override
  public RewritePositionDeletePlan plan() {
    // [..]
  }

  @Override
  protected Iterable<PositionDeletesScanTask> filterFiles(Iterable<PositionDeletesScanTask> tasks) {
    return Iterables.filter(tasks, this::wronglySized);
  }

  @Override
  protected Iterable<List<PositionDeletesScanTask>> filterFileGroups(
      List<List<PositionDeletesScanTask>> groups) {
    return Iterables.filter(groups, this::shouldRewrite);
  }

  @Override
  protected long defaultTargetFileSize() {
    return PropertyUtil.propertyAsLong(
        table().properties(),
        TableProperties.DELETE_TARGET_FILE_SIZE_BYTES,
        TableProperties.DELETE_TARGET_FILE_SIZE_BYTES_DEFAULT);
  }
}

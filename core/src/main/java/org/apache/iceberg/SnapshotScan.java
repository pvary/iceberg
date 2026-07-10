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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.IndexCatalog;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.index.IndexDefinition;
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.metrics.DefaultMetricsContext;
import org.apache.iceberg.metrics.ImmutableScanReport;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.metrics.Timer;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a common base class to share code between different BaseScan implementations that handle
 * scans of a particular snapshot.
 *
 * @param <ThisT> actual BaseScan implementation class type
 * @param <T> type of ScanTask returned
 * @param <G> type of ScanTaskGroup returned
 */
public abstract class SnapshotScan<ThisT, T extends ScanTask, G extends ScanTaskGroup<T>>
    extends BaseScan<ThisT, T, G> {

  private static final Logger LOG = LoggerFactory.getLogger(SnapshotScan.class);

  private ScanMetrics scanMetrics;
  private IndexSelection indexSelection;

  protected SnapshotScan(Table table, Schema schema, TableScanContext context) {
    super(table, schema, context);
  }

  protected Long snapshotId() {
    return context().snapshotId();
  }

  protected abstract CloseableIterable<T> doPlanFiles();

  // controls whether to use the snapshot schema while time travelling
  protected boolean useSnapshotSchema() {
    return false;
  }

  protected ScanMetrics scanMetrics() {
    if (scanMetrics == null) {
      this.scanMetrics = ScanMetrics.of(new DefaultMetricsContext());
    }

    return scanMetrics;
  }

  protected Map<Integer, PartitionSpec> specs() {
    Map<Integer, PartitionSpec> specs = table().specs();
    // requires latest schema
    if (!useSnapshotSchema()
        || snapshotId() == null
        || table().currentSnapshot() == null
        || snapshotId().equals(table().currentSnapshot().snapshotId())) {
      return specs;
    }

    // this is a time travel request
    Schema snapshotSchema = tableSchema();
    ImmutableMap.Builder<Integer, PartitionSpec> newSpecs =
        ImmutableMap.builderWithExpectedSize(specs.size());
    for (Map.Entry<Integer, PartitionSpec> entry : specs.entrySet()) {
      newSpecs.put(entry.getKey(), entry.getValue().toUnbound().bind(snapshotSchema, true));
    }

    return newSpecs.build();
  }

  public ThisT useSnapshot(long scanSnapshotId) {
    Preconditions.checkArgument(
        snapshotId() == null, "Cannot override snapshot, already set snapshot id=%s", snapshotId());
    Preconditions.checkArgument(
        table().snapshot(scanSnapshotId) != null,
        "Cannot find snapshot with ID %s",
        scanSnapshotId);
    Schema newSchema =
        useSnapshotSchema() ? SnapshotUtil.schemaFor(table(), scanSnapshotId) : tableSchema();
    TableScanContext newContext = context().useSnapshotId(scanSnapshotId);
    return newRefinedScan(table(), newSchema, newContext);
  }

  public ThisT useRef(String name) {
    if (SnapshotRef.MAIN_BRANCH.equals(name)) {
      return newRefinedScan(table(), tableSchema(), context());
    }

    Preconditions.checkArgument(
        snapshotId() == null, "Cannot override ref, already set snapshot id=%s", snapshotId());
    Snapshot snapshot = table().snapshot(name);
    Preconditions.checkArgument(snapshot != null, "Cannot find ref %s", name);
    TableScanContext newContext = context().useSnapshotId(snapshot.snapshotId());
    return newRefinedScan(table(), SnapshotUtil.schemaFor(table(), name), newContext);
  }

  public ThisT asOfTime(long timestampMillis) {
    Preconditions.checkArgument(
        snapshotId() == null, "Cannot override snapshot, already set snapshot id=%s", snapshotId());

    return useSnapshot(SnapshotUtil.snapshotIdAsOfTime(table(), timestampMillis));
  }

  @Override
  public CloseableIterable<T> planFiles() {
    Snapshot snapshot = snapshot();

    if (snapshot == null) {
      LOG.info("Scanning empty table {}", table());
      return CloseableIterable.empty();
    }

    LOG.info(
        "Scanning table {} snapshot {} created at {} with filter {}",
        table(),
        snapshot.snapshotId(),
        DateTimeUtil.formatTimestampMillis(snapshot.timestampMillis()),
        ExpressionUtil.toSanitizedString(filter()));

    Listeners.notifyAll(new ScanEvent(table().name(), snapshot.snapshotId(), filter(), schema()));
    List<Integer> projectedFieldIds = Lists.newArrayList(TypeUtil.getProjectedIds(schema()));
    List<String> projectedFieldNames =
        projectedFieldIds.stream().map(schema()::findColumnName).collect(Collectors.toList());

    Timer.Timed planningDuration = scanMetrics().totalPlanningDuration().start();

    return CloseableIterable.whenComplete(
        doPlanFiles(),
        () -> {
          planningDuration.stop();
          Map<String, String> metadata = Maps.newHashMap(context().options());
          metadata.putAll(EnvironmentContext.get());
          ScanReport scanReport =
              ImmutableScanReport.builder()
                  .schemaId(schema().schemaId())
                  .projectedFieldIds(projectedFieldIds)
                  .projectedFieldNames(projectedFieldNames)
                  .tableName(table().name())
                  .snapshotId(snapshot.snapshotId())
                  .filter(
                      ExpressionUtil.sanitize(
                          schema().asStruct(), filter(), context().caseSensitive()))
                  .scanMetrics(ScanMetricsResult.fromScanMetrics(scanMetrics()))
                  .metadata(metadata)
                  .build();
          context().metricsReporter().report(scanReport);
        });
  }

  public Snapshot snapshot() {
    Snapshot snapshot = tableSnapshot();
    if (snapshot == null) {
      return null;
    }

    IndexSnapshot indexSnapshot = indexSelection(snapshot).indexSnapshot();
    return indexSnapshot != null ? indexSnapshot.snapshot() : snapshot;
  }

  /**
   * Whether tasks planned for this scan must read the referenced data files immediately.
   *
   * <p>This is true when a skipping index is selected: the index serves the scan's filter columns
   * from its optimized columns and records the data file location and row position for each indexed
   * row, but does not cover all of the projected columns. In that case the index is used to locate
   * matching rows and the original data files are read immediately from the positions recorded in
   * the index. A covering index, which serves both the filter and the projection, does not trigger
   * an immediate read.
   *
   * @return true if planned {@link FileScanTask}s should read data files immediately
   */
  protected boolean immediateDataFileRead() {
    Snapshot snapshot = tableSnapshot();
    if (snapshot == null) {
      return false;
    }

    return indexSelection(snapshot).immediateDataFileRead();
  }

  private Snapshot tableSnapshot() {
    return snapshotId() != null ? table().snapshot(snapshotId()) : table().currentSnapshot();
  }

  private IndexSelection indexSelection(Snapshot snapshot) {
    if (indexSelection == null) {
      this.indexSelection = evaluateIndexes(snapshot);
    }

    return indexSelection;
  }

  private IndexSelection evaluateIndexes(Snapshot snapshot) {
    Collection<IndexDefinition> availableIndexes = context().availableIndexes();
    IndexCatalog indexCatalog = context().indexCatalog();
    if (availableIndexes == null || indexCatalog == null) {
      return IndexSelection.NONE;
    }

    Set<Integer> filterColumnIds =
        Binder.boundReferences(
            schema().asStruct(), Collections.singletonList(filter()), isCaseSensitive());
    Set<Integer> projectedColumnIds = TypeUtil.getProjectedIds(schema());

    List<IndexDefinition> applicableIndexes = Lists.newArrayList();
    for (IndexDefinition index : availableIndexes) {
      if (isApplicable(index, snapshot, filterColumnIds)) {
        applicableIndexes.add(index);
      }
    }

    // covering indexes are preferred: they serve both the filter and the projection, so the data
    // files do not need to be read
    for (IndexDefinition index : applicableIndexes) {
      if (isCoveringIndex(index, projectedColumnIds)) {
        return new IndexSelection(loadIndexSnapshot(indexCatalog, index, snapshot), false);
      }
    }

    // otherwise a skipping index is used to locate matching rows and the referenced data files are
    // read immediately from the positions recorded in the index
    for (IndexDefinition index : applicableIndexes) {
      if (isSkippingIndex(index, filterColumnIds)) {
        return new IndexSelection(loadIndexSnapshot(indexCatalog, index, snapshot), true);
      }
    }

    return IndexSelection.NONE;
  }

  /**
   * Whether the index applies to the current snapshot and serves the scan's filter columns from its
   * optimized columns. This is a precondition for both covering and skipping indexes.
   */
  private static boolean isApplicable(
      IndexDefinition index, Snapshot snapshot, Set<Integer> filterColumnIds) {
    boolean snapshotAvailable =
        Arrays.stream(index.availableTableSnapshots()).anyMatch(id -> id == snapshot.snapshotId());
    if (!snapshotAvailable) {
      return false;
    }

    return toIdSet(index.optimizedColumnIds()).containsAll(filterColumnIds);
  }

  /**
   * Whether the index covers the scan: all projected columns are available from the index and
   * optimized columns. An empty filter is allowed.
   */
  private static boolean isCoveringIndex(IndexDefinition index, Set<Integer> projectedColumnIds) {
    Set<Integer> coveredColumnIds = toIdSet(index.indexColumnIds());
    coveredColumnIds.addAll(toIdSet(index.optimizedColumnIds()));
    return coveredColumnIds.containsAll(projectedColumnIds);
  }

  /**
   * Whether the index can be used to skip to matching rows: it records the data file location and
   * row position for each indexed row. A non-empty filter is required.
   */
  private static boolean isSkippingIndex(IndexDefinition index, Set<Integer> filterColumnIds) {
    if (filterColumnIds.isEmpty()) {
      return false;
    }

    Set<Integer> indexedColumnIds = toIdSet(index.indexColumnIds());
    return indexedColumnIds.contains(MetadataColumns.INDEX_FILE_PATH.fieldId())
        && indexedColumnIds.contains(MetadataColumns.INDEX_ROW_POSITION.fieldId());
  }

  private static IndexSnapshot loadIndexSnapshot(
      IndexCatalog indexCatalog, IndexDefinition index, Snapshot snapshot) {
    return indexCatalog.loadIndex(index.id()).snapshotForTableSnapshot(snapshot.snapshotId());
  }

  private static Set<Integer> toIdSet(int[] ids) {
    return Arrays.stream(ids).boxed().collect(Collectors.toSet());
  }

  private static final class IndexSelection {
    private static final IndexSelection NONE = new IndexSelection(null, false);

    private final IndexSnapshot indexSnapshot;
    private final boolean immediateDataFileRead;

    private IndexSelection(IndexSnapshot indexSnapshot, boolean immediateDataFileRead) {
      this.indexSnapshot = indexSnapshot;
      this.immediateDataFileRead = immediateDataFileRead;
    }

    IndexSnapshot indexSnapshot() {
      return indexSnapshot;
    }

    boolean immediateDataFileRead() {
      return immediateDataFileRead;
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("table", table())
        .add("projection", schema().asStruct())
        .add("filter", filter())
        .add("ignoreResiduals", shouldIgnoreResiduals())
        .add("caseSensitive", isCaseSensitive())
        .toString();
  }
}

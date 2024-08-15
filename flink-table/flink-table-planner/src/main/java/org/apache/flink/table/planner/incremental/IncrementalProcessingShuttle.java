/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.incremental;

import org.apache.flink.configuration.IncrementalProcessingOptions;
import org.apache.flink.incremental.Offset;
import org.apache.flink.incremental.SourceOffsets;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsIncrementalScan;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.planner.hint.FlinkHints;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalSink;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.DefaultRelShuttle;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.configuration.IncrementalProcessingOptions.SCAN_RANGE_TIMESTAMP_FORMAT;

/**
 * A shuttle class used by the {@link Planner} to try generating an incremental processing plan.
 *
 * <p>This class overrides the visit method to process input RelNodes and generate new RelNodes that
 * represent an incremental version of the original plan. If an unsupported RelNode is encountered,
 * the generation of the incremental plan will be terminated.
 *
 * <p>During the generation process, the abstract time semantics are represented using the {@link
 * IncrementalTimeType} enum.
 *
 * <p>Given the following SQL:
 *
 * <pre>{@code
 * INSERT INTO sink SELECT * FROM t1 JOIN t2 ON t1.a = t2.a;
 * }</pre>
 *
 * <p>The original AST is:
 *
 * <pre>{@Code
 * LogicalSink(table=[testCatalog.default.sink], fields=[a])
 * +- LogicalProject(a=[$0])
 *    +- LogicalJoin(condition=[=($0, $1)], joinType=[inner])
 *       :- LogicalTableScan(table=[[testCatalog, default, t1]])
 *       +- LogicalTableScan(table=[[testCatalog, default, t2]])
 * }</pre>
 *
 * <p>The incremental plan will be:
 *
 * <pre>{@Code
 * LogicalSink(table=[testCatalog.default.sink], fields=[a])
 * +- LogicalProject(a=[$0])
 *    +- LogicalUnion(all=[true])
 *       :- LogicalJoin(condition=[=($0, $1)], joinType=[inner])
 *       :  :- LogicalTableScan(table=[[testCatalog, default, t1]], hints=[[[OPTIONS inheritPath:[] options:{scan-mode=FULL_NEW}]]])
 *       :  +- LogicalTableScan(table=[[testCatalog, default, t2]], hints=[[[OPTIONS inheritPath:[] options:{scan-mode=DELTA}]]])
 *       +- LogicalJoin(condition=[=($0, $1)], joinType=[inner])
 *          :- LogicalTableScan(table=[[testCatalog, default, t1]], hints=[[[OPTIONS inheritPath:[] options:{scan-mode=DELTA}]]])
 *          +- LogicalTableScan(table=[[testCatalog, default, t2]], hints=[[[OPTIONS inheritPath:[] options:{scan-mode=FULL_OLD}]]])
 * }</pre>
 *
 * <p>To use {@link LogicalTableScan}s in an incremental processing plan, the corresponding table
 * sources must implement the {@link SupportsIncrementalScan} interface. This shuttle interacts with
 * the sources to obtain {@link Offset}s and sets the scan ranges for the sources.
 *
 * @see IncrementalTimeType
 * @see SupportsIncrementalScan
 */
public class IncrementalProcessingShuttle extends DefaultRelShuttle {
    private static final Logger LOG = LoggerFactory.getLogger(IncrementalProcessingShuttle.class);

    /** The hint is used to avoid table source scan reuse. */
    private static final String SCAN_MODE_HINT_NAME = "scan-mode";

    private final Map<RelNode, IncrementalTimeType> timeTypeMap = new HashMap<>();
    private final TableConfig tableConfig;
    @Nullable private final SourceOffsets restoredOffsets;

    /** Cache source offsets to avoid repeated interaction with the sources. */
    private final SourceOffsets cachedStartOffsets;

    /** Cache source offsets to avoid repeated interaction with the sources. */
    private final SourceOffsets cachedEndOffsets;

    public IncrementalProcessingShuttle(
            TableConfig tableConfig,
            @Nullable SourceOffsets restoredOffsets,
            SourceOffsets cachedStartOffsets,
            SourceOffsets cachedEndOffsets) {
        super();
        this.tableConfig = tableConfig;
        this.restoredOffsets = restoredOffsets;
        this.cachedStartOffsets = cachedStartOffsets;
        this.cachedEndOffsets = cachedEndOffsets;
    }

    private RelNode defaultVisit(RelNode rel, IncrementalTimeType targetTimeType) {
        List<RelNode> newInputs = new ArrayList<>();
        for (RelNode input : rel.getInputs()) {
            timeTypeMap.put(input, targetTimeType);
            RelNode newInput = input.accept(this);
            if (newInput == null) {
                return null;
            }
            newInputs.add(newInput);
        }
        return rel.copy(rel.getTraitSet(), newInputs);
    }

    private RelNode visitJoin(LogicalJoin join, IncrementalTimeType targetTimeType) {
        if (targetTimeType == IncrementalTimeType.DELTA) {
            RelNode leftL = join.getLeft();
            timeTypeMap.put(leftL, IncrementalTimeType.FULL_NEW);
            RelNode newLeftL = leftL.accept(this);
            if (newLeftL == null) {
                return null;
            }
            RelNode rightL = join.getRight();
            timeTypeMap.put(rightL, IncrementalTimeType.DELTA);
            RelNode newRightL = rightL.accept(this);
            if (newRightL == null) {
                return null;
            }
            RelNode joinL = join.copy(join.getTraitSet(), Arrays.asList(newLeftL, newRightL));

            RelNode leftR = leftL.copy(leftL.getTraitSet(), leftL.getInputs());
            timeTypeMap.put(leftR, IncrementalTimeType.DELTA);
            RelNode newLeftR = leftR.accept(this);
            if (newLeftR == null) {
                return null;
            }
            RelNode rightR = rightL.copy(rightL.getTraitSet(), rightL.getInputs());
            timeTypeMap.put(rightR, IncrementalTimeType.FULL_OLD);
            RelNode newRightR = rightR.accept(this);
            if (newRightR == null) {
                return null;
            }
            RelNode joinR = join.copy(join.getTraitSet(), Arrays.asList(newLeftR, newRightR));
            return LogicalUnion.create(Arrays.asList(joinL, joinR), true);
        } else {
            return defaultVisit(join, targetTimeType);
        }
    }

    private Offset tryGetScanRangeStart(String sourceName, SupportsIncrementalScan tableSource) {
        if (restoredOffsets == null) {
            String raw =
                    tableConfig.get(
                            IncrementalProcessingOptions
                                    .INCREMENTAL_PROCESSING_INIT_SCAN_RANGE_START_TIMESTAMP);
            if (raw == null) {
                raw =
                        tableConfig.get(
                                IncrementalProcessingOptions
                                        .INCREMENTAL_PROCESSING_SCAN_RANGE_START_TIMESTAMP);
            }
            if (raw.equals(IncrementalProcessingOptions.SCAN_RANGE_START_AUTO)
                    || raw.equals(IncrementalProcessingOptions.SCAN_RANGE_START_EARLIEST)) {
                return new Offset();
            }
            return tableSource.getOffset(tryParseTimestamp(raw));
        }
        String raw =
                tableConfig.get(
                        IncrementalProcessingOptions
                                .INCREMENTAL_PROCESSING_SCAN_RANGE_START_TIMESTAMP);
        if (raw.equals(IncrementalProcessingOptions.SCAN_RANGE_START_AUTO)) {
            if (!restoredOffsets.containsOffset(sourceName)) {
                throw new IllegalStateException(
                        "The restored source offsets do not contain source " + sourceName);
            }
            return restoredOffsets.getOffset(sourceName);
        } else if (raw.equals(IncrementalProcessingOptions.SCAN_RANGE_START_EARLIEST)) {
            return new Offset();
        }
        return tableSource.getOffset(tryParseTimestamp(raw));
    }

    private Offset tryGetScanRangeEnd(SupportsIncrementalScan tableSource) {
        String raw = null;
        if (restoredOffsets == null) {
            raw =
                    tableConfig.get(
                            IncrementalProcessingOptions
                                    .INCREMENTAL_PROCESSING_INIT_SCAN_RANGE_END_TIMESTAMP);
        }
        if (raw == null) {
            raw =
                    tableConfig.get(
                            IncrementalProcessingOptions
                                    .INCREMENTAL_PROCESSING_SCAN_RANGE_END_TIMESTAMP);
        }
        if (raw.equals(IncrementalProcessingOptions.SCAN_RANGE_END_LATEST)) {
            return tableSource.getEndOffset();
        }
        return tableSource.getOffset(tryParseTimestamp(raw));
    }

    private long tryParseTimestamp(String raw) {
        try {
            LocalDateTime dateTime =
                    LocalDateTime.parse(
                            raw, DateTimeFormatter.ofPattern(SCAN_RANGE_TIMESTAMP_FORMAT));
            return dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(
                    "Invalid timestamp format: "
                            + raw
                            + ", should be: "
                            + SCAN_RANGE_TIMESTAMP_FORMAT);
        }
    }

    private RelNode visitTableScan(LogicalTableScan tableScan, IncrementalTimeType targetTimeType) {
        // Checks whether the source table supports incremental scan.
        TableSourceTable tableSourceTable = tableScan.getTable().unwrap(TableSourceTable.class);
        if (tableSourceTable == null
                || !(tableSourceTable.tableSource() instanceof SupportsIncrementalScan)) {
            return null;
        }

        String sourceName = tableSourceTable.contextResolvedTable().getIdentifier().toString();
        SupportsIncrementalScan tableSource =
                (SupportsIncrementalScan) tableSourceTable.tableSource();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Found a qualified LogicalTableScan for {}", sourceName);
        }

        Offset startOffset, endOffset;
        if (cachedStartOffsets.containsOffset(sourceName)) {
            startOffset = cachedStartOffsets.getOffset(sourceName);
        } else {
            startOffset = tryGetScanRangeStart(sourceName, tableSource);
            cachedStartOffsets.setOffset(sourceName, startOffset);
        }
        if (cachedEndOffsets.containsOffset(sourceName)) {
            endOffset = cachedEndOffsets.getOffset(sourceName);
        } else {
            endOffset = tryGetScanRangeEnd(tableSource);
            cachedEndOffsets.setOffset(sourceName, endOffset);
        }

        Offset scanStartOffset =
                (targetTimeType == IncrementalTimeType.DELTA ? startOffset : new Offset());
        Offset scanEndOffset =
                (targetTimeType == IncrementalTimeType.FULL_OLD ? startOffset : endOffset);
        DynamicTableSource newTableSource =
                tableSource.withScanRange(scanStartOffset, scanEndOffset);
        if (newTableSource == null) {
            return null;
        }

        TableSourceTable newTableSourceTable =
                tableSourceTable.copy(
                        newTableSource,
                        tableSourceTable.getStatistic(),
                        tableSourceTable.abilitySpecs());
        Map<String, String> hint = new HashMap<>();
        hint.put(SCAN_MODE_HINT_NAME, targetTimeType.name());
        List<RelHint> hints = tableScan.getHints();
        List<RelHint> newHints = new ArrayList<>(hints);
        newHints.add(RelHint.builder(FlinkHints.HINT_NAME_OPTIONS).hintOptions(hint).build());
        LogicalTableScan newTableScan =
                LogicalTableScan.create(tableScan.getCluster(), newTableSourceTable, newHints);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Finished processing a LogicalTableScan for {}.", sourceName);
        }
        return newTableScan;
    }

    @Override
    public RelNode visit(RelNode rel) {
        IncrementalTimeType targetTimeType =
                timeTypeMap.getOrDefault(rel, IncrementalTimeType.DELTA);
        if (rel instanceof LogicalSink || rel instanceof LogicalProject) {
            return defaultVisit(rel, targetTimeType);
        } else if (rel instanceof LogicalJoin) {
            // Only supports INNER join for now.
            LogicalJoin join = (LogicalJoin) rel;
            if (join.getJoinType() != JoinRelType.INNER) {
                return null;
            }
            return visitJoin(join, targetTimeType);
        } else if (rel instanceof LogicalTableScan) {
            return visitTableScan((LogicalTableScan) rel, targetTimeType);
        }
        // TODO Support other RelNodes
        return null;
    }
}

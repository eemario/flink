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

import org.apache.flink.configuration.BatchIncrementalExecutionOptions;
import org.apache.flink.incremental.SourceOffsets;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsScanRange;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.planner.hint.FlinkHints;
import org.apache.flink.table.planner.plan.abilities.sink.OverwriteSpec;
import org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalSink;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.DefaultRelShuttle;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.SqlKind;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import scala.collection.JavaConverters;

import static org.apache.flink.configuration.BatchIncrementalExecutionOptions.SCAN_RANGE_TIMESTAMP_FORMAT;

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
 * INSERT OVERWRITE sink SELECT * FROM t1 JOIN t2 ON t1.a = t2.a;
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
 * <p>Note that when running incremental processing for the first time, the FULL_OLD side will have
 * no data and can be removed from the plan.
 *
 * <p>To use {@link LogicalTableScan}s in an incremental processing plan, the corresponding table
 * sources must implement the {@link SupportsScanRange} interface. This shuttle interacts with the
 * sources to set the scan ranges for the sources.
 *
 * @see IncrementalTimeType
 * @see SupportsScanRange
 */
public class IncrementalProcessingShuttle extends DefaultRelShuttle {
    private static final Logger LOG = LoggerFactory.getLogger(IncrementalProcessingShuttle.class);

    /** The hint is used to avoid table source scan reuse. */
    private static final String SCAN_MODE_HINT_NAME = "scan-mode";

    private static final long EARLIEST = -1;

    private static final Set<SqlKind> SUPPORTED_REXCALL_KINDS =
            new HashSet<>(Arrays.asList(SqlKind.EQUALS));

    private final Map<RelNode, IncrementalTimeType> timeTypeMap = new HashMap<>();
    private final TableConfig tableConfig;
    @Nullable private final SourceOffsets restoredOffsets;

    /** Cache source offsets to avoid repeated interaction with the sources. */
    private final SourceOffsets cachedStartOffsets;

    /** Cache source offsets to avoid repeated interaction with the sources. */
    private final SourceOffsets cachedEndOffsets;

    /** Empty RelNodes that can be removed in DELTA join. */
    private final Set<RelNode> emptyRelNodeSet = new HashSet<>();

    public IncrementalProcessingShuttle(
            TableConfig tableConfig, @Nullable SourceOffsets restoredOffsets) {
        super();
        this.tableConfig = tableConfig;
        this.restoredOffsets = restoredOffsets;
        this.cachedStartOffsets = new SourceOffsets();
        this.cachedEndOffsets = new SourceOffsets();
    }

    public SourceOffsets getCachedStartOffsets() {
        return cachedStartOffsets;
    }

    public SourceOffsets getCachedEndOffsets() {
        return cachedEndOffsets;
    }

    @Override
    public RelNode visit(RelNode rel) {
        // default DELTA is for root node
        IncrementalTimeType targetTimeType =
                timeTypeMap.getOrDefault(rel, IncrementalTimeType.DELTA);
        if (rel instanceof LogicalProject) {
            return defaultVisit(rel, targetTimeType);
        } else if (rel instanceof LogicalSink) {
            return visitSink((LogicalSink) rel, targetTimeType);
        } else if (rel instanceof LogicalJoin) {
            // Only supports INNER join for now
            LogicalJoin join = (LogicalJoin) rel;
            if (join.getJoinType() != JoinRelType.INNER) {
                return null;
            }
            return visitInnerJoin(join, targetTimeType);
        } else if (rel instanceof LogicalTableScan) {
            return visitTableScan((LogicalTableScan) rel, targetTimeType);
        } else if (rel instanceof LogicalFilter) {
            return visitFilter((LogicalFilter) rel, targetTimeType);
        }
        // TODO Support other RelNodes
        LOG.info("Unsupported RelNode: {}.", rel.getClass().getName());
        return null;
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
        RelNode newRelNode = rel.copy(rel.getTraitSet(), newInputs);
        if (emptyRelNodeSet.containsAll(newInputs)) {
            emptyRelNodeSet.add(newRelNode);
        }
        return newRelNode;
    }

    private RelNode visitSink(LogicalSink sink, IncrementalTimeType targetTimeType) {
        // checks that the sink has OverwriteSpec with overwrite = true, removes the spec and sets
        // overwrite to false for the sink
        List<SinkAbilitySpec> newSinkAbilitySpecs = new ArrayList<>();
        for (SinkAbilitySpec sinkAbilitySpec : sink.abilitySpecs()) {
            if (!(sinkAbilitySpec instanceof OverwriteSpec)
                    || !sinkAbilitySpec.equals(new OverwriteSpec(true))) {
                newSinkAbilitySpecs.add(sinkAbilitySpec);
            }
        }
        if (newSinkAbilitySpecs.size() == sink.abilitySpecs().length
                || !(sink.tableSink() instanceof SupportsOverwrite)) {
            LOG.info(
                    "Only supports incremental processing for INSERT OVERWRITE, but the sink for table {} is not an overwrite sink.",
                    sink.contextResolvedTable().getIdentifier());
            return null;
        }
        ((SupportsOverwrite) sink.tableSink()).applyOverwrite(false);

        RelNode input = sink.getInput();
        timeTypeMap.put(input, targetTimeType);
        RelNode newInput = input.accept(this);
        if (newInput == null) {
            return null;
        }
        return LogicalSink.create(
                newInput,
                sink.hints(),
                sink.contextResolvedTable(),
                sink.tableSink(),
                JavaConverters.mapAsJavaMap(sink.staticPartitions()),
                sink.targetColumns(),
                newSinkAbilitySpecs.toArray(new SinkAbilitySpec[0]));
    }

    private RelNode visitInnerJoin(LogicalJoin join, IncrementalTimeType targetTimeType) {
        if (targetTimeType != IncrementalTimeType.DELTA) {
            return defaultVisit(join, targetTimeType);
        } else {
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

            RelNode rightR = rightL.copy(rightL.getTraitSet(), rightL.getInputs());
            timeTypeMap.put(rightR, IncrementalTimeType.FULL_OLD);
            RelNode newRightR = rightR.accept(this);
            if (newRightR == null) {
                return null;
            }
            if (emptyRelNodeSet.contains(newRightR)) {
                // if the FULL_OLD side is empty, which indicates that this is the first run of
                // incremental processing, the DELTA join plan can be simplified
                return joinL;
            }

            RelNode leftR = leftL.copy(leftL.getTraitSet(), leftL.getInputs());
            timeTypeMap.put(leftR, IncrementalTimeType.DELTA);
            RelNode newLeftR = leftR.accept(this);
            if (newLeftR == null) {
                return null;
            }
            RelNode joinR = join.copy(join.getTraitSet(), Arrays.asList(newLeftR, newRightR));
            return LogicalUnion.create(Arrays.asList(joinL, joinR), true);
        }
    }

    private RelNode visitTableScan(LogicalTableScan tableScan, IncrementalTimeType targetTimeType) {
        // Checks whether the table source supports scan range
        TableSourceTable tableSourceTable = tableScan.getTable().unwrap(TableSourceTable.class);
        if (tableSourceTable == null
                || !(tableSourceTable.tableSource() instanceof SupportsScanRange)) {
            LOG.info(
                    "The source for table {} does not support scan range. Failed to generate an incremental plan.",
                    tableScan.getTable().getQualifiedName());
            return null;
        }

        // TODO optimize this check after supporting retract data
        if (tableSourceTable
                .contextResolvedTable()
                .getResolvedTable()
                .getResolvedSchema()
                .getPrimaryKey()
                .isPresent()) {
            LOG.info(
                    "The query contains primary key table {}. Failed to generate an incremental plan.",
                    tableScan.getTable().getQualifiedName());
            return null;
        }

        ObjectIdentifier sourceIdentifier = tableSourceTable.contextResolvedTable().getIdentifier();
        Optional<Catalog> optionalCatalog = tableSourceTable.contextResolvedTable().getCatalog();
        if (!optionalCatalog.isPresent()) {
            LOG.info(
                    "The table {} has no catalog. Failed to generate an incremental plan.",
                    sourceIdentifier);
            return null;
        }

        Catalog catalog = optionalCatalog.get();
        long startOffset, endOffset;
        String sourceName = sourceIdentifier.toString();
        // uses cached offsets if exist
        if (cachedStartOffsets.containsOffset(sourceName)) {
            startOffset = cachedStartOffsets.getOffset(sourceName);
        } else {
            startOffset = getScanRangeStart(sourceIdentifier, catalog);
            cachedStartOffsets.setOffset(sourceName, startOffset);
        }
        if (cachedEndOffsets.containsOffset(sourceName)) {
            endOffset = cachedEndOffsets.getOffset(sourceName);
        } else {
            endOffset = getScanRangeEnd(sourceIdentifier, catalog);
            cachedEndOffsets.setOffset(sourceName, endOffset);
        }

        // sets scan range for the table source
        long scanStartOffset =
                (targetTimeType == IncrementalTimeType.DELTA ? startOffset : EARLIEST);
        long scanEndOffset =
                (targetTimeType == IncrementalTimeType.FULL_OLD ? startOffset : endOffset);
        ScanTableSource newTableSource;
        try {
            newTableSource =
                    ((SupportsScanRange) tableSourceTable.tableSource())
                            .applyScanRange(scanStartOffset, scanEndOffset);
        } catch (Exception e) {
            LOG.info("Failed to set scan range for table source {}.", sourceIdentifier, e);
            return null;
        }

        // creates a new TableSourceTable and LogicalTableScan
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

        if (scanEndOffset == EARLIEST) {
            LOG.info(
                    "The {} scan range for table {} is ({},{}] and may be optimized in DELTA join",
                    targetTimeType,
                    sourceIdentifier,
                    scanStartOffset,
                    scanEndOffset);
            emptyRelNodeSet.add(newTableScan);
        }
        return newTableScan;
    }

    private RelNode visitFilter(LogicalFilter filter, IncrementalTimeType targetTimeType) {
        // Only supports SUPPORTED_REXCALL_KINDS without sub queries for now
        if (filter.getCondition() instanceof RexCall) {
            RexCall rexCall = (RexCall) filter.getCondition();
            if (SUPPORTED_REXCALL_KINDS.contains(rexCall.getKind())) {
                if (!rexCall.getOperands().stream().anyMatch(r -> r instanceof RexSubQuery)) {
                    return defaultVisit(filter, targetTimeType);
                }
            }
        }
        LOG.info(
                "Only supports filter {} without sub queries, but meets {}.",
                SUPPORTED_REXCALL_KINDS,
                filter.explain());
        return null;
    }

    private long getScanRangeStart(ObjectIdentifier sourceIdentifier, Catalog catalog) {
        String raw =
                tableConfig.get(
                        BatchIncrementalExecutionOptions.INCREMENTAL_SCAN_RANGE_START_TIMESTAMP);
        if (raw.equals(BatchIncrementalExecutionOptions.SCAN_RANGE_START_EARLIEST)) {
            return EARLIEST;
        }

        if (raw.equals(BatchIncrementalExecutionOptions.SCAN_RANGE_START_AUTO)) {
            if (restoredOffsets == null) {
                return EARLIEST;
            }
            String sourceName = sourceIdentifier.toString();
            if (!restoredOffsets.containsOffset(sourceName)) {
                throw new IllegalStateException(
                        "The restored source offsets do not contain " + sourceName);
            }
            return restoredOffsets.getOffset(sourceName);
        }

        try {
            return catalog.getTableTimestamp(sourceIdentifier.toObjectPath(), parseTimestamp(raw));
        } catch (TableNotExistException e) {
            throw new IllegalStateException(
                    "The source table "
                            + sourceIdentifier
                            + " does not exist in the catalog. This should not happen.");
        }
    }

    private long getScanRangeEnd(ObjectIdentifier sourceIdentifier, Catalog catalog) {
        String raw =
                tableConfig.get(
                        BatchIncrementalExecutionOptions.INCREMENTAL_SCAN_RANGE_END_TIMESTAMP);
        long timestamp;
        if (raw.equals(BatchIncrementalExecutionOptions.SCAN_RANGE_END_LATEST)) {
            timestamp = System.currentTimeMillis();
        } else {
            timestamp = parseTimestamp(raw);
        }

        try {
            return catalog.getTableTimestamp(sourceIdentifier.toObjectPath(), timestamp);
        } catch (TableNotExistException e) {
            throw new IllegalStateException(
                    "The source table "
                            + sourceIdentifier
                            + " does not exist in the catalog. This should not happen.");
        }
    }

    private long parseTimestamp(String raw) {
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
}

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
package org.apache.flink.table.planner.plan.rules

import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalAggregate, FlinkLogicalCalc, FlinkLogicalCorrelate, FlinkLogicalDataStreamTableScan, FlinkLogicalDistribution, FlinkLogicalExpand, FlinkLogicalIntermediateTableScan, FlinkLogicalJoin, FlinkLogicalLegacySink, FlinkLogicalLegacyTableSourceScan, FlinkLogicalMatch, FlinkLogicalOverAggregate, FlinkLogicalRank, FlinkLogicalScriptTransform, FlinkLogicalSink, FlinkLogicalSnapshot, FlinkLogicalSort, FlinkLogicalTableFunctionScan, FlinkLogicalTableSourceScan, FlinkLogicalUnion, FlinkLogicalValues, FlinkLogicalWindowAggregate}
import org.apache.flink.table.planner.plan.rules.physical.FlinkExpandConversionRule
import org.apache.flink.table.planner.plan.rules.physical.batch.{BatchPhysicalBoundedStreamScanRule, BatchPhysicalCalcRule, BatchPhysicalConstantTableFunctionScanRule, BatchPhysicalCorrelateRule, BatchPhysicalDistributionRule, BatchPhysicalExpandRule, BatchPhysicalHashAggRule, BatchPhysicalHashJoinRule, BatchPhysicalIntermediateTableScanRule, BatchPhysicalLegacySinkRule, BatchPhysicalLegacyTableSourceScanRule, BatchPhysicalLimitRule, BatchPhysicalLookupJoinRule, BatchPhysicalMatchRule, BatchPhysicalNestedLoopJoinRule, BatchPhysicalOverAggregateRule, BatchPhysicalPythonAggregateRule, BatchPhysicalPythonCalcRule, BatchPhysicalPythonCorrelateRule, BatchPhysicalPythonWindowAggregateRule, BatchPhysicalRankRule, BatchPhysicalScriptTransformRule, BatchPhysicalSingleRowJoinRule, BatchPhysicalSinkRule, BatchPhysicalSortAggRule, BatchPhysicalSortLimitRule, BatchPhysicalSortMergeJoinRule, BatchPhysicalSortRule, BatchPhysicalTableSourceScanRule, BatchPhysicalUnionRule, BatchPhysicalValuesRule, BatchPhysicalWindowAggregateRule, BatchPhysicalWindowTableFunctionRule, EnforceLocalHashAggRule, EnforceLocalSortAggRule, PushLocalHashAggIntoScanRule, PushLocalHashAggWithCalcIntoScanRule, PushLocalSortAggIntoScanRule, PushLocalSortAggWithCalcIntoScanRule, PushLocalSortAggWithSortAndCalcIntoScanRule, PushLocalSortAggWithSortIntoScanRule, RemoveRedundantLocalHashAggRule, RemoveRedundantLocalRankRule, RemoveRedundantLocalSortAggRule}
import org.apache.flink.table.planner.plan.rules.physical.incremental.TwoStageOptimizedAggregateRule
import org.apache.flink.table.planner.plan.rules.physical.stream.StreamPhysicalGroupAggregateRule

import org.apache.calcite.tools.{RuleSet, RuleSets}

import scala.collection.JavaConverters._

object FlinkIncrementalRuleSets {

  /** RuleSet to translate calcite nodes to flink nodes */
  private val LOGICAL_CONVERTERS: RuleSet = RuleSets.ofList(
    FlinkLogicalAggregate.STREAM_CONVERTER, // hack for incremental
    FlinkLogicalOverAggregate.CONVERTER,
    FlinkLogicalCalc.CONVERTER,
    FlinkLogicalCorrelate.CONVERTER,
    FlinkLogicalJoin.CONVERTER,
    FlinkLogicalSort.BATCH_CONVERTER,
    FlinkLogicalUnion.CONVERTER,
    FlinkLogicalValues.CONVERTER,
    FlinkLogicalTableSourceScan.CONVERTER,
    FlinkLogicalLegacyTableSourceScan.CONVERTER,
    FlinkLogicalTableFunctionScan.CONVERTER,
    FlinkLogicalDataStreamTableScan.CONVERTER,
    FlinkLogicalIntermediateTableScan.CONVERTER,
    FlinkLogicalExpand.CONVERTER,
    FlinkLogicalRank.CONVERTER,
    FlinkLogicalWindowAggregate.CONVERTER,
    FlinkLogicalSnapshot.CONVERTER,
    FlinkLogicalMatch.CONVERTER,
    FlinkLogicalSink.CONVERTER,
    FlinkLogicalLegacySink.CONVERTER,
    FlinkLogicalDistribution.BATCH_CONVERTER,
    FlinkLogicalScriptTransform.BATCH_CONVERTER
  )

  /** RuleSet to do logical optimize for incremental */
  val LOGICAL_OPT_RULES: RuleSet = RuleSets.ofList(
    (
      FlinkBatchRuleSets.LIMIT_RULES.asScala ++
        FlinkBatchRuleSets.FILTER_RULES.asScala ++
        FlinkBatchRuleSets.PROJECT_RULES.asScala ++
        FlinkBatchRuleSets.PRUNE_EMPTY_RULES.asScala ++
        FlinkBatchRuleSets.LOGICAL_RULES.asScala ++
        LOGICAL_CONVERTERS.asScala // hack for incremental
    ).asJava)

  /** RuleSet to do physical optimize for incremental */
  val PHYSICAL_OPT_RULES: RuleSet = RuleSets.ofList(
    FlinkExpandConversionRule.BATCH_INSTANCE,
    // source
    BatchPhysicalBoundedStreamScanRule.INSTANCE,
    BatchPhysicalTableSourceScanRule.INSTANCE,
    BatchPhysicalLegacyTableSourceScanRule.INSTANCE,
    BatchPhysicalIntermediateTableScanRule.INSTANCE,
    BatchPhysicalValuesRule.INSTANCE,
    // calc
    BatchPhysicalCalcRule.INSTANCE,
    BatchPhysicalPythonCalcRule.INSTANCE,
    // union
    BatchPhysicalUnionRule.INSTANCE,
    // sort
    BatchPhysicalSortRule.INSTANCE,
    BatchPhysicalLimitRule.INSTANCE,
    BatchPhysicalSortLimitRule.INSTANCE,
    // rank
    BatchPhysicalRankRule.INSTANCE,
    RemoveRedundantLocalRankRule.INSTANCE,
    // expand
    BatchPhysicalExpandRule.INSTANCE,
    // group agg
//    BatchPhysicalHashAggRule.INSTANCE,
//    BatchPhysicalSortAggRule.INSTANCE,
    StreamPhysicalGroupAggregateRule.INCREMENTAL_INSTANCE, // hack for incremental
    RemoveRedundantLocalSortAggRule.WITHOUT_SORT,
    RemoveRedundantLocalSortAggRule.WITH_SORT,
    RemoveRedundantLocalHashAggRule.INSTANCE,
    BatchPhysicalPythonAggregateRule.INSTANCE,
    // over agg
    BatchPhysicalOverAggregateRule.INSTANCE,
    // window agg
    BatchPhysicalWindowAggregateRule.INSTANCE,
    BatchPhysicalPythonWindowAggregateRule.INSTANCE,
    // window tvf
    BatchPhysicalWindowTableFunctionRule.INSTANCE,
    // join
    BatchPhysicalHashJoinRule.INSTANCE,
    BatchPhysicalSortMergeJoinRule.INSTANCE,
    BatchPhysicalNestedLoopJoinRule.INSTANCE,
    BatchPhysicalSingleRowJoinRule.INSTANCE,
    BatchPhysicalLookupJoinRule.SNAPSHOT_ON_TABLESCAN,
    BatchPhysicalLookupJoinRule.SNAPSHOT_ON_CALC_TABLESCAN,
    // CEP
    BatchPhysicalMatchRule.INSTANCE,
    // correlate
    BatchPhysicalConstantTableFunctionScanRule.INSTANCE,
    BatchPhysicalCorrelateRule.INSTANCE,
    BatchPhysicalPythonCorrelateRule.INSTANCE,
    // sink
    BatchPhysicalSinkRule.INSTANCE,
    BatchPhysicalLegacySinkRule.INSTANCE,
    // hive distribution
    BatchPhysicalDistributionRule.INSTANCE,
    // hive transform
    BatchPhysicalScriptTransformRule.INSTANCE
  )

  /** RuleSet to optimize plans after batch exec execution. */
  val PHYSICAL_REWRITE: RuleSet = RuleSets.ofList(
    EnforceLocalHashAggRule.INSTANCE,
    EnforceLocalSortAggRule.INSTANCE,
    PushLocalHashAggIntoScanRule.INSTANCE,
    PushLocalHashAggWithCalcIntoScanRule.INSTANCE,
    PushLocalSortAggIntoScanRule.INSTANCE,
    PushLocalSortAggWithSortIntoScanRule.INSTANCE,
    PushLocalSortAggWithCalcIntoScanRule.INSTANCE,
    PushLocalSortAggWithSortAndCalcIntoScanRule.INSTANCE,
    // optimize agg rule
    TwoStageOptimizedAggregateRule.INSTANCE // special rule for incremental
  )
}

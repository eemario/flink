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
package org.apache.flink.table.planner.plan.optimize

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.catalog.{CatalogManager, FunctionCatalog}
import org.apache.flink.table.module.ModuleManager
import org.apache.flink.table.planner.calcite.{FlinkRelBuilder, RexFactory}
import org.apache.flink.table.planner.delegation.IncrementalPlanner
import org.apache.flink.table.planner.plan.`trait`.{ModifyKindSet, ModifyKindSetTraitDef, UpdateKind, UpdateKindTraitDef}
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.calcite.{LegacySink, Sink}
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamPhysicalDataStreamScan, StreamPhysicalIntermediateTableScan, StreamPhysicalLegacyTableSourceScan, StreamPhysicalRel, StreamPhysicalTableSourceScan}
import org.apache.flink.table.planner.plan.optimize.program.{BatchOptimizeContext, FlinkIncrementalProgram, IncrementalOptimizeContext}
import org.apache.flink.table.planner.plan.schema.IntermediateRelTable
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapContext
import org.apache.flink.table.planner.utils.TableConfigUtils
import org.apache.flink.util.Preconditions

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.TableScan

import java.util
import java.util.Collections

import scala.collection.JavaConversions._

/** A [[CommonSubGraphBasedOptimizer]] for Incremental. */
class IncrementalCommonSubGraphBasedOptimizer(planner: IncrementalPlanner)
  extends CommonSubGraphBasedOptimizer {

  override protected def doOptimize(roots: Seq[RelNode]): Seq[RelNodeBlock] = {
    // build RelNodeBlock plan
    val sinkBlocks =
      RelNodeBlockPlanBuilder.buildRelNodeBlockPlan(roots, planner.getTableConfig)
    // optimize recursively RelNodeBlock
    optimizeSinkBlocks(sinkBlocks)
  }

  private def optimizeSinkBlocks(sinkBlocks: Seq[RelNodeBlock]): Seq[RelNodeBlock] = {
    // infer trait properties for sink block
    sinkBlocks.foreach {
      sinkBlock =>
        // don't require update before by default
        sinkBlock.setUpdateBeforeRequired(false)
    }

    if (sinkBlocks.size == 1) {
      // If there is only one sink block, the given relational expressions are a simple tree
      // (only one root), not a dag. So many operations (e.g. infer and propagate
      // requireUpdateBefore) can be omitted to save optimization time.
      val block = sinkBlocks.head
      val optimizedTree =
        optimizeTree(block.getPlan, block.isUpdateBeforeRequired, isSinkBlock = true)
      block.setOptimizedPlan(optimizedTree)
      return sinkBlocks
    }
    // TODO FLINK-24048: Move changeLog inference out of optimizing phase
    // infer modifyKind property for each blocks independently
    sinkBlocks.foreach(b => optimizeBlock(b, isSinkBlock = true))
    // infer and propagate updateKind property for each blocks
    sinkBlocks.foreach(b => propagateUpdateKind(b, b.isUpdateBeforeRequired, isSinkBlock = true))
    // clear the intermediate result
    sinkBlocks.foreach(resetIntermediateResult)
    // optimize recursively RelNodeBlock
    sinkBlocks.foreach(b => optimizeBlock(b, isSinkBlock = true))
    sinkBlocks
  }

  private def optimizeBlock(block: RelNodeBlock, isSinkBlock: Boolean): Unit = {
    block.children.foreach {
      child =>
        if (child.getNewOutputNode.isEmpty) {
          optimizeBlock(child, isSinkBlock = false)
        }
    }

    val blockLogicalPlan = block.getPlan
    blockLogicalPlan match {
      case _: LegacySink | _: Sink =>
        require(isSinkBlock)
        val optimizedTree = optimizeTree(
          blockLogicalPlan,
          updateBeforeRequired = block.isUpdateBeforeRequired,
          isSinkBlock = true)
        block.setOptimizedPlan(optimizedTree)

      case o =>
        val optimizedPlan = optimizeTree(
          o,
          updateBeforeRequired = block.isUpdateBeforeRequired,
          isSinkBlock = isSinkBlock)
        val modifyKindSetTrait = optimizedPlan.getTraitSet.getTrait(ModifyKindSetTraitDef.INSTANCE)
        val name = createUniqueIntermediateRelTableName
        val intermediateRelTable = createIntermediateRelTable(
          name,
          optimizedPlan,
          modifyKindSetTrait.modifyKindSet,
          block.isUpdateBeforeRequired)
        val newTableScan = wrapIntermediateRelTableToTableScan(intermediateRelTable, name)
        block.setNewOutputNode(newTableScan)
        block.setOutputTableName(name)
        block.setOptimizedPlan(optimizedPlan)
    }
  }

  /**
   * Generates the optimized [[RelNode]] tree from the original relational node tree.
   *
   * @param relNode
   *   The original [[RelNode]] tree
   * @return
   *   The optimized [[RelNode]] tree
   */
  private def optimizeTree(
      relNode: RelNode,
      updateBeforeRequired: Boolean,
      isSinkBlock: Boolean): RelNode = {
    val tableConfig = planner.getTableConfig
    val programs = TableConfigUtils
      .getCalciteConfig(tableConfig)
      .getBatchProgram
      .getOrElse(FlinkIncrementalProgram.buildProgram(tableConfig))
    Preconditions.checkNotNull(programs)

    val context = unwrapContext(relNode)

    programs.optimize(
      relNode,
      new BatchOptimizeContext {

        override def isBatchMode: Boolean = true

        override def getTableConfig: TableConfig = tableConfig

        override def getFunctionCatalog: FunctionCatalog = planner.functionCatalog

        override def getCatalogManager: CatalogManager = planner.catalogManager

        override def getModuleManager: ModuleManager = planner.moduleManager

        override def getRexFactory: RexFactory = context.getRexFactory

        override def getFlinkRelBuilder: FlinkRelBuilder = planner.createRelBuilder

        override def isUpdateBeforeRequired: Boolean = updateBeforeRequired

        override def needFinalTimeIndicatorConversion: Boolean = isSinkBlock

        override def getClassLoader: ClassLoader = context.getClassLoader
      }
    )
  }

  /**
   * Infer updateKind property for each block. Optimize order: from parent block to child blocks.
   * NOTES: this method should not change the original RelNode tree.
   *
   * @param block
   *   The [[RelNodeBlock]] instance.
   * @param updateBeforeRequired
   *   True if UPDATE_BEFORE message is required for updates
   * @param isSinkBlock
   *   True if the given block is sink block.
   */
  private def propagateUpdateKind(
      block: RelNodeBlock,
      updateBeforeRequired: Boolean,
      isSinkBlock: Boolean): Unit = {
    val blockLogicalPlan = block.getPlan
    // infer updateKind with required trait
    val optimizedPlan =
      optimizeTree(blockLogicalPlan, updateBeforeRequired, isSinkBlock)
    // propagate the inferred updateKind to the child blocks
    propagateTraits(optimizedPlan)

    block.children.foreach {
      child =>
        propagateUpdateKind(
          child,
          updateBeforeRequired = child.isUpdateBeforeRequired,
          isSinkBlock = false)
    }

    def propagateTraits(rel: RelNode): Unit = rel match {
      case _: StreamPhysicalDataStreamScan | _: StreamPhysicalIntermediateTableScan |
          _: StreamPhysicalLegacyTableSourceScan | _: StreamPhysicalTableSourceScan =>
        val scan = rel.asInstanceOf[TableScan]
        val updateKindTrait = scan.getTraitSet.getTrait(UpdateKindTraitDef.INSTANCE)
        val tableName = scan.getTable.getQualifiedName.mkString(".")
        val inputBlocks = block.children.filter(b => tableName.equals(b.getOutputTableName))
        Preconditions.checkArgument(inputBlocks.size <= 1)
        if (inputBlocks.size == 1) {
          val childBlock = inputBlocks.head
          // propagate updateKind trait to child block
          val requireUB = updateKindTrait.updateKind == UpdateKind.BEFORE_AND_AFTER
          childBlock.setUpdateBeforeRequired(requireUB || childBlock.isUpdateBeforeRequired)
        }
      case ser: StreamPhysicalRel => ser.getInputs.foreach(e => propagateTraits(e))
      case _ => // do nothing
    }
  }

  private def resetIntermediateResult(block: RelNodeBlock): Unit = {
    block.setNewOutputNode(null)
    block.setOutputTableName(null)
    block.setOptimizedPlan(null)

    block.children.foreach {
      child =>
        if (child.getNewOutputNode.nonEmpty) {
          resetIntermediateResult(child)
        }
    }
  }

  private def createIntermediateRelTable(
      name: String,
      relNode: RelNode,
      modifyKindSet: ModifyKindSet,
      isUpdateBeforeRequired: Boolean): IntermediateRelTable = {
    val uniqueKeys = getUniqueKeys(relNode)
    val fmq = FlinkRelMetadataQuery
      .reuseOrCreate(planner.createRelBuilder.getCluster.getMetadataQuery)
    val monotonicity = fmq.getRelModifiedMonotonicity(relNode)
    val windowProperties = fmq.getRelWindowProperties(relNode)
    val statistic = FlinkStatistic
      .builder()
      .uniqueKeys(uniqueKeys)
      .relModifiedMonotonicity(monotonicity)
      .relWindowProperties(windowProperties)
      .build()
    new IntermediateRelTable(
      Collections.singletonList(name),
      relNode,
      modifyKindSet,
      isUpdateBeforeRequired,
      fmq.getUpsertKeys(relNode),
      statistic)
  }

  private def getUniqueKeys(relNode: RelNode): util.Set[_ <: util.Set[String]] = {
    val rowType = relNode.getRowType
    val fmq =
      FlinkRelMetadataQuery.reuseOrCreate(planner.createRelBuilder.getCluster.getMetadataQuery)
    val uniqueKeys = fmq.getUniqueKeys(relNode)
    if (uniqueKeys != null) {
      uniqueKeys.filter(_.nonEmpty).map {
        uniqueKey =>
          val keys = new util.HashSet[String]()
          uniqueKey.asList().foreach(idx => keys.add(rowType.getFieldNames.get(idx)))
          keys
      }
    } else {
      null
    }
  }

  override protected def postOptimize(expanded: Seq[RelNode]): Seq[RelNode] = {
    StreamNonDeterministicPhysicalPlanResolver.resolvePhysicalPlan(expanded, planner.getTableConfig)
  }
}

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
package org.apache.flink.table.planner.delegation

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.catalog.{CatalogManager, FunctionCatalog}
import org.apache.flink.table.delegation.Executor
import org.apache.flink.table.module.ModuleManager
import org.apache.flink.table.planner.plan.`trait`.{FlinkRelDistributionTraitDef, MiniBatchIntervalTraitDef, ModifyKindSetTraitDef, UpdateKindTraitDef}
import org.apache.flink.table.planner.plan.optimize.{IncrementalCommonSubGraphBasedOptimizer, Optimizer}

import org.apache.calcite.plan.{ConventionTraitDef, RelTrait, RelTraitDef}
import org.apache.calcite.rel.RelCollationTraitDef

class IncrementalPlanner(
    executor: Executor,
    tableConfig: TableConfig,
    moduleManager: ModuleManager,
    functionCatalog: FunctionCatalog,
    catalogManager: CatalogManager,
    classLoader: ClassLoader)
  extends BatchPlanner(
    executor,
    tableConfig,
    moduleManager,
    functionCatalog,
    catalogManager,
    classLoader) {

  override protected def getTraitDefs: Array[RelTraitDef[_ <: RelTrait]] = {
    Array(
      ConventionTraitDef.INSTANCE,
      FlinkRelDistributionTraitDef.INSTANCE,
      RelCollationTraitDef.INSTANCE, // for sort
      ModifyKindSetTraitDef.INSTANCE,
      UpdateKindTraitDef.INSTANCE
    )
  }

  override protected def getOptimizer: Optimizer = new IncrementalCommonSubGraphBasedOptimizer(this)
}

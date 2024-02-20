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
package org.apache.flink.table.planner.codegen.runtimefilter

import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.codegen.CodeGenUtils.{DEFAULT_INPUT1_TERM, DEFAULT_INPUT2_TERM, className, newName}
import org.apache.flink.table.planner.codegen.OperatorCodeGenerator.{INPUT_SELECTION, generateCollect}
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, OperatorCodeGenerator, ProjectionCodeGenerator}
import org.apache.flink.table.planner.typeutils.RowTypeUtils
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory
import org.apache.flink.table.runtime.operators.runtimefilter.util.{RuntimeFilter, RuntimeFilterUtils}
import org.apache.flink.table.runtime.typeutils.RowDataSerializer
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.util.Preconditions

/** Operator code generator for runtime filter operator. */
object RuntimeFilterCodeGenerator {
  def gen(
      ctx: CodeGeneratorContext,
      buildType: RowType,
      probeType: RowType,
      probeIndices: Array[Int]): CodeGenOperatorFactory[RowData] = {
    val projectedType = RowTypeUtils.projectRowType(probeType, probeIndices)
    val rowDataSerializer = new RowDataSerializer(projectedType)
    val serializer = ctx.addReusableObject(rowDataSerializer, "serializer")

    val probeGenProj = ProjectionCodeGenerator.generateProjection(
      ctx,
      "RuntimeFilterProjection",
      probeType,
      projectedType,
      probeIndices)
    ctx.addReusableInnerClass(probeGenProj.getClassName, probeGenProj.getCode)

    val probeProjection = newName(ctx, "probeToBinaryRow")
    ctx.addReusableMember(s"private transient ${probeGenProj.getClassName} $probeProjection;")
    val probeProjRefs = ctx.addReusableObject(probeGenProj.getReferences, "probeProjRefs")
    ctx.addReusableOpenStatement(
      s"$probeProjection = new ${probeGenProj.getClassName}($probeProjRefs);")

    val buildComplete = newName(ctx, "buildComplete")
    ctx.addReusableMember(s"private transient boolean $buildComplete;")
    ctx.addReusableOpenStatement(s"$buildComplete = false;")

    val filter = newName(ctx, "filter")
    val filterClass = className[RuntimeFilter]
    ctx.addReusableMember(s"private transient $filterClass $filter;")
    val filterUtilsClass = className[RuntimeFilterUtils]

    val processElement1Code =
      s"""
         |${className[Preconditions]}.checkState(!$buildComplete, "Should not build completed.");
         |
         |if ($filter == null && !$DEFAULT_INPUT1_TERM.isNullAt(1)) {
         |    $filter = $filterUtilsClass.convertRowDataToRuntimeFilter($DEFAULT_INPUT1_TERM, $serializer);
         |}
         |""".stripMargin

    val processElement2Code =
      s"""
         |${className[Preconditions]}.checkState($buildComplete, "Should build completed.");
         |
         |if ($filter != null) {
         |    if ($filter.test($probeProjection.apply($DEFAULT_INPUT2_TERM))) {
         |        ${generateCollect(s"$DEFAULT_INPUT2_TERM")}
         |    }
         |} else {
         |    ${generateCollect(s"$DEFAULT_INPUT2_TERM")}
         |}
         |""".stripMargin

    val nextSelectionCode =
      s"return $buildComplete ? $INPUT_SELECTION.SECOND : $INPUT_SELECTION.FIRST;"

    val endInputCode1 =
      s"""
         |${className[Preconditions]}.checkState(!$buildComplete, "Should not build completed.");
         |
         |LOG.info("RuntimeFilter build completed.");
         |$buildComplete = true;
         |""".stripMargin

    val endInputCode2 =
      s"""
         |${className[Preconditions]}.checkState($buildComplete, "Should build completed.");
         |
         |LOG.info("Finish RuntimeFilter probe phase.");
         |""".stripMargin

    new CodeGenOperatorFactory[RowData](
      OperatorCodeGenerator.generateTwoInputStreamOperator(
        ctx,
        "RuntimeFilterOperator",
        processElement1Code,
        processElement2Code,
        buildType,
        probeType,
        DEFAULT_INPUT1_TERM,
        DEFAULT_INPUT2_TERM,
        nextSelectionCode = Some(nextSelectionCode),
        endInputCode1 = Some(endInputCode1),
        endInputCode2 = Some(endInputCode2)
      ))
  }
}

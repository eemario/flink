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

import org.apache.flink.incremental.SourceOffsets;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil;
import org.apache.flink.table.planner.utils.PlanKind;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import scala.Enumeration;

import static scala.runtime.BoxedUnit.UNIT;

/** Test for {@link IncrementalProcessingShuttle}. */
public class IncrementalProcessingShuttleTest extends TableTestBase {
    protected final TableTestUtil util = batchTestUtil(TableConfig.getDefault());
    private final Catalog catalog = new GenericInMemoryCatalog("MockCatalog", "default");

    @BeforeEach
    void before() {
        util.tableEnv().registerCatalog("testCatalog", catalog);
        util.tableEnv().executeSql("use catalog testCatalog");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE t1 (\n"
                                + "  a BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true',\n"
                                + " 'enable-incremental-processing' = 'true'\n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE t2 (\n"
                                + "  a BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true',\n"
                                + " 'enable-incremental-processing' = 'true'\n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE t3 (\n"
                                + "  a BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true',\n"
                                + " 'enable-incremental-processing' = 'true'\n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE t0 (\n"
                                + "  a BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE sink (\n"
                                + "  a BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");
    }

    private String getStringFromRelNode(RelNode relNode) {
        return FlinkRelOptUtil.toString(
                relNode, SqlExplainLevel.EXPPLAN_ATTRIBUTES, false, false, true, false, true);
    }

    @Test
    void testIncrementalProcessingShuttleWithFilter() {
        // Unsupported for now.
        String sql = "INSERT INTO sink SELECT * FROM t1 where a > 0;";

        List<Operation> operationList = util.getPlanner().getParser().parse(sql);
        assert operationList.size() == 1 && operationList.get(0) instanceof ModifyOperation;

        RelNode origin =
                TableTestUtil.toRelNode(util.tableEnv(), (ModifyOperation) operationList.get(0));
        RelNode incremental =
                origin.accept(
                        new IncrementalProcessingShuttle(
                                util.tableConfig(),
                                null,
                                new SourceOffsets(),
                                new SourceOffsets()));
        assert incremental == null;
    }

    @Test
    void testIncrementalProcessingShuttleWithUnion() {
        // Unsupported for now.
        String sql = "INSERT INTO sink SELECT * FROM t1 UNION ALL SELECT * FROM t1;";

        List<Operation> operationList = util.getPlanner().getParser().parse(sql);
        assert operationList.size() == 1 && operationList.get(0) instanceof ModifyOperation;

        RelNode origin =
                TableTestUtil.toRelNode(util.tableEnv(), (ModifyOperation) operationList.get(0));
        RelNode incremental =
                origin.accept(
                        new IncrementalProcessingShuttle(
                                util.tableConfig(),
                                null,
                                new SourceOffsets(),
                                new SourceOffsets()));
        assert incremental == null;
    }

    @Test
    void testIncrementalProcessingShuttleWithUnsupportedSource() {
        String sql = "INSERT INTO sink SELECT * FROM t0;";

        List<Operation> operationList = util.getPlanner().getParser().parse(sql);
        assert operationList.size() == 1 && operationList.get(0) instanceof ModifyOperation;

        RelNode origin =
                TableTestUtil.toRelNode(util.tableEnv(), (ModifyOperation) operationList.get(0));
        RelNode incremental =
                origin.accept(
                        new IncrementalProcessingShuttle(
                                util.tableConfig(),
                                null,
                                new SourceOffsets(),
                                new SourceOffsets()));
        assert incremental == null;
    }

    @Test
    void testIncrementalProcessingShuttleWithProject() {
        String sql = "INSERT INTO sink SELECT * FROM t1;";

        List<Operation> operationList = util.getPlanner().getParser().parse(sql);
        assert operationList.size() == 1 && operationList.get(0) instanceof ModifyOperation;

        RelNode origin =
                TableTestUtil.toRelNode(util.tableEnv(), (ModifyOperation) operationList.get(0));
        util.assertEqualsOrExpand("origin", getStringFromRelNode(origin), true);

        SourceOffsets endOffsets = new SourceOffsets();
        RelNode incremental =
                origin.accept(
                        new IncrementalProcessingShuttle(
                                util.tableConfig(), null, new SourceOffsets(), endOffsets));
        assert incremental != null;
        assert endOffsets.getOffsets().size() == 1;
        util.assertPlanEquals(
                new RelNode[] {incremental},
                new ExplainDetail[] {},
                true,
                new Enumeration.Value[] {PlanKind.AST(), PlanKind.OPT_REL(), PlanKind.OPT_EXEC()},
                () -> UNIT,
                true);
    }

    @Test
    void testIncrementalProcessingShuttleWithJoin() {
        String sql = "INSERT INTO sink SELECT t1.a FROM t1 JOIN t2 on t1.a = t2.a;";

        List<Operation> operationList = util.getPlanner().getParser().parse(sql);
        assert operationList.size() == 1 && operationList.get(0) instanceof ModifyOperation;

        RelNode origin =
                TableTestUtil.toRelNode(util.tableEnv(), (ModifyOperation) operationList.get(0));
        util.assertEqualsOrExpand("origin", getStringFromRelNode(origin), true);

        SourceOffsets endOffsets = new SourceOffsets();
        RelNode incremental =
                origin.accept(
                        new IncrementalProcessingShuttle(
                                util.tableConfig(), null, new SourceOffsets(), endOffsets));
        assert incremental != null;
        assert endOffsets.getOffsets().size() == 2;
        util.assertPlanEquals(
                new RelNode[] {incremental},
                new ExplainDetail[] {},
                true,
                new Enumeration.Value[] {PlanKind.AST(), PlanKind.OPT_REL(), PlanKind.OPT_EXEC()},
                () -> UNIT,
                true);
    }

    @Test
    void testIncrementalProcessingShuttleWithMultipleJoins() {
        String sql =
                "INSERT INTO sink SELECT t1.a FROM t1 JOIN t2 on t1.a = t2.a JOIN t3 on t1.a = t3.a;";

        List<Operation> operationList = util.getPlanner().getParser().parse(sql);
        assert operationList.size() == 1 && operationList.get(0) instanceof ModifyOperation;

        RelNode origin =
                TableTestUtil.toRelNode(util.tableEnv(), (ModifyOperation) operationList.get(0));
        util.assertEqualsOrExpand("origin", getStringFromRelNode(origin), true);

        SourceOffsets endOffsets = new SourceOffsets();
        RelNode incremental =
                origin.accept(
                        new IncrementalProcessingShuttle(
                                util.tableConfig(), null, new SourceOffsets(), endOffsets));
        assert incremental != null;
        assert endOffsets.getOffsets().size() == 3;
        util.assertPlanEquals(
                new RelNode[] {incremental},
                new ExplainDetail[] {},
                true,
                new Enumeration.Value[] {PlanKind.AST(), PlanKind.OPT_REL(), PlanKind.OPT_EXEC()},
                () -> UNIT,
                true);
    }
}

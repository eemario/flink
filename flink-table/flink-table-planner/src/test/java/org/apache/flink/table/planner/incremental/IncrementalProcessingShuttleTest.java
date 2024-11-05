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
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
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

import javax.annotation.Nullable;

import java.util.List;

import scala.Enumeration;

import static org.apache.flink.table.utils.EncodingUtils.escapeIdentifier;
import static org.assertj.core.api.Assertions.assertThat;
import static scala.runtime.BoxedUnit.UNIT;

/** Test for {@link IncrementalProcessingShuttle}. */
public class IncrementalProcessingShuttleTest extends TableTestBase {
    private static final String TEST_CATALOG_NAME = "testCatalog";
    private static final String TEST_DATABASE_NAME = "default";
    private final TableTestUtil util = batchTestUtil(TableConfig.getDefault());
    private final Catalog catalog = new MockCatalog("MockCatalog", TEST_DATABASE_NAME);

    @BeforeEach
    public void before() {
        util.tableEnv().registerCatalog(TEST_CATALOG_NAME, catalog);
        util.tableEnv().executeSql("USE CATALOG " + TEST_CATALOG_NAME);
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE t1 (\n"
                                + "  a BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true',\n"
                                + " 'enable-scan-range' = 'true'\n"
                                + ")");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE t2 (\n"
                                + "  a BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true',\n"
                                + " 'enable-scan-range' = 'true'\n"
                                + ")");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE sink (\n"
                                + "  a BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true',\n"
                                + " 'enable-overwrite' = 'true'\n"
                                + ")");
    }

    @Test
    public void testIncrementalProcessingShuttleWithUnsupportedFilter() {
        // Unsupported for now.
        String sql = "INSERT OVERWRITE sink SELECT * FROM t1 WHERE a IN (SELECT a from t2);";
        assertUnsupported(sql);
    }

    @Test
    public void testIncrementalProcessingShuttleWithInsertInto() {
        // Should not support INSERT INTO
        String sql = "INSERT INTO sink SELECT * FROM t1;";
        assertUnsupported(sql);
    }

    @Test
    public void testIncrementalProcessingShuttleWithUnion() {
        // Unsupported for now.
        String sql = "INSERT OVERWRITE sink SELECT * FROM t1 UNION ALL SELECT * FROM t1;";
        assertUnsupported(sql);
    }

    @Test
    public void testIncrementalProcessingShuttleWithUnsupportedSource() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE t0 (\n"
                                + "  a BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");
        String sql = "INSERT OVERWRITE sink SELECT * FROM t0;";
        assertUnsupported(sql);
    }

    @Test
    public void testIncrementalProcessingShuttleWithFilter() {
        String sql = "INSERT OVERWRITE sink SELECT * FROM t1 WHERE a = 0;";
        assertSupportedPlans(sql, 1, null);
    }

    @Test
    public void testIncrementalProcessingShuttleWithProject() {
        String sql = "INSERT OVERWRITE sink SELECT a FROM t1;";
        assertSupportedPlans(sql, 1, null);
    }

    @Test
    public void testIncrementalProcessingShuttleWithJoin() {
        // mock restored source offsets
        SourceOffsets restoredOffsets = new SourceOffsets();
        restoredOffsets.setOffset(getFullyQualifiedName("t1"), System.currentTimeMillis());
        restoredOffsets.setOffset(getFullyQualifiedName("t2"), System.currentTimeMillis());

        String sql = "INSERT OVERWRITE sink SELECT t1.a FROM t1 JOIN t2 on t1.a = t2.a;";
        assertSupportedPlans(sql, 2, restoredOffsets);
    }

    @Test
    public void testIncrementalProcessingShuttleWithJoinFallbackToFullPlan() {
        String sql = "INSERT OVERWRITE sink SELECT t1.a FROM t1 JOIN t2 on t1.a = t2.a;";
        assertSupportedPlans(sql, 2, null);
    }

    @Test
    public void testIncrementalProcessingShuttleWithMultipleJoins() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE t3 (\n"
                                + "  a BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true',\n"
                                + " 'enable-scan-range' = 'true'\n"
                                + ")");
        // mock restored source offsets
        SourceOffsets restoredOffsets = new SourceOffsets();
        restoredOffsets.setOffset(getFullyQualifiedName("t1"), System.currentTimeMillis());
        restoredOffsets.setOffset(getFullyQualifiedName("t2"), System.currentTimeMillis());
        restoredOffsets.setOffset(getFullyQualifiedName("t3"), System.currentTimeMillis());

        String sql =
                "INSERT OVERWRITE sink SELECT t1.a FROM t1 JOIN t2 on t1.a = t2.a JOIN t3 on t1.a = t3.a;";
        assertSupportedPlans(sql, 3, restoredOffsets);
    }

    @Test
    public void testIncrementalProcessingShuttleWithMultipleJoinsFallbackToFullPlan() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE t3 (\n"
                                + "  a BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true',\n"
                                + " 'enable-scan-range' = 'true'\n"
                                + ")");
        String sql =
                "INSERT OVERWRITE sink SELECT t1.a FROM t1 JOIN t2 on t1.a = t2.a JOIN t3 on t1.a = t3.a;";
        assertSupportedPlans(sql, 3, null);
    }

    @Test
    public void testIncrementalProcessingShuttleWithAggregate() {
        // Unsupported for now.
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE aggSink (\n"
                                + "  a BIGINT,\n"
                                + "  b BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true',\n"
                                + " 'enable-overwrite' = 'true'\n"
                                + ")");
        String sql = "INSERT OVERWRITE aggSink SELECT a, AVG(a) FROM t1 GROUP BY a;";
        assertUnsupported(sql);
    }

    @Test
    public void testIncrementalProcessingShuttleWithOuterJoin() {
        // Unsupported for now.
        String sql = "INSERT OVERWRITE sink SELECT t1.a from t1 FULL OUTER JOIN t2 ON t1.a = t2.a;";
        assertUnsupported(sql);
    }

    private String getStringFromRelNode(RelNode relNode) {
        return FlinkRelOptUtil.toString(
                relNode, SqlExplainLevel.EXPPLAN_ATTRIBUTES, false, false, true, false, true);
    }

    private void assertUnsupported(String sql) {
        List<Operation> operationList = util.getPlanner().getParser().parse(sql);
        assertThat(operationList.size()).isEqualTo(1);
        assertThat(operationList.get(0)).isInstanceOf(ModifyOperation.class);
        RelNode origin =
                TableTestUtil.toRelNode(util.tableEnv(), (ModifyOperation) operationList.get(0));
        RelNode incremental =
                origin.accept(new IncrementalProcessingShuttle(util.tableConfig(), null));
        assertThat(incremental).isNull();
    }

    private void assertSupportedPlans(
            String sql, int sourceNum, @Nullable SourceOffsets restoredOffsets) {
        List<Operation> operationList = util.getPlanner().getParser().parse(sql);
        assertThat(operationList.size()).isEqualTo(1);
        assertThat(operationList.get(0)).isInstanceOf(ModifyOperation.class);
        RelNode origin =
                TableTestUtil.toRelNode(util.tableEnv(), (ModifyOperation) operationList.get(0));
        util.assertEqualsOrExpand("origin", getStringFromRelNode(origin), true);
        IncrementalProcessingShuttle shuttle =
                new IncrementalProcessingShuttle(util.tableConfig(), restoredOffsets);
        RelNode incremental = origin.accept(shuttle);
        assertThat(incremental).isNotNull();
        assertThat(shuttle.getCachedEndOffsets().getOffsets().size()).isEqualTo(sourceNum);
        util.assertPlanEquals(
                new RelNode[] {incremental},
                new ExplainDetail[] {},
                true,
                new Enumeration.Value[] {PlanKind.AST(), PlanKind.OPT_REL(), PlanKind.OPT_EXEC()},
                () -> UNIT,
                true);
    }

    /** Get fully qualified table name as {@link ObjectIdentifier#toString()}. */
    private String getFullyQualifiedName(String tableName) {
        return String.format(
                "%s.%s.%s",
                escapeIdentifier(TEST_CATALOG_NAME),
                escapeIdentifier(TEST_DATABASE_NAME),
                escapeIdentifier(tableName));
    }

    /** Catalog which supports {@link Catalog#getTableTimestamp(ObjectPath, long)}. */
    private static class MockCatalog extends GenericInMemoryCatalog {
        public MockCatalog(String name) {
            super(name);
        }

        public MockCatalog(String name, String defaultDatabase) {
            super(name, defaultDatabase);
        }

        @Override
        public long getTableTimestamp(ObjectPath tablePath, long timestamp)
                throws TableNotExistException, CatalogException {
            return timestamp;
        }
    }
}

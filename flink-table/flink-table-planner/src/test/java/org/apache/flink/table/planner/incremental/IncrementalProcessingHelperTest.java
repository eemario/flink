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
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link IncrementalProcessingHelper}. */
public class IncrementalProcessingHelperTest extends TableTestBase {
    private final TableTestUtil util = batchTestUtil(TableConfig.getDefault());
    private final Catalog catalog = new MockCatalog("MockCatalog", "default");
    @TempDir private java.nio.file.Path temporaryFolder;

    @BeforeEach
    public void before() {
        util.tableEnv().registerCatalog("testCatalog", catalog);
        util.tableEnv().executeSql("USE CATALOG testCatalog");

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
                        "CREATE TABLE sink (\n"
                                + "  a BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true',\n"
                                + " 'enable-overwrite' = 'true'\n"
                                + ")");
    }

    @Test
    public void testStreamingMode() throws IOException {
        TableConfig tableConfig =
                createTableConfig(true, TempDirUtils.newFolder(temporaryFolder).getAbsolutePath());
        String sqlSupported = "INSERT OVERWRITE sink SELECT t1.a FROM t1;";
        IncrementalProcessingPlan plan =
                createIncrementalProcessingPlan(Arrays.asList(sqlSupported), tableConfig, true);

        assertThat(plan).isNull();
    }

    @Test
    public void testIncrementalModeDisabled() throws IOException {
        TableConfig tableConfig =
                createTableConfig(false, TempDirUtils.newFolder(temporaryFolder).getAbsolutePath());
        String sqlSupported = "INSERT OVERWRITE sink SELECT t1.a FROM t1;";
        IncrementalProcessingPlan plan =
                createIncrementalProcessingPlan(Arrays.asList(sqlSupported), tableConfig, false);

        assertThat(plan).isNull();
    }

    @Test
    public void testIncrementalCheckpointsDirNull() throws IOException {
        TableConfig tableConfig = createTableConfig(true, null);
        String sqlSupported = "INSERT OVERWRITE sink SELECT t1.a FROM t1;";
        IncrementalProcessingPlan plan =
                createIncrementalProcessingPlan(Arrays.asList(sqlSupported), tableConfig, false);

        assertThat(plan).isNull();
    }

    @Test
    public void testIncrementalCheckpointsDirInvalid() {
        TableConfig tableConfig = createTableConfig(true, "invalid:path");
        String sqlSupported = "INSERT OVERWRITE sink SELECT t1.a FROM t1;";

        assertThatThrownBy(
                        () ->
                                createIncrementalProcessingPlan(
                                        Arrays.asList(sqlSupported), tableConfig, false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testIncrementalSqlSupported() throws IOException {
        TableConfig tableConfig =
                createTableConfig(true, TempDirUtils.newFolder(temporaryFolder).getAbsolutePath());
        String sqlSupported = "INSERT OVERWRITE sink SELECT t1.a FROM t1;";
        IncrementalProcessingPlan plan =
                createIncrementalProcessingPlan(Arrays.asList(sqlSupported), tableConfig, false);

        assertThat(plan).isNotNull();
        assertThat(plan.getEndOffsets().getOffsets().size()).isEqualTo(1);
        util.assertEqualsOrExpand("incremental", getStringFromRelNodes(plan.getRelNodes()), true);
    }

    @Test
    public void testIncrementalSqlUnSupported() throws IOException {
        TableConfig tableConfig =
                createTableConfig(true, TempDirUtils.newFolder(temporaryFolder).getAbsolutePath());
        String sqlUnSupported = "INSERT INTO sink SELECT t1.a FROM t1;";
        IncrementalProcessingPlan plan =
                createIncrementalProcessingPlan(Arrays.asList(sqlUnSupported), tableConfig, false);

        assertThat(plan).isNull();
    }

    @Test
    public void testIncrementalSqlContainsSupported() throws Exception {
        TableConfig tableConfig =
                createTableConfig(true, TempDirUtils.newFolder(temporaryFolder).getAbsolutePath());
        String sqlSupported = "INSERT OVERWRITE sink SELECT t1.a FROM t1;";
        String sqlUnSupported = "INSERT INTO sink SELECT t1.a FROM t1;";
        IncrementalProcessingPlan plan =
                createIncrementalProcessingPlan(
                        Arrays.asList(sqlSupported, sqlUnSupported), tableConfig, false);

        assertThat(plan).isNotNull();
        assertThat(plan.getEndOffsets().getOffsets().size()).isEqualTo(1);
        util.assertEqualsOrExpand("incremental", getStringFromRelNodes(plan.getRelNodes()), true);
    }

    private TableConfig createTableConfig(
            boolean enableIncrementalProcessing, @Nullable String checkpointsDir) {
        TableConfig tableConfig = util.tableConfig();
        if (enableIncrementalProcessing) {
            tableConfig.set(
                    BatchIncrementalExecutionOptions.INCREMENTAL_EXECUTION_MODE,
                    BatchIncrementalExecutionOptions.IncrementalExecutionMode.AUTO);
        }
        if (checkpointsDir != null) {
            tableConfig.set(
                    BatchIncrementalExecutionOptions.INCREMENTAL_CHECKPOINTS_DIRECTORY,
                    checkpointsDir);
        }
        return tableConfig;
    }

    private IncrementalProcessingPlan createIncrementalProcessingPlan(
            List<String> sqlStatements, TableConfig tableConfig, boolean isStreamingMode)
            throws IOException {
        List<RelNode> relNodes = new ArrayList<>();
        for (String sql : sqlStatements) {
            List<Operation> operationList = util.getPlanner().getParser().parse(sql);
            RelNode relNode =
                    TableTestUtil.toRelNode(
                            util.tableEnv(), (ModifyOperation) operationList.get(0));
            relNodes.add(relNode);
        }
        return IncrementalProcessingHelper.tryGenerateIncrementalProcessingPlan(
                relNodes, tableConfig, isStreamingMode);
    }

    private String getStringFromRelNodes(List<RelNode> relNodes) {
        return String.join(
                System.lineSeparator(),
                relNodes.stream()
                        .map(
                                relNode ->
                                        FlinkRelOptUtil.toString(
                                                relNode,
                                                SqlExplainLevel.EXPPLAN_ATTRIBUTES,
                                                false,
                                                false,
                                                true,
                                                false,
                                                true))
                        .collect(Collectors.toList()));
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

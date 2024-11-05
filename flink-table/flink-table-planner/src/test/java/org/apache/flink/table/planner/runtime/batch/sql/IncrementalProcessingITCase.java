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

package org.apache.flink.table.planner.runtime.batch.sql;

import org.apache.flink.configuration.BatchIncrementalExecutionOptions;
import org.apache.flink.legacy.table.connector.source.SourceFunctionProvider;
import org.apache.flink.runtime.incremental.FileSystemIncrementalBatchCheckpointStore;
import org.apache.flink.runtime.incremental.IncrementalBatchCheckpointStore;
import org.apache.flink.streaming.api.functions.source.legacy.FromElementsFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsScanRange;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.factories.TableFactoryHarness;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** IT test for incremental processing. */
public class IncrementalProcessingITCase extends BatchTestBase {
    private String basePath;

    private TableEnvironment tEnv;

    private Catalog catalog;

    private IncrementalSource source1;

    private IncrementalSource source2;

    @BeforeEach
    @Override
    public void before() throws Exception {
        super.before();
        basePath = createTempFolder().getPath();
        tEnv = tEnv();
        catalog = new MockCatalog("default", "default");
        tEnv.registerCatalog("testCatalog", catalog);
        tEnv.executeSql("USE CATALOG testCatalog");

        tEnv.getConfig()
                .getConfiguration()
                .set(
                        BatchIncrementalExecutionOptions.INCREMENTAL_EXECUTION_MODE,
                        BatchIncrementalExecutionOptions.IncrementalExecutionMode.AUTO);
        tEnv.getConfig()
                .getConfiguration()
                .set(BatchIncrementalExecutionOptions.INCREMENTAL_CHECKPOINTS_DIRECTORY, basePath);

        Schema schema1 =
                Schema.newBuilder()
                        .column("x", DataTypes.INT())
                        .column("y", DataTypes.STRING())
                        .column("z", DataTypes.INT())
                        .build();
        source1 = new IncrementalSource();
        TableDescriptor sourceDescriptor1 =
                TableFactoryHarness.newBuilder().schema(schema1).source(source1).build();
        tEnv.createTable("t1", sourceDescriptor1);

        Schema schema2 =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.DOUBLE())
                        .build();
        source2 = new IncrementalSource();
        TableDescriptor sourceDescriptor2 =
                TableFactoryHarness.newBuilder().schema(schema2).source(source2).build();
        tEnv.createTable("t2", sourceDescriptor2);

        tEnv.executeSql(
                String.format(
                        "CREATE TEMPORARY TABLE sink (\n"
                                + "  a INT,\n"
                                + "  b DOUBLE,\n"
                                + "  y STRING,\n"
                                + "  z INT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true',\n"
                                + " 'enable-overwrite' = 'true'\n"
                                + ")",
                        basePath));
    }

    @Test
    public void testIncrementalProcessing() throws Exception {
        String insertSql =
                "INSERT OVERWRITE sink SELECT * FROM (SELECT t2.a, t2.b, t1.y, t1.z FROM t2 JOIN t1 ON t1.x = t2.a) WHERE z = 2;";

        List<RowData> rowData1 =
                Arrays.asList(
                        GenericRowData.of(1, StringData.fromString("1"), 1),
                        GenericRowData.of(1, StringData.fromString("1"), 2),
                        GenericRowData.of(2, StringData.fromString("2"), 2));
        source1.insertRowData(rowData1);

        List<RowData> rowData2 =
                Arrays.asList(
                        GenericRowData.of(1, 0.1),
                        GenericRowData.of(2, 0.2),
                        GenericRowData.of(3, 0.3));
        source2.insertRowData(rowData2);

        tEnv.executeSql(insertSql).await();

        assertThat(TestValuesTableFactory.getResults("sink"))
                .isEqualTo(Arrays.asList(Row.of(1, 0.1, "1", 2), Row.of(2, 0.2, "2", 2)));

        IncrementalBatchCheckpointStore store =
                new FileSystemIncrementalBatchCheckpointStore(basePath);
        assertThat(store.getLatestSourceOffsets().getOffsets().size()).isEqualTo(2);
        assertThat(store.getHistory().getRetainedHistoryRecords().size()).isEqualTo(1);

        TestValuesTableFactory.clearAllData();
        List<RowData> rowData3 =
                Arrays.asList(
                        GenericRowData.of(3, StringData.fromString("3"), 1),
                        GenericRowData.of(3, StringData.fromString("3"), 2));
        source1.insertRowData(rowData3);
        assertThat(source1.getInsertedRowData().size()).isEqualTo(2);

        tEnv.executeSql(insertSql).await();

        assertThat(TestValuesTableFactory.getResults("sink"))
                .isEqualTo(Arrays.asList(Row.of(3, 0.3, "3", 2)));

        assertThat(store.getLatestSourceOffsets().getOffsets().size()).isEqualTo(2);
        assertThat(store.getHistory().getRetainedHistoryRecords().size()).isEqualTo(2);
    }

    // ---------------------------------------------------------------------------------------------

    /** Source which supports incremental processing. */
    private static class IncrementalSource extends TableFactoryHarness.ScanSourceBase
            implements SupportsScanRange {

        private List<Long> timestamps;
        private List<Collection<RowData>> insertedRowData;

        @Nullable private final Integer startIndex;
        @Nullable private final Integer endIndex;

        public IncrementalSource() {
            this(new ArrayList<>(), new ArrayList<>(), null, null);
        }

        public IncrementalSource(
                List<Long> timestamps,
                List<Collection<RowData>> insertedRowData,
                @Nullable Integer startIndex,
                @Nullable Integer endIndex) {
            this.timestamps = timestamps;
            this.insertedRowData = insertedRowData;
            this.startIndex = startIndex;
            this.endIndex = endIndex;
        }

        public void insertRowData(Collection<RowData> data) {
            timestamps.add(System.currentTimeMillis());
            insertedRowData.add(data);
        }

        public List<Collection<RowData>> getInsertedRowData() {
            return insertedRowData;
        }

        @Override
        public DynamicTableSource copy() {
            return new IncrementalSource(timestamps, insertedRowData, startIndex, endIndex);
        }

        @Override
        public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
            Collection<RowData> rows = new ArrayList<>();
            int start = startIndex != null ? startIndex : -1;
            int end = endIndex != null ? endIndex : timestamps.size() - 1;
            for (int i = start + 1; i <= end; i++) {
                rows.addAll(insertedRowData.get(i));
            }
            return SourceFunctionProvider.of(new FromElementsFunction<>(rows), true);
        }

        @Override
        public ScanTableSource applyScanRange(long start, long end) {
            int startIndex = offsetToIndex(start);
            int endIndex = offsetToIndex(end);
            if (startIndex > endIndex || startIndex < -1 || endIndex >= timestamps.size()) {
                return null;
            }
            return withScanRange(startIndex, endIndex);
        }

        private int offsetToIndex(long offset) {
            if (offset == -1) {
                return -1;
            }
            int index = Collections.binarySearch(timestamps, offset);
            if (index < 0) {
                index = -(index + 1) - 1;
            }
            return index;
        }

        private IncrementalSource withScanRange(int startIndex, int endIndex) {
            return new IncrementalSource(timestamps, insertedRowData, startIndex, endIndex);
        }
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

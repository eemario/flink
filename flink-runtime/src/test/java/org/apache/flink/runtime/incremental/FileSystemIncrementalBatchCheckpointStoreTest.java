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

package org.apache.flink.runtime.incremental;

import org.apache.flink.api.common.JobID;
import org.apache.flink.incremental.SourceOffsets;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FileSystemIncrementalBatchCheckpointStore}. */
public class FileSystemIncrementalBatchCheckpointStoreTest {
    @TempDir private java.nio.file.Path temporaryFolder;

    @Test
    void testGenerateWorkingDir() throws IOException {
        String path = TempDirUtils.newFolder(temporaryFolder).getAbsolutePath();
        FileSystemIncrementalBatchCheckpointStore store =
                new FileSystemIncrementalBatchCheckpointStore(path);
        assertThat(store.getWorkingDir().toString()).isEqualTo(path + "/incremental-processing");
    }

    @Test
    void testAddCheckpoint() throws Exception {
        String path = TempDirUtils.newFolder(temporaryFolder).getAbsolutePath();
        FileSystemIncrementalBatchCheckpointStore store =
                new FileSystemIncrementalBatchCheckpointStore(path);

        SourceOffsets sourceOffsets = new SourceOffsets();
        sourceOffsets.setOffset("test", System.currentTimeMillis());
        History history = new History(1);
        HistoryRecord historyRecord = new HistoryRecord((new JobID()).toString(), true);
        history.addHistoryRecord(historyRecord);
        store.addCheckpointAndSubsumeOldestOne(
                new IncrementalBatchCheckpoint(sourceOffsets, history));

        assertThat(store.getLatestSourceOffsets()).isEqualTo(sourceOffsets);
        assertThat(store.getHistory().getRetainedHistoryRecords().size()).isEqualTo(1);
        assertThat(store.getHistory().getRetainedHistoryRecords().get(0)).isEqualTo(historyRecord);
    }

    @Test
    void testSubsumeOldCheckpoint() throws Exception {
        String path = TempDirUtils.newFolder(temporaryFolder).getAbsolutePath();
        FileSystemIncrementalBatchCheckpointStore store =
                new FileSystemIncrementalBatchCheckpointStore(path);

        SourceOffsets sourceOffsets1 = new SourceOffsets();
        sourceOffsets1.setOffset("test", System.currentTimeMillis());
        History history = new History(2);
        HistoryRecord historyRecord1 = new HistoryRecord((new JobID()).toString(), false);
        history.addHistoryRecord(historyRecord1);
        store.addCheckpointAndSubsumeOldestOne(
                new IncrementalBatchCheckpoint(sourceOffsets1, history));

        SourceOffsets sourceOffsets2 = new SourceOffsets();
        sourceOffsets2.setOffset("test", System.currentTimeMillis());
        HistoryRecord historyRecord2 = new HistoryRecord((new JobID()).toString(), true);
        history.addHistoryRecord(historyRecord2);
        store.addCheckpointAndSubsumeOldestOne(
                new IncrementalBatchCheckpoint(sourceOffsets2, history));

        assertThat(store.getLatestSourceOffsets()).isEqualTo(sourceOffsets2);
        assertThat(store.getHistory().getRetainedHistoryRecords().size()).isEqualTo(2);
        assertThat(store.getHistory().getRetainedHistoryRecords().get(0)).isEqualTo(historyRecord1);
        assertThat(store.getHistory().getRetainedHistoryRecords().get(1)).isEqualTo(historyRecord2);
    }

    @Test
    void testHistorySizeLimit() {
        History history = new History(1);
        HistoryRecord historyRecord1 = new HistoryRecord((new JobID()).toString(), false);
        HistoryRecord historyRecord2 = new HistoryRecord((new JobID()).toString(), true);
        history.setRetainedHistoryRecords(
                new LinkedList<>(Arrays.asList(historyRecord1, historyRecord2)));

        assertThat(history.getRetainedHistoryRecords().size()).isEqualTo(1);
        assertThat(history.getRetainedHistoryRecords().get(0)).isEqualTo(historyRecord2);

        HistoryRecord historyRecord3 = new HistoryRecord((new JobID()).toString(), true);
        history.addHistoryRecord(historyRecord3);

        assertThat(history.getRetainedHistoryRecords().size()).isEqualTo(1);
        assertThat(history.getRetainedHistoryRecords().get(0)).isEqualTo(historyRecord3);
    }
}

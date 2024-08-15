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
import org.apache.flink.incremental.Offset;
import org.apache.flink.incremental.SourceOffsets;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FileSystemSourceOffsetsStore}. */
public class FileSystemSourceOffsetsStoreTest {
    @TempDir private java.nio.file.Path temporaryFolder;

    @Test
    void testGenerateWorkingDir() throws IOException {
        String path = "/tmp/flink";
        FileSystemSourceOffsetsStore store = new FileSystemSourceOffsetsStore(path);
        assertThat(store.getWorkingDir().getPath()).isEqualTo("/tmp/flink/incremental-processing");
    }

    @Test
    void testReadAndWrite() throws Exception {
        String path = TempDirUtils.newFolder(temporaryFolder).getAbsolutePath();
        FileSystemSourceOffsetsStore store = new FileSystemSourceOffsetsStore(path);

        SourceOffsets sourceOffsets = new SourceOffsets();
        sourceOffsets.setOffset("test", new Offset(new byte[] {1, 2, 3}));
        History history = new History(1);
        HistoryRecord historyRecord = new HistoryRecord(new JobID(), true);
        history.addHistoryRecord(historyRecord);
        store.writeSourceOffsets(sourceOffsets, historyRecord, 1);

        assertThat(store.readSourceOffsets()).isEqualTo(sourceOffsets);
        assertThat(store.readHistory()).isEqualTo(history);
    }

    @Test
    void testReadAndWriteOverNumRetainedHistory() throws Exception {
        String path = TempDirUtils.newFolder(temporaryFolder).getAbsolutePath();
        FileSystemSourceOffsetsStore store = new FileSystemSourceOffsetsStore(path);

        SourceOffsets sourceOffsets = new SourceOffsets();
        sourceOffsets.setOffset("test", new Offset());
        History history = new History(1);
        HistoryRecord historyRecord1 = new HistoryRecord(new JobID(), true);
        HistoryRecord historyRecord2 = new HistoryRecord(new JobID(), true);
        history.addHistoryRecord(historyRecord2);
        store.writeSourceOffsets(sourceOffsets, historyRecord1, 1);
        store.writeSourceOffsets(sourceOffsets, historyRecord2, 1);

        assertThat(store.readSourceOffsets()).isEqualTo(sourceOffsets);
        assertThat(store.readHistory()).isEqualTo(history);
    }
}

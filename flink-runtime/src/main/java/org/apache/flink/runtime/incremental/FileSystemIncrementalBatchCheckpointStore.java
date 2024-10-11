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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.incremental.SourceOffsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Implementation of {@link IncrementalBatchCheckpointStore} that stores the {@link
 * IncrementalBatchCheckpoint} in a {@link FileSystem}.
 *
 * <p>Write and read operations are performed synchronously, with the calling thread directly
 * interacting with the file system.
 */
public class FileSystemIncrementalBatchCheckpointStore implements IncrementalBatchCheckpointStore {
    private static final Logger LOG =
            LoggerFactory.getLogger(FileSystemIncrementalBatchCheckpointStore.class);

    private final Path workingDir;
    private final FileSystem fileSystem;
    private final SimpleVersionedSerializer<SourceOffsets> sourceOffsetsSerializer;
    private final SimpleVersionedSerializer<History> historySerializer;

    public FileSystemIncrementalBatchCheckpointStore(String path) throws IOException {
        this.workingDir = new Path(path + IncrementalProcessingCheckpointUtils.SUB_DIR);
        this.fileSystem = this.workingDir.getFileSystem();
        if (!fileSystem.exists(workingDir)) {
            fileSystem.mkdirs(workingDir);
            LOG.info("Created incremental processing checkpoints dir {}.", workingDir);
        }
        this.sourceOffsetsSerializer = SourceOffsetsSerializer.INSTANCE;
        this.historySerializer = HistorySerializer.INSTANCE;
    }

    @VisibleForTesting
    Path getWorkingDir() {
        return workingDir;
    }

    @Override
    public void addCheckpointAndSubsumeOldestOne(
            IncrementalBatchCheckpoint incrementalBatchCheckpoint) throws IOException {
        // Writes source offsets
        Path file =
                new Path(workingDir, IncrementalProcessingCheckpointUtils.SOURCE_OFFSETS_FILE_NAME);
        try (DataOutputStream outputStream =
                new DataOutputStream(fileSystem.create(file, FileSystem.WriteMode.OVERWRITE))) {
            byte[] binarySourceOffsets =
                    sourceOffsetsSerializer.serialize(
                            incrementalBatchCheckpoint.getSourceOffsets());
            outputStream.writeInt(binarySourceOffsets.length);
            outputStream.write(binarySourceOffsets);
            outputStream.flush();
        }

        // Writes history
        Path historyFile =
                new Path(workingDir, IncrementalProcessingCheckpointUtils.HISTORY_FILE_NAME);
        try (DataOutputStream historyOutputStream =
                new DataOutputStream(
                        fileSystem.create(historyFile, FileSystem.WriteMode.OVERWRITE))) {
            byte[] binaryHistory =
                    historySerializer.serialize(incrementalBatchCheckpoint.getHistory());
            historyOutputStream.writeInt(binaryHistory.length);
            historyOutputStream.write(binaryHistory);
            historyOutputStream.flush();
        }
    }

    @Override
    public SourceOffsets getLatestSourceOffsets() throws IOException {
        Path readFile =
                new Path(workingDir, IncrementalProcessingCheckpointUtils.SOURCE_OFFSETS_FILE_NAME);
        if (!fileSystem.exists(readFile)) {
            return null;
        }

        try (DataInputStream inputStream = new DataInputStream(fileSystem.open(readFile))) {
            int binarySize = inputStream.readInt();
            byte[] binarySourceOffsets = new byte[binarySize];
            inputStream.readFully(binarySourceOffsets);
            return sourceOffsetsSerializer.deserialize(
                    SourceOffsetsSerializer.INSTANCE.getVersion(), binarySourceOffsets);
        }
    }

    @Override
    public History getHistory() throws IOException {
        Path readFile =
                new Path(workingDir, IncrementalProcessingCheckpointUtils.HISTORY_FILE_NAME);
        if (!fileSystem.exists(readFile)) {
            return null;
        }

        try (DataInputStream dataInputStream = new DataInputStream(fileSystem.open(readFile))) {
            return IncrementalProcessingCheckpointUtils.parseHistory(dataInputStream);
        }
    }
}

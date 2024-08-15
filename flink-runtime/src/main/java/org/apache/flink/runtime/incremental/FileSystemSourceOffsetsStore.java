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
 * Implementation of {@link SourceOffsetsStore} that stores the incremental processing {@link
 * History} and {@link SourceOffsets} in a {@link FileSystem}.
 *
 * <p>Write and read operations are performed synchronously, with the calling thread directly
 * interacting with the file system.
 */
public class FileSystemSourceOffsetsStore implements SourceOffsetsStore {
    private static final Logger LOG = LoggerFactory.getLogger(FileSystemSourceOffsetsStore.class);
    private static final String SUB_DIR = "/incremental-processing";
    private static final String FILE_NAME = "source-offsets";
    private static final String HISTORY_FILE_NAME = "history";
    private final Path workingDir;
    private final FileSystem fileSystem;
    private final SimpleVersionedSerializer<SourceOffsets> sourceOffsetsSerializer;
    private final SimpleVersionedSerializer<History> historySerializer;

    public FileSystemSourceOffsetsStore(String path) throws IOException {
        this.workingDir = new Path(path + SUB_DIR);
        this.fileSystem = this.workingDir.getFileSystem();
        if (!fileSystem.exists(workingDir)) {
            fileSystem.mkdirs(workingDir);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Create incremental processing dir {}.", workingDir);
            }
        }
        this.sourceOffsetsSerializer = SourceOffsetsSerializer.INSTANCE;
        this.historySerializer = HistorySerializer.INSTANCE;
    }

    @VisibleForTesting
    Path getWorkingDir() {
        return workingDir;
    }

    @Override
    public void writeSourceOffsets(
            SourceOffsets sourceOffsets, HistoryRecord historyRecord, int numRetainedHistory)
            throws Exception {
        try {
            History retainedHistory = readHistory();
            if (retainedHistory == null) {
                retainedHistory = new History(numRetainedHistory);
            } else {
                retainedHistory.updateNumRetainedHistory(numRetainedHistory);
            }
            retainedHistory.addHistoryRecord(historyRecord);
            // Writes history
            Path historyFile = new Path(workingDir, HISTORY_FILE_NAME);
            DataOutputStream historyOutputStream =
                    new DataOutputStream(
                            fileSystem.create(historyFile, FileSystem.WriteMode.OVERWRITE));
            byte[] binaryHistory = historySerializer.serialize(retainedHistory);
            historyOutputStream.writeInt(binaryHistory.length);
            historyOutputStream.write(binaryHistory);
            historyOutputStream.flush();
            // Writes source offsets
            Path file = new Path(workingDir, FILE_NAME);
            DataOutputStream outputStream =
                    new DataOutputStream(fileSystem.create(file, FileSystem.WriteMode.OVERWRITE));
            byte[] binarySourceOffsets = sourceOffsetsSerializer.serialize(sourceOffsets);
            outputStream.writeInt(binarySourceOffsets.length);
            outputStream.write(binarySourceOffsets);
            outputStream.flush();
        } catch (Exception e) {
            throw new IOException("Fail to write source offsets.", e);
        }
    }

    @Override
    public SourceOffsets readSourceOffsets() {
        try {
            Path readFile = new Path(workingDir, FILE_NAME);
            DataInputStream inputStream = new DataInputStream(fileSystem.open(readFile));
            int binarySize = inputStream.readInt();
            byte[] binarySourceOffsets = new byte[binarySize];
            inputStream.readFully(binarySourceOffsets);
            return sourceOffsetsSerializer.deserialize(
                    SourceOffsetsSerializer.INSTANCE.getVersion(), binarySourceOffsets);
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Cannot read source offsets.", e);
            }
            return null;
        }
    }

    @Override
    public History readHistory() {
        try {
            Path readFile = new Path(workingDir, HISTORY_FILE_NAME);
            DataInputStream inputStream = new DataInputStream(fileSystem.open(readFile));
            int binarySize = inputStream.readInt();
            byte[] binarySourceOffsets = new byte[binarySize];
            inputStream.readFully(binarySourceOffsets);
            return historySerializer.deserialize(
                    HistorySerializer.INSTANCE.getVersion(), binarySourceOffsets);
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Cannot read history.", e);
            }
            return null;
        }
    }
}

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

import java.io.DataInputStream;
import java.io.IOException;

/** Utility methods for incremental processing checkpoint. */
public class IncrementalProcessingCheckpointUtils {
    public static final String SUB_DIR = "/incremental-processing";
    public static final String SOURCE_OFFSETS_FILE_NAME = "source-offsets";
    public static final String HISTORY_FILE_NAME = "history";

    /**
     * The method for parsing history is extracted because it may be used outside of {@link
     * FileSystemIncrementalBatchCheckpointStore}.
     */
    public static History parseHistory(DataInputStream dataInputStream) throws IOException {
        int binarySize = dataInputStream.readInt();
        byte[] binarySourceOffsets = new byte[binarySize];
        dataInputStream.readFully(binarySourceOffsets);
        return HistorySerializer.INSTANCE.deserialize(
                HistorySerializer.INSTANCE.getVersion(), binarySourceOffsets);
    }
}

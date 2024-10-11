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

import org.apache.flink.incremental.SourceOffsets;

/** A class that holds the content of checkpoint for incremental processing. */
public class IncrementalBatchCheckpoint {
    private final SourceOffsets sourceOffsets;
    private final History history;

    public IncrementalBatchCheckpoint(SourceOffsets sourceOffsets, History history) {
        this.sourceOffsets = sourceOffsets;
        this.history = history;
    }

    public SourceOffsets getSourceOffsets() {
        return sourceOffsets;
    }

    public History getHistory() {
        return history;
    }
}

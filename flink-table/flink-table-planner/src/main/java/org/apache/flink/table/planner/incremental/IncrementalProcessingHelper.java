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
import org.apache.flink.incremental.SourceOffsets;
import org.apache.flink.runtime.incremental.FileSystemIncrementalBatchCheckpointStore;
import org.apache.flink.runtime.incremental.IncrementalBatchCheckpointStore;
import org.apache.flink.table.api.TableConfig;

import org.apache.calcite.rel.RelNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.configuration.BatchIncrementalExecutionOptions.INCREMENTAL_CHECKPOINTS_DIRECTORY;
import static org.apache.flink.configuration.BatchIncrementalExecutionOptions.INCREMENTAL_EXECUTION_MODE;
import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY;

/** Utility class for incremental processing. */
public class IncrementalProcessingHelper {
    private static final Logger LOG = LoggerFactory.getLogger(IncrementalProcessingHelper.class);

    /**
     * This method tries to generate an incremental processing plan under the following conditions:
     *
     * <p>1. Batch mode. 2. The configured incremental execution mode is AUTO. 3. The incremental
     * checkpoints directory is configured.
     *
     * <p>If all these conditions are met, it will use {@link IncrementalProcessingShuttle} to try
     * to generate an incremental plan. It returns {@link IncrementalProcessingPlan} if the
     * incremental plan is successfully generated, otherwise returns null.
     */
    public static IncrementalProcessingPlan tryGenerateIncrementalProcessingPlan(
            List<RelNode> relNodes, TableConfig tableConfig, boolean isStreamingMode)
            throws IOException {
        if (isStreamingMode
                || tableConfig.get(INCREMENTAL_EXECUTION_MODE)
                        == BatchIncrementalExecutionOptions.IncrementalExecutionMode.DISABLED) {
            return null;
        }

        String checkpointsDir = tableConfig.get(INCREMENTAL_CHECKPOINTS_DIRECTORY);
        if (checkpointsDir == null) {
            checkpointsDir = tableConfig.get(CHECKPOINTS_DIRECTORY);
        }
        if (checkpointsDir == null) {
            return null;
        }

        LOG.info("Start trying to generate an incremental plan.");
        IncrementalBatchCheckpointStore store =
                new FileSystemIncrementalBatchCheckpointStore(checkpointsDir);
        SourceOffsets restoredOffsets = store.getLatestSourceOffsets();
        boolean incremental = false;
        List<RelNode> maybeIncrementalRelNodes = new ArrayList<>();

        IncrementalProcessingShuttle shuttle =
                new IncrementalProcessingShuttle(tableConfig, restoredOffsets);
        for (RelNode relNode : relNodes) {
            RelNode result = relNode.accept(shuttle);
            if (result != null) {
                incremental = true;
                maybeIncrementalRelNodes.add(result);
            } else {
                maybeIncrementalRelNodes.add(relNode);
            }
        }

        if (incremental) {
            LOG.info(
                    "Generated an incremental plan with start offsets: {}, end offsets: {}.",
                    shuttle.getCachedStartOffsets(),
                    shuttle.getCachedEndOffsets());
            return new IncrementalProcessingPlan(
                    maybeIncrementalRelNodes, shuttle.getCachedEndOffsets());
        } else {
            LOG.info("Cannot generate an incremental plan.");
            return null;
        }
    }
}

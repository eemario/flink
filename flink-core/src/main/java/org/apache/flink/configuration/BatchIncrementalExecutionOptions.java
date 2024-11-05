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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;

import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY;

/** The set of configuration options relating to incremental execution. */
@PublicEvolving
public class BatchIncrementalExecutionOptions {
    public static final String SCAN_RANGE_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String SCAN_RANGE_START_AUTO = "AUTO";
    public static final String SCAN_RANGE_START_EARLIEST = "EARLIEST";
    public static final String SCAN_RANGE_END_LATEST = "LATEST";

    /** The mode of incremental execution. */
    public enum IncrementalExecutionMode {
        DISABLED,
        AUTO
    }

    @Documentation.ExcludeFromDocumentation
    public static final ConfigOption<IncrementalExecutionMode> INCREMENTAL_EXECUTION_MODE =
            ConfigOptions.key("execution.batch.incremental.mode")
                    .enumType(IncrementalExecutionMode.class)
                    .defaultValue(IncrementalExecutionMode.DISABLED)
                    .withDescription(
                            "The mode of incremental execution. It can be 'AUTO' or 'DISABLED'."
                                    + " When it is set to 'AUTO', Flink will run a batch job in the way of incremental computing if possible."
                                    + " When it is set to 'DISABLED', Flink will not use incremental computing.");

    @Documentation.ExcludeFromDocumentation
    public static final ConfigOption<String> INCREMENTAL_CHECKPOINTS_DIRECTORY =
            ConfigOptions.key("execution.batch.incremental.checkpoints.dir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The directory for incremental execution checkpoints."
                                                    + " If not configured, it will default to %s.",
                                            TextElement.code(CHECKPOINTS_DIRECTORY.key()))
                                    .build());

    @Documentation.ExcludeFromDocumentation
    public static final ConfigOption<Integer> INCREMENTAL_CHECKPOINTS_HISTORY_SIZE =
            ConfigOptions.key("execution.batch.incremental.checkpoints.history-size")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "The number of retained history records for incremental execution.");

    @Documentation.ExcludeFromDocumentation
    public static final ConfigOption<Integer> INCREMENTAL_CHECKPOINTS_ATTEMPTS =
            ConfigOptions.key("execution.batch.incremental.checkpoints.attempts")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "The number of retry attempts when storing checkpoints for incremental execution.");

    @Documentation.ExcludeFromDocumentation
    public static final ConfigOption<String> INCREMENTAL_SCAN_RANGE_START_TIMESTAMP =
            ConfigOptions.key("execution.batch.incremental.scan-range.start-timestamp")
                    .stringType()
                    .defaultValue(SCAN_RANGE_START_AUTO)
                    .withDescription(
                            "The (exclusive) start timestamp of the scan range. It can be "
                                    + SCAN_RANGE_TIMESTAMP_FORMAT
                                    + " format string, 'EARLIEST' or 'AUTO'."
                                    + " When set to 'EARLIEST', it means using the time before all data as start."
                                    + " When set to 'AUTO', it means using the previously stored offsets as start or the time before all data if no offsets are stored.");

    @Documentation.ExcludeFromDocumentation
    public static final ConfigOption<String> INCREMENTAL_SCAN_RANGE_END_TIMESTAMP =
            ConfigOptions.key("execution.batch.incremental.scan-range.end-timestamp")
                    .stringType()
                    .defaultValue(SCAN_RANGE_END_LATEST)
                    .withDescription(
                            "The (inclusive) end timestamp of the scan range. It can be "
                                    + SCAN_RANGE_TIMESTAMP_FORMAT
                                    + " format string or 'LATEST'."
                                    + " When set to 'LATEST', it means using the system time at the moment of generating the incremental plan as end.");
}

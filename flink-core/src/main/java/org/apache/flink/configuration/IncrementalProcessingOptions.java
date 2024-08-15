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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.InlineElement;
import org.apache.flink.configuration.description.TextElement;

import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY;
import static org.apache.flink.configuration.description.TextElement.text;

/** The set of configuration options relating to incremental processing. */
@PublicEvolving
public class IncrementalProcessingOptions {
    public static final String SCAN_RANGE_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String SCAN_RANGE_START_AUTO = "AUTO";
    public static final String SCAN_RANGE_START_EARLIEST = "EARLIEST";
    public static final String SCAN_RANGE_END_LATEST = "LATEST";

    /** The mode of incremental processing. */
    public enum IncrementalProcessingMode implements DescribedEnum {
        DISABLED(text("Incremental processing is disabled")),
        AUTO(text("Incremental processing will be applied when possible"));

        private final InlineElement description;

        IncrementalProcessingMode(InlineElement description) {
            this.description = description;
        }

        @Override
        public InlineElement getDescription() {
            return description;
        }
    }

    @Experimental @Documentation.ExcludeFromDocumentation
    public static final ConfigOption<IncrementalProcessingMode> INCREMENTAL_PROCESSING_MODE =
            ConfigOptions.key("incremental-processing.mode")
                    .enumType(IncrementalProcessingMode.class)
                    .defaultValue(IncrementalProcessingMode.DISABLED)
                    .withDescription(
                            "The mode of incremental processing. "
                                    + "When set to AUTO, incremental processing will be applied when possible.");

    @Experimental @Documentation.ExcludeFromDocumentation
    public static final ConfigOption<String> INCREMENTAL_PROCESSING_CHECKPOINTS_PATH =
            ConfigOptions.key("incremental-processing.checkpoints.path")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys(CHECKPOINTS_DIRECTORY.key())
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The path of incremental processing checkpoints. "
                                                    + "If not configured, it will default to %s.",
                                            TextElement.code(CHECKPOINTS_DIRECTORY.key()))
                                    .build());

    @Experimental @Documentation.ExcludeFromDocumentation
    public static final ConfigOption<Integer> INCREMENTAL_PROCESSING_NUM_RETAINED_HISTORY =
            ConfigOptions.key("incremental-processing.num-retained-history")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "The number of retained history records in incremental processing.");

    @Experimental @Documentation.ExcludeFromDocumentation
    public static final ConfigOption<Integer> INCREMENTAL_PROCESSING_STORE_RETRY_ATTEMPTS =
            ConfigOptions.key("incremental-processing.store-retry-attempts")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "The number of retry attempts when storing source offsets in incremental processing.");

    @Experimental @Documentation.ExcludeFromDocumentation
    public static final ConfigOption<String> INCREMENTAL_PROCESSING_SCAN_RANGE_START_TIMESTAMP =
            ConfigOptions.key("incremental-processing.scan-range.start-timestamp")
                    .stringType()
                    .defaultValue(SCAN_RANGE_START_AUTO)
                    .withDescription(
                            "The (exclusive) start timestamp of the scan range. It can be "
                                    + SCAN_RANGE_TIMESTAMP_FORMAT
                                    + " format string, 'EARLIEST' or 'AUTO'.");

    @Experimental @Documentation.ExcludeFromDocumentation
    public static final ConfigOption<String> INCREMENTAL_PROCESSING_SCAN_RANGE_END_TIMESTAMP =
            ConfigOptions.key("incremental-processing.scan-range.end-timestamp")
                    .stringType()
                    .defaultValue(SCAN_RANGE_END_LATEST)
                    .withDescription(
                            "The (inclusive) end timestamp of the scan range. It can be "
                                    + SCAN_RANGE_TIMESTAMP_FORMAT
                                    + " format string or 'LATEST'.");

    @Experimental @Documentation.ExcludeFromDocumentation
    public static final ConfigOption<String>
            INCREMENTAL_PROCESSING_INIT_SCAN_RANGE_START_TIMESTAMP =
                    ConfigOptions.key("incremental-processing.init-scan-range.start-timestamp")
                            .stringType()
                            .noDefaultValue()
                            .withFallbackKeys(
                                    INCREMENTAL_PROCESSING_SCAN_RANGE_START_TIMESTAMP.key())
                            .withDescription(
                                    Description.builder()
                                            .text(
                                                    "The (exclusive) start timestamp of the scan range for the initial processing. It can be "
                                                            + SCAN_RANGE_TIMESTAMP_FORMAT
                                                            + " format string, 'EARLIEST' or 'AUTO'. If not configured, it will default to %s.",
                                                    TextElement.code(
                                                            INCREMENTAL_PROCESSING_SCAN_RANGE_START_TIMESTAMP
                                                                    .key()))
                                            .build());

    @Experimental @Documentation.ExcludeFromDocumentation
    public static final ConfigOption<String> INCREMENTAL_PROCESSING_INIT_SCAN_RANGE_END_TIMESTAMP =
            ConfigOptions.key("incremental-processing.init-scan-range.end-timestamp")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys(INCREMENTAL_PROCESSING_SCAN_RANGE_END_TIMESTAMP.key())
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The (inclusive) end timestamp of the scan range for the initial processing. It can be "
                                                    + SCAN_RANGE_TIMESTAMP_FORMAT
                                                    + " format string or 'LATEST'. If not configured, it will default to %s.",
                                            TextElement.code(
                                                    INCREMENTAL_PROCESSING_SCAN_RANGE_END_TIMESTAMP
                                                            .key()))
                                    .build());
}

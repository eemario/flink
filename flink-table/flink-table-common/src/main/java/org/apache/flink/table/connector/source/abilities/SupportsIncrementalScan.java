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

package org.apache.flink.table.connector.source.abilities;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.incremental.Offset;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;

import javax.annotation.Nullable;

/**
 * Interface for {@link ScanTableSource}s that support incremental scan.
 *
 * <p>Implementations of this interface provide the necessary functionality to obtain offsets
 * required for incremental processing and to configure the scan range based on the offsets.
 *
 * @see Offset
 */
@PublicEvolving
public interface SupportsIncrementalScan {
    /**
     * Returns a new instance of {@link DynamicTableSource} configured with a specific scan range.
     * This method is used to create a table source that will only read data within the specified
     * range defined by the start and end {@link Offset}s.
     *
     * @param start The (exclusive) starting offset of the scan range.
     * @param end The (inclusive) ending offset of the scan range.
     * @return A new instance of DynamicTableSource configured with the specified scan range.
     */
    @Nullable
    DynamicTableSource withScanRange(Offset start, Offset end);

    /**
     * Retrieves the end offset for the incremental scan. This method returns the offset that
     * represents the (inclusive) end of the data that can be scanned at the moment.
     *
     * @return The end offset for the incremental scan.
     */
    Offset getEndOffset();

    /**
     * Retrieves the offset at a given timestamp. This method is used to determine the offset that
     * corresponds to a specific timestamp, which can be useful for aligning incremental offsets
     * with the timestamps provided by the users.
     *
     * @param timestamp The timestamp for which to retrieve the offset.
     * @return The offset corresponding to the given timestamp.
     */
    Offset getOffset(long timestamp);
}

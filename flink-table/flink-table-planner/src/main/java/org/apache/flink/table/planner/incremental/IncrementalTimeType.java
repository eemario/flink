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

import org.apache.flink.table.delegation.Planner;

/**
 * Enum representing the abstract time semantics used during the {@link Planner}'s incremental
 * processing plan generation.
 *
 * <p>An incremental processing plan produces results for the time interval (start, end]. There are
 * three types of abstract time semantics involved in the plan generation: FULL_NEW, FULL_OLD, and
 * DELTA, representing the data for (EARLIEST, end], (EARLIEST, start], and (start, end]
 * respectively, where EARLIEST indicates the time point before all data.
 */
public enum IncrementalTimeType {
    /** Represents the data for the time interval (EARLIEST, end]. */
    FULL_NEW,
    /** Represents the data for the time interval (EARLIEST, start]. */
    FULL_OLD,
    /** Represents the data for the time interval (start, end]. */
    DELTA
}

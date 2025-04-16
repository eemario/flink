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

package org.apache.flink.runtime.application;

import org.apache.flink.shaded.guava33.com.google.common.collect.BiMap;
import org.apache.flink.shaded.guava33.com.google.common.collect.EnumBiMap;

import java.util.Optional;

/** Possible states of an application once it has been accepted by the dispatcher. */
public enum ApplicationStatus {
    CREATED,
    RUNNING,
    FINISHED,
    FAILED,
    CANCELED,
    UNKNOWN;

    // ------------------------------------------------------------------------

    private static final BiMap<
                    org.apache.flink.runtime.clusterframework.ApplicationStatus, ApplicationStatus>
            APPLICATION_STATUS_BI_MAP =
                    EnumBiMap.create(
                            org.apache.flink.runtime.clusterframework.ApplicationStatus.class,
                            ApplicationStatus.class);

    static {
        APPLICATION_STATUS_BI_MAP.put(
                org.apache.flink.runtime.clusterframework.ApplicationStatus.FAILED, FAILED);
        APPLICATION_STATUS_BI_MAP.put(
                org.apache.flink.runtime.clusterframework.ApplicationStatus.CANCELED, CANCELED);
        APPLICATION_STATUS_BI_MAP.put(
                org.apache.flink.runtime.clusterframework.ApplicationStatus.SUCCEEDED, FINISHED);
    }

    public static Optional<ApplicationStatus> fromApplicationStatus(
            org.apache.flink.runtime.clusterframework.ApplicationStatus applicationStatus) {
        return Optional.ofNullable(APPLICATION_STATUS_BI_MAP.getOrDefault(applicationStatus, null));
    }
}

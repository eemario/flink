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

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;

import java.io.Serializable;
import java.util.Collection;

/** Read-only information about an {@link AbstractApplication}. */
public class ArchivedApplication implements Serializable {

    private static final long serialVersionUID = 7231383912742578429L;

    private final ApplicationID applicationId;

    private final String applicationName;

    private final ApplicationState applicationState;

    private final long[] statusTimestamps;

    private final Collection<JobDetails> jobs;

    private final Collection<ApplicationExceptionHistoryEntry> exceptionHistory;

    public ArchivedApplication(
            ApplicationID applicationId,
            String applicationName,
            ApplicationState applicationState,
            long[] statusTimestamps,
            Collection<JobDetails> jobs,
            Collection<ApplicationExceptionHistoryEntry> exceptionHistory) {
        this.applicationId = applicationId;
        this.applicationName = applicationName;
        this.applicationState = applicationState;
        this.statusTimestamps = statusTimestamps;
        this.jobs = jobs;
        this.exceptionHistory = exceptionHistory;
    }

    public ApplicationID getApplicationId() {
        return applicationId;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public ApplicationState getApplicationStatus() {
        return applicationState;
    }

    public long getStatusTimestamp(ApplicationState status) {
        return this.statusTimestamps[status.ordinal()];
    }

    public Collection<JobDetails> getJobs() {
        return jobs;
    }

    public Collection<ApplicationExceptionHistoryEntry> getExceptionHistory() {
        return exceptionHistory;
    }
}

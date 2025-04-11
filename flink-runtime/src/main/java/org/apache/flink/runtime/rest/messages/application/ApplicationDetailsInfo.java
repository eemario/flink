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

package org.apache.flink.runtime.rest.messages.application;

import org.apache.flink.runtime.application.ApplicationID;
import org.apache.flink.runtime.application.ApplicationStatus;
import org.apache.flink.runtime.messages.webmonitor.JobIdsWithStatusOverview;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.json.ApplicationIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.ApplicationIDSerializer;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/** Details about an application. */
public class ApplicationDetailsInfo implements ResponseBody {

    public static final String FIELD_NAME_APPLICATION_ID = "id";

    public static final String FIELD_NAME_APPLICATION_STATUS = "status";

    public static final String FIELD_NAME_TIMESTAMPS = "timestamps";

    public static final String FIELD_NAME_JOBS = "jobs";

    @JsonProperty(FIELD_NAME_APPLICATION_ID)
    @JsonSerialize(using = ApplicationIDSerializer.class)
    private final ApplicationID applicationId;

    @JsonProperty(FIELD_NAME_APPLICATION_STATUS)
    private final ApplicationStatus applicationStatus;

    @JsonProperty(FIELD_NAME_TIMESTAMPS)
    private final Map<ApplicationStatus, Long> timestamps;

    @JsonProperty(FIELD_NAME_JOBS)
    private final Collection<JobIdsWithStatusOverview.JobIdWithStatus> jobs;

    @JsonCreator
    public ApplicationDetailsInfo(
            @JsonDeserialize(using = ApplicationIDDeserializer.class)
                    @JsonProperty(FIELD_NAME_APPLICATION_ID)
                    ApplicationID applicationId,
            @JsonProperty(FIELD_NAME_APPLICATION_STATUS) ApplicationStatus applicationStatus,
            @JsonProperty(FIELD_NAME_TIMESTAMPS) Map<ApplicationStatus, Long> timestamps,
            @JsonProperty(FIELD_NAME_JOBS)
                    Collection<JobIdsWithStatusOverview.JobIdWithStatus> jobs) {
        this.applicationId = Preconditions.checkNotNull(applicationId);
        this.applicationStatus = Preconditions.checkNotNull(applicationStatus);
        this.timestamps = Preconditions.checkNotNull(timestamps);
        this.jobs = Preconditions.checkNotNull(jobs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ApplicationDetailsInfo that = (ApplicationDetailsInfo) o;
        return applicationStatus == that.applicationStatus
                && Objects.equals(applicationId, that.applicationId)
                && Objects.equals(timestamps, that.timestamps)
                && Objects.equals(jobs, that.jobs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(applicationId, applicationStatus, timestamps, jobs);
    }

    public ApplicationID getApplicationId() {
        return applicationId;
    }

    public ApplicationStatus getApplicationStatus() {
        return applicationStatus;
    }

    public Map<ApplicationStatus, Long> getTimestamps() {
        return timestamps;
    }

    public Collection<JobIdsWithStatusOverview.JobIdWithStatus> getJobs() {
        return jobs;
    }
}

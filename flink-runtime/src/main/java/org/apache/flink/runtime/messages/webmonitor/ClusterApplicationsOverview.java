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

package org.apache.flink.runtime.messages.webmonitor;

import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherFactory;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.application.ApplicationDetailsInfo;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** An overview of the cluster including mode and all the applications in it. */
public class ClusterApplicationsOverview implements ResponseBody, InfoMessage {
    private static final long serialVersionUID = 1L;

    public static final String FIELD_NAME_CLUSTER = "cluster";

    @JsonProperty(FIELD_NAME_CLUSTER)
    private final ClusterInfo cluster;

    @JsonCreator
    public ClusterApplicationsOverview(@JsonProperty(FIELD_NAME_CLUSTER) ClusterInfo cluster) {
        this.cluster = checkNotNull(cluster);
    }

    public ClusterInfo getCluster() {
        return cluster;
    }

    // ------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return cluster.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof ClusterApplicationsOverview) {
            ClusterApplicationsOverview that = (ClusterApplicationsOverview) obj;
            return cluster.equals(that.getCluster());
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "ClusterApplicationsOverview {" + cluster + "}";
    }

    // -------------------------------------------------------------------------
    // Static classes
    // -------------------------------------------------------------------------

    public static final class ClusterInfo implements Serializable {

        private static final long serialVersionUID = 1L;

        public static final String FIELD_NAME_CLUSTER_MODE = "mode";

        public static final String FIELD_NAME_CLUSTER_STATUS = "status";

        public static final String FIELD_NAME_TIMESTAMPS = "timestamps";

        public static final String FIELD_NAME_APPLICATIONS = "applications";

        @JsonProperty(FIELD_NAME_CLUSTER_MODE)
        private final DispatcherFactory.DispatcherMode mode;

        @JsonProperty(FIELD_NAME_CLUSTER_STATUS)
        private final Dispatcher.Status status;

        @JsonProperty(FIELD_NAME_TIMESTAMPS)
        private final Map<Dispatcher.Status, Long> timestamps;

        @JsonProperty(FIELD_NAME_APPLICATIONS)
        private final Collection<ApplicationDetailsInfo> applications;

        @JsonCreator
        public ClusterInfo(
                @JsonProperty(FIELD_NAME_CLUSTER_MODE) DispatcherFactory.DispatcherMode mode,
                @JsonProperty(FIELD_NAME_CLUSTER_STATUS) Dispatcher.Status status,
                @JsonProperty(FIELD_NAME_TIMESTAMPS) Map<Dispatcher.Status, Long> timestamps,
                @JsonProperty(FIELD_NAME_APPLICATIONS)
                        Collection<ApplicationDetailsInfo> applications) {
            this.mode = Preconditions.checkNotNull(mode);
            this.status = Preconditions.checkNotNull(status);
            this.timestamps = Preconditions.checkNotNull(timestamps);
            this.applications = Preconditions.checkNotNull(applications);
        }

        public DispatcherFactory.DispatcherMode getMode() {
            return mode;
        }

        public Dispatcher.Status getStatus() {
            return status;
        }

        public Map<Dispatcher.Status, Long> getTimestamps() {
            return timestamps;
        }

        public Collection<ApplicationDetailsInfo> getApplications() {
            return applications;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ClusterInfo that = (ClusterInfo) o;
            return mode == that.mode
                    && status == that.status
                    && Objects.equals(timestamps, that.timestamps)
                    && Objects.equals(applications, that.applications);
        }

        @Override
        public int hashCode() {
            return Objects.hash(mode, status, timestamps, applications);
        }

        @Override
        public String toString() {
            return "ClusterInfo {"
                    + "mode="
                    + mode
                    + ", status="
                    + status
                    + ", timestamps="
                    + timestamps
                    + ", applications="
                    + applications
                    + '}';
        }
    }
}

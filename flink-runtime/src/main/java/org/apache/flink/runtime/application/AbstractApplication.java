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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.client.JobStatusPollingUtils;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/** Instance of an application. */
public abstract class AbstractApplication implements Serializable {

    private final ApplicationID applicationId;

    private final Set<JobID> jobs = new HashSet<>();

    private ApplicationStatus applicationStatus = ApplicationStatus.CREATED;

    /**
     * Timestamps (in milliseconds as returned by {@code System.currentTimeMillis()} when the
     * application transitioned into a certain status. The index into this array is the ordinal of
     * the enum value, i.e. the timestamp when the application went into state "RUNNING" is at
     * {@code timestamps[RUNNING.ordinal()]}.
     */
    private final long[] statusTimestamps;

    public AbstractApplication(ApplicationID applicationId) {
        this.applicationId = applicationId;
        this.statusTimestamps = new long[ApplicationStatus.values().length];
        this.statusTimestamps[ApplicationStatus.CREATED.ordinal()] = System.currentTimeMillis();
    }

    public ApplicationID getApplicationId() {
        return applicationId;
    }

    public ApplicationStatus getApplicationStatus() {
        return applicationStatus;
    }

    public void setApplicationStatus(ApplicationStatus applicationStatus) {
        this.statusTimestamps[applicationStatus.ordinal()] = System.currentTimeMillis();
        this.applicationStatus = applicationStatus;
    }

    public long getStatusTimestamp(ApplicationStatus status) {
        return this.statusTimestamps[status.ordinal()];
    }

    public Map<ApplicationStatus, Long> getStatusTimestamps() {
        final Map<ApplicationStatus, Long> timestamps =
                CollectionUtil.newHashMapWithExpectedSize(ApplicationStatus.values().length);

        for (ApplicationStatus applicationStatus : ApplicationStatus.values()) {
            timestamps.put(applicationStatus, getStatusTimestamp(applicationStatus));
        }
        return timestamps;
    }

    public Set<JobID> getJobs() {
        return jobs;
    }

    public boolean addJob(JobID jobId) {
        return jobs.add(jobId);
    }

    public abstract CompletableFuture<Acknowledge> runAsync(
            final DispatcherGateway dispatcherGateway,
            final ScheduledExecutor scheduledExecutor,
            final FatalErrorHandler errorHandler,
            final Duration timeout);

    public abstract void cancel() throws Exception;

    protected CompletableFuture<JobResult> getJobResult(
            final DispatcherGateway dispatcherGateway,
            final JobID jobId,
            final ScheduledExecutor scheduledExecutor,
            final boolean tolerateMissingResult,
            final Duration timeout,
            final Duration retryPeriod) {
        final CompletableFuture<JobResult> jobResultFuture =
                JobStatusPollingUtils.getJobResult(
                        dispatcherGateway, jobId, scheduledExecutor, timeout, retryPeriod);
        if (tolerateMissingResult) {
            // Return "unknown" job result if dispatcher no longer knows the actual result.
            return FutureUtils.handleException(
                    jobResultFuture,
                    FlinkJobNotFoundException.class,
                    exception ->
                            new JobResult.Builder()
                                    .jobId(jobId)
                                    .applicationStatus(
                                            org.apache.flink.runtime.clusterframework
                                                    .ApplicationStatus.UNKNOWN)
                                    .netRuntime(Long.MAX_VALUE)
                                    .build());
        }
        return jobResultFuture;
    }

    protected CompletableFuture<Void> getApplicationResult(
            final DispatcherGateway dispatcherGateway,
            final Collection<JobID> applicationJobIds,
            final Set<JobID> tolerateMissingResult,
            final ScheduledExecutor executor,
            final boolean terminateOnException,
            final Duration timeout,
            final Duration retryPeriod) {
        final List<CompletableFuture<?>> jobResultFutures =
                applicationJobIds.stream()
                        .map(
                                jobId ->
                                        unwrapJobResultException(
                                                getJobResult(
                                                        dispatcherGateway,
                                                        jobId,
                                                        executor,
                                                        tolerateMissingResult.contains(jobId),
                                                        timeout,
                                                        retryPeriod)))
                        .collect(Collectors.toList());
        return terminateOnException
                ? FutureUtils.waitForAll(jobResultFutures)
                : FutureUtils.completeAll(jobResultFutures);
    }

    protected abstract CompletableFuture<JobResult> unwrapJobResultException(
            final CompletableFuture<JobResult> jobResult);
}

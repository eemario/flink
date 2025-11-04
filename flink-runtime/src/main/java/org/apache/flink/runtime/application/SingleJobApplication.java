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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.blob.PermanentBlobService;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** An implementation of {@link AbstractApplication} designed for executing a single job. */
@Internal
public class SingleJobApplication extends AbstractApplication
        implements JobStatusListener, ApplicationStoreEntry {

    private static final Logger LOG = LoggerFactory.getLogger(SingleJobApplication.class);

    private final ExecutionPlan executionPlan;

    private final Duration rpcTimeout;

    private transient DispatcherGateway dispatcherGateway;

    private transient Executor mainThreadExecutor;

    private transient FatalErrorHandler errorHandler;

    private transient boolean isRecovered;

    public SingleJobApplication(ExecutionPlan executionPlan, Duration rpcTimeout) {
        super(executionPlan.getApplicationId());
        this.executionPlan = executionPlan;
        this.rpcTimeout = rpcTimeout;
    }

    public void setIsRecovered(boolean isRecovered) {
        this.isRecovered = isRecovered;
    }

    public ExecutionPlan getExecutionPlan() {
        return executionPlan;
    }

    @Override
    public ApplicationStoreEntry toApplicationStoreEntry() {
        return this;
    }

    @Override
    public AbstractApplication getApplication(
            PermanentBlobService blobService, Collection<JobID> recoveredJobIds) {
        return this;
    }

    @Override
    public CompletableFuture<Acknowledge> execute(
            final DispatcherGateway dispatcherGateway,
            final ScheduledExecutor scheduledExecutor,
            final Executor mainThreadExecutor,
            final FatalErrorHandler errorHandler) {
        transitionToRunning();

        this.dispatcherGateway = dispatcherGateway;
        this.mainThreadExecutor = mainThreadExecutor;
        this.errorHandler = errorHandler;

        if (isRecovered) {
            return CompletableFuture.completedFuture(Acknowledge.get());
        }

        return dispatcherGateway
                .submitJob(executionPlan, rpcTimeout)
                .handle(
                        (ack, t) -> {
                            if (t != null) {
                                LOG.error("Job submission failed.", t);
                                transitionToFailing();
                                transitionToFailed();
                                throw new CompletionException(t);
                            }
                            return ack;
                        });
    }

    @Override
    public void cancel() {
        ApplicationState currentState = getApplicationStatus();
        if (currentState == ApplicationState.CREATED) {
            // nothing to cancel
            transitionToCancelling();
            transitionToCanceled();
        } else if (currentState == ApplicationState.RUNNING) {
            transitionToCancelling();
            internalCancel();
        }
    }

    @Override
    public void dispose() {}

    @Override
    public String getName() {
        return "SingleJobApplication(" + executionPlan.getName() + ")";
    }

    @Override
    public void jobStatusChanges(JobID jobId, JobStatus newJobStatus, long timestamp) {
        if (newJobStatus.isGloballyTerminalState()) {
            checkNotNull(mainThreadExecutor);

            mainThreadExecutor.execute(
                    () -> {
                        LOG.info("Application completed with job status {}", newJobStatus);
                        if (newJobStatus == JobStatus.FINISHED) {
                            transitionToFinished();
                        } else if (newJobStatus == JobStatus.CANCELED) {
                            if (getApplicationStatus() != ApplicationState.CANCELLING) {
                                transitionToCancelling();
                            }
                            transitionToCanceled();
                        } else {
                            transitionToFailing();
                            transitionToFailed();
                        }
                    });
        }
    }

    private void internalCancel() {
        checkNotNull(dispatcherGateway);
        checkNotNull(errorHandler);

        final JobID jobId = executionPlan.getJobID();
        dispatcherGateway
                .requestJobStatus(jobId, rpcTimeout)
                .thenCompose(
                        jobStatus -> {
                            if (!jobStatus.isGloballyTerminalState()) {
                                LOG.info(
                                        "Cancelling job for application '{}' ({})",
                                        getName(),
                                        getApplicationId());
                                return dispatcherGateway.cancelJob(jobId, rpcTimeout);
                            }
                            return CompletableFuture.completedFuture(Acknowledge.get());
                        })
                .exceptionally(
                        t -> {
                            errorHandler.onFatalError(t);
                            return null;
                        });
    }
}

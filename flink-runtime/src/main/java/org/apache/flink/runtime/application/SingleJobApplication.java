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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ClientOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.runtime.client.UnsuccessfulExecutionException;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import static org.apache.flink.runtime.application.ApplicationStatus.fromApplicationStatus;

public class SingleJobApplication extends AbstractApplication {

    private static final Logger LOG = LoggerFactory.getLogger(SingleJobApplication.class);

    private static boolean isCanceledOrFailed(
            org.apache.flink.runtime.clusterframework.ApplicationStatus applicationStatus) {
        return applicationStatus
                        == org.apache.flink.runtime.clusterframework.ApplicationStatus.CANCELED
                || applicationStatus
                        == org.apache.flink.runtime.clusterframework.ApplicationStatus.FAILED;
    }

    private final ExecutionPlan executionPlan;

    private final Configuration configuration;

    private final boolean needJobSubmission;

    private transient CompletableFuture<Void> applicationCompletionFuture;

    private transient CompletableFuture<Acknowledge> finishApplicationFuture;

    public SingleJobApplication(
            final ExecutionPlan executionPlan,
            final Configuration configuration,
            final boolean needJobSubmission) {
        super(executionPlan.getApplicationID());
        this.executionPlan = executionPlan;
        this.configuration = configuration;
        this.needJobSubmission = needJobSubmission;
    }

    @VisibleForTesting
    public ExecutionPlan getExecutionPlan() {
        return executionPlan;
    }

    @Override
    public CompletableFuture<Acknowledge> runAsync(
            DispatcherGateway dispatcherGateway,
            ScheduledExecutor scheduledExecutor,
            FatalErrorHandler errorHandler,
            Duration timeout) {
        LOG.info(
                "Start running application ({}) with a single job ({}).",
                getApplicationId(),
                executionPlan.getJobID());
        setApplicationStatus(ApplicationStatus.RUNNING);
        CompletableFuture<Acknowledge> jobSubmissionFuture =
                dispatcherGateway.submitJob(executionPlan, timeout);

        applicationCompletionFuture =
                jobSubmissionFuture
                        .handle(
                                (ignored, throwable) -> {
                                    if (throwable != null) {
                                        throw new CompletionException(throwable);
                                    }
                                    return getApplicationResult(
                                            dispatcherGateway,
                                            Collections.singletonList(executionPlan.getJobID()),
                                            Collections.emptySet(),
                                            scheduledExecutor,
                                            configuration.get(
                                                    DeploymentOptions
                                                            .TERMINATE_APPLICATION_ON_ANY_JOB_EXCEPTION),
                                            configuration.get(ClientOptions.CLIENT_TIMEOUT),
                                            configuration.get(ClientOptions.CLIENT_RETRY_PERIOD));
                                })
                        .thenCompose(Function.identity());

        finishApplicationFuture = finishApplication(dispatcherGateway);

        // not blocking by job execution
        return jobSubmissionFuture;
    }

    @Override
    public void cancel() throws Exception {}

    /**
     * Logs final application status and invokes error handler in case of unexpected failures.
     * Optionally shuts down the given dispatcherGateway when the application completes (either
     * successfully or in case of failure), depending on the corresponding config option.
     */
    private CompletableFuture<Acknowledge> finishApplication(
            final DispatcherGateway dispatcherGateway) {
        return applicationCompletionFuture
                .handle(
                        (ignored, t) -> {
                            if (t == null) {
                                LOG.info("Application completed SUCCESSFULLY");
                                return finish(
                                        org.apache.flink.runtime.clusterframework.ApplicationStatus
                                                .SUCCEEDED);
                            }
                            final Optional<
                                            org.apache.flink.runtime.clusterframework
                                                    .ApplicationStatus>
                                    maybeApplicationStatus = extractApplicationStatus(t);
                            if (maybeApplicationStatus.isPresent()
                                    && isCanceledOrFailed(maybeApplicationStatus.get())) {
                                final org.apache.flink.runtime.clusterframework.ApplicationStatus
                                        applicationStatus = maybeApplicationStatus.get();
                                LOG.info("Application {}", applicationStatus);
                                return finish(applicationStatus);
                            }
                            if (t instanceof CancellationException) {
                                LOG.warn(
                                        "Application has been cancelled because the {} is being stopped.",
                                        SingleJobApplication.class.getSimpleName());
                                setApplicationStatus(
                                        org.apache.flink.runtime.application.ApplicationStatus
                                                .CANCELED);
                                return CompletableFuture.completedFuture(Acknowledge.get());
                            }
                            LOG.warn("Application failed unexpectedly: ", t);
                            setApplicationStatus(
                                    org.apache.flink.runtime.application.ApplicationStatus.FAILED);
                            return FutureUtils.<Acknowledge>completedExceptionally(t);
                        })
                .thenCompose(Function.identity());
    }

    private CompletableFuture<Acknowledge> finish(
            org.apache.flink.runtime.clusterframework.ApplicationStatus applicationStatus) {
        setApplicationStatus(fromApplicationStatus(applicationStatus).orElseThrow());
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    private Optional<org.apache.flink.runtime.clusterframework.ApplicationStatus>
            extractApplicationStatus(Throwable t) {
        final Optional<UnsuccessfulExecutionException> maybeException =
                ExceptionUtils.findThrowable(t, UnsuccessfulExecutionException.class);
        return maybeException.map(UnsuccessfulExecutionException::getStatus);
    }

    @Override
    protected CompletableFuture<JobResult> unwrapJobResultException(
            final CompletableFuture<JobResult> jobResult) {
        return jobResult.thenApply(
                result -> {
                    if (result.isSuccess()) {
                        return result;
                    }

                    // TODO use user classloader
                    throw new CompletionException(
                            UnsuccessfulExecutionException.fromJobResult(
                                    result, Thread.currentThread().getContextClassLoader()));
                });
    }
}

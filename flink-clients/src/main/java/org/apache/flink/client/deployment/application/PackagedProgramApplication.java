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

package org.apache.flink.client.deployment.application;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.deployment.application.executors.EmbeddedExecutorServiceLoader;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.ClientOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.application.AbstractApplication;
import org.apache.flink.runtime.application.ApplicationID;
import org.apache.flink.runtime.client.DuplicateJobSubmissionException;
import org.apache.flink.runtime.client.UnsuccessfulExecutionException;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.apache.flink.runtime.application.ApplicationStatus.fromApplicationStatus;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class PackagedProgramApplication extends AbstractApplication {

    private static final Logger LOG = LoggerFactory.getLogger(PackagedProgramApplication.class);

    private static boolean isCanceledOrFailed(ApplicationStatus applicationStatus) {
        return applicationStatus == ApplicationStatus.CANCELED
                || applicationStatus == ApplicationStatus.FAILED;
    }

    private static final String FAILED_JOB_NAME = "(application driver)";

    private final PackagedProgram program;

    private final Collection<JobID> recoveredJobIds;

    private final Configuration configuration;

    private final boolean handleFatalError;

    private final boolean shouldShutDownOnFinish;

    private final boolean enforceSingleJobExecution;

    private final boolean submitFailedJobOnApplicationError;

    private transient CompletableFuture<Void> applicationCompletionFuture;

    private transient ScheduledFuture<?> applicationExecutionTask;

    private transient CompletableFuture<Acknowledge> finishApplicationFuture;

    public PackagedProgramApplication(
            final PackagedProgram program,
            final Configuration configuration,
            final boolean enforceSingleJobExecution) {
        this(
                new ApplicationID(),
                program,
                Collections.emptyList(),
                configuration,
                false,
                enforceSingleJobExecution,
                false,
                false);
    }

    public PackagedProgramApplication(
            final ApplicationID applicationId,
            final PackagedProgram program,
            final Collection<JobID> recoveredJobIds,
            final Configuration configuration,
            final boolean handleFatalError,
            final boolean enforceSingleJobExecution,
            final boolean submitFailedJobOnApplicationError,
            final boolean shouldShutDownOnFinish) {
        super(applicationId);
        this.program = checkNotNull(program);
        this.recoveredJobIds = checkNotNull(recoveredJobIds);
        this.configuration = checkNotNull(configuration);
        this.handleFatalError = handleFatalError;
        this.enforceSingleJobExecution = enforceSingleJobExecution;
        this.submitFailedJobOnApplicationError = submitFailedJobOnApplicationError;
        this.shouldShutDownOnFinish = shouldShutDownOnFinish;
    }

    @Override
    public CompletableFuture<Acknowledge> runAsync(
            final DispatcherGateway dispatcherGateway,
            final ScheduledExecutor scheduledExecutor,
            final FatalErrorHandler errorHandler,
            final Duration timeout) {
        final CompletableFuture<List<JobID>> applicationExecutionFuture = new CompletableFuture<>();
        final Set<JobID> tolerateMissingResult = Collections.synchronizedSet(new HashSet<>());

        // we need to hand in a future as return value because we need to get those JobIs out
        // from the scheduled task that executes the user program
        applicationExecutionTask =
                scheduledExecutor.schedule(
                        () ->
                                runApplicationEntryPoint(
                                        applicationExecutionFuture,
                                        tolerateMissingResult,
                                        dispatcherGateway,
                                        scheduledExecutor,
                                        enforceSingleJobExecution,
                                        submitFailedJobOnApplicationError),
                        0L,
                        TimeUnit.MILLISECONDS);

        applicationCompletionFuture =
                applicationExecutionFuture
                        .handle(
                                (jobIds, throwable) -> {
                                    if (throwable != null) {
                                        throw new CompletionException(throwable);
                                    }
                                    return getApplicationResult(
                                            dispatcherGateway,
                                            jobIds,
                                            tolerateMissingResult,
                                            scheduledExecutor,
                                            configuration.get(
                                                    DeploymentOptions
                                                            .TERMINATE_APPLICATION_ON_ANY_JOB_EXCEPTION),
                                            configuration.get(ClientOptions.CLIENT_TIMEOUT),
                                            configuration.get(ClientOptions.CLIENT_RETRY_PERIOD));
                                })
                        .thenCompose(Function.identity());

        finishApplicationFuture = finishApplication(dispatcherGateway, errorHandler);

        // not blocking
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public void cancel() throws Exception {
        if (applicationExecutionTask != null) {
            applicationExecutionTask.cancel(true);
        }

        if (applicationCompletionFuture != null) {
            applicationCompletionFuture.cancel(true);
        }
    }

    /**
     * Logs final application status and invokes error handler in case of unexpected failures.
     * Optionally shuts down the given dispatcherGateway when the application completes (either
     * successfully or in case of failure), depending on the corresponding config option.
     */
    private CompletableFuture<Acknowledge> finishApplication(
            final DispatcherGateway dispatcherGateway, final FatalErrorHandler errorHandler) {
        final CompletableFuture<Acknowledge> shutdownFuture =
                applicationCompletionFuture
                        .handle(
                                (ignored, t) -> {
                                    program.close();
                                    if (t == null) {
                                        LOG.info("Application completed SUCCESSFULLY");
                                        return finish(
                                                dispatcherGateway, ApplicationStatus.SUCCEEDED);
                                    }
                                    final Optional<ApplicationStatus> maybeApplicationStatus =
                                            extractApplicationStatus(t);
                                    if (maybeApplicationStatus.isPresent()
                                            && isCanceledOrFailed(maybeApplicationStatus.get())) {
                                        final ApplicationStatus applicationStatus =
                                                maybeApplicationStatus.get();
                                        LOG.info("Application {}: ", applicationStatus, t);
                                        return finish(dispatcherGateway, applicationStatus);
                                    }
                                    if (t instanceof CancellationException) {
                                        LOG.warn(
                                                "Application has been cancelled because the {} is being stopped.",
                                                PackagedProgramApplication.class.getSimpleName());
                                        setApplicationStatus(
                                                org.apache.flink.runtime.application
                                                        .ApplicationStatus.CANCELED);
                                        return CompletableFuture.completedFuture(Acknowledge.get());
                                    }
                                    LOG.warn("Application failed unexpectedly: ", t);
                                    setApplicationStatus(
                                            org.apache.flink.runtime.application.ApplicationStatus
                                                    .FAILED);
                                    return FutureUtils.<Acknowledge>completedExceptionally(t);
                                })
                        .thenCompose(Function.identity());
        FutureUtils.handleUncaughtException(
                shutdownFuture,
                (t, e) -> {
                    if (handleFatalError) {
                        errorHandler.onFatalError(e);
                    }
                });
        return shutdownFuture;
    }

    private CompletableFuture<Acknowledge> finish(
            DispatcherGateway dispatcherGateway, ApplicationStatus applicationStatus) {
        final ApplicationStatus applicationResult =
                configuration.get(DeploymentOptions.TERMINATE_APPLICATION_ON_ANY_JOB_EXCEPTION)
                        ? applicationStatus
                        : ApplicationStatus.SUCCEEDED;

        setApplicationStatus(fromApplicationStatus(applicationResult).orElseThrow());

        return shouldShutDownOnFinish
                ? dispatcherGateway.shutDownCluster(applicationStatus)
                : CompletableFuture.completedFuture(Acknowledge.get());
    }

    private Optional<ApplicationStatus> extractApplicationStatus(Throwable t) {
        final Optional<UnsuccessfulExecutionException> maybeException =
                ExceptionUtils.findThrowable(t, UnsuccessfulExecutionException.class);
        return maybeException.map(UnsuccessfulExecutionException::getStatus);
    }

    /**
     * Runs the user program entrypoint and completes the given {@code jobIdsFuture} with the {@link
     * JobID JobIDs} of the submitted jobs.
     *
     * <p>This should be executed in a separate thread (or task).
     */
    private void runApplicationEntryPoint(
            final CompletableFuture<List<JobID>> jobIdsFuture,
            final Set<JobID> tolerateMissingResult,
            final DispatcherGateway dispatcherGateway,
            final ScheduledExecutor scheduledExecutor,
            final boolean enforceSingleJobExecution,
            final boolean submitFailedJobOnApplicationError) {
        if (submitFailedJobOnApplicationError && !enforceSingleJobExecution) {
            jobIdsFuture.completeExceptionally(
                    new ApplicationExecutionException(
                            String.format(
                                    "Submission of failed job in case of an application error ('%s') is not supported in non-HA setups.",
                                    DeploymentOptions.SUBMIT_FAILED_JOB_ON_APPLICATION_ERROR
                                            .key())));
            return;
        }
        final List<JobID> applicationJobIds = new ArrayList<>(recoveredJobIds);
        try {
            final PipelineExecutorServiceLoader executorServiceLoader =
                    new EmbeddedExecutorServiceLoader(
                            applicationJobIds, dispatcherGateway, scheduledExecutor);

            setApplicationStatus(org.apache.flink.runtime.application.ApplicationStatus.RUNNING);
            ClientUtils.executeProgram(
                    executorServiceLoader,
                    configuration,
                    program,
                    enforceSingleJobExecution,
                    true /* suppress sysout */,
                    getApplicationId());

            if (applicationJobIds.isEmpty()) {
                jobIdsFuture.completeExceptionally(
                        new ApplicationExecutionException(
                                "The application contains no execute() calls."));
            } else {
                jobIdsFuture.complete(applicationJobIds);
            }
        } catch (Throwable t) {
            // If we're running in a single job execution mode, it's safe to consider re-submission
            // of an already finished a success.
            final Optional<DuplicateJobSubmissionException> maybeDuplicate =
                    ExceptionUtils.findThrowable(t, DuplicateJobSubmissionException.class);
            if (enforceSingleJobExecution
                    && maybeDuplicate.isPresent()
                    && maybeDuplicate.get().isGloballyTerminated()) {
                final JobID jobId = maybeDuplicate.get().getJobID();
                tolerateMissingResult.add(jobId);
                jobIdsFuture.complete(Collections.singletonList(jobId));
            } else if (submitFailedJobOnApplicationError && applicationJobIds.isEmpty()) {
                final JobID failedJobId =
                        JobID.fromHexString(
                                configuration.get(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID));
                dispatcherGateway
                        .submitFailedJob(failedJobId, FAILED_JOB_NAME, t)
                        .thenAccept(
                                ignored ->
                                        jobIdsFuture.complete(
                                                Collections.singletonList(failedJobId)));
            } else {
                jobIdsFuture.completeExceptionally(
                        new ApplicationExecutionException("Could not execute application.", t));
            }
        }
    }

    /**
     * If the given {@link JobResult} indicates success, this passes through the {@link JobResult}.
     * Otherwise, this returns a future that is finished exceptionally (potentially with an
     * exception from the {@link JobResult}).
     */
    @Override
    protected CompletableFuture<JobResult> unwrapJobResultException(
            final CompletableFuture<JobResult> jobResult) {
        return jobResult.thenApply(
                result -> {
                    if (result.isSuccess()) {
                        return result;
                    }

                    throw new CompletionException(
                            UnsuccessfulExecutionException.fromJobResult(
                                    result, program.getUserCodeClassLoader()));
                });
    }
}

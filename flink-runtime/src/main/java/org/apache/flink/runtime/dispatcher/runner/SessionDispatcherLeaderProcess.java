/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.application.AbstractApplication;
import org.apache.flink.runtime.application.ApplicationResult;
import org.apache.flink.runtime.application.ApplicationResultStore;
import org.apache.flink.runtime.application.ApplicationStore;
import org.apache.flink.runtime.application.ApplicationStoreEntry;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.client.DuplicateApplicationSubmissionException;
import org.apache.flink.runtime.client.DuplicateJobSubmissionException;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.jobmanager.ExecutionPlanStore;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.FunctionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * Process which encapsulates the job recovery logic and life cycle management of a {@link
 * Dispatcher}.
 */
public class SessionDispatcherLeaderProcess extends AbstractDispatcherLeaderProcess
        implements ExecutionPlanStore.ExecutionPlanListener, ApplicationStore.ApplicationListener {

    private final DispatcherGatewayServiceFactory dispatcherGatewayServiceFactory;

    private final ExecutionPlanStore executionPlanStore;

    private final JobResultStore jobResultStore;

    private final ApplicationStore applicationStore;

    private final ApplicationResultStore applicationResultStore;

    private final BlobServer blobServer;

    private final Executor ioExecutor;

    private CompletableFuture<Void> onGoingRecoveryOperation = FutureUtils.completedVoidFuture();

    private SessionDispatcherLeaderProcess(
            UUID leaderSessionId,
            DispatcherGatewayServiceFactory dispatcherGatewayServiceFactory,
            ExecutionPlanStore executionPlanStore,
            JobResultStore jobResultStore,
            ApplicationStore applicationStore,
            ApplicationResultStore applicationResultStore,
            BlobServer blobServer,
            Executor ioExecutor,
            FatalErrorHandler fatalErrorHandler) {
        super(leaderSessionId, fatalErrorHandler);

        this.dispatcherGatewayServiceFactory = dispatcherGatewayServiceFactory;
        this.executionPlanStore = executionPlanStore;
        this.jobResultStore = jobResultStore;
        this.applicationStore = applicationStore;
        this.applicationResultStore = applicationResultStore;
        this.blobServer = blobServer;
        this.ioExecutor = ioExecutor;
    }

    @Override
    protected void onStart() {
        startServices();

        onGoingRecoveryOperation =
                createDispatcherBasedOnRecoveredExecutionPlansAndRecoveredDirtyJobResults();
    }

    private void startServices() {
        try {
            executionPlanStore.start(this);
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Could not start %s when trying to start the %s.",
                            executionPlanStore.getClass().getSimpleName(),
                            getClass().getSimpleName()),
                    e);
        }

        try {
            applicationStore.start(this);
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Could not start %s when trying to start the %s.",
                            applicationStore.getClass().getSimpleName(),
                            getClass().getSimpleName()),
                    e);
        }
    }

    private void createDispatcherIfRunning(
            Collection<ExecutionPlan> executionPlans,
            Collection<JobResult> recoveredDirtyJobResults,
            Collection<AbstractApplication> recoveredApplications,
            Collection<ApplicationResult> recoveredDirtyApplicationResults) {
        runIfStateIs(
                State.RUNNING,
                () ->
                        createDispatcher(
                                executionPlans,
                                recoveredDirtyJobResults,
                                recoveredApplications,
                                recoveredDirtyApplicationResults));
    }

    private void createDispatcher(
            Collection<ExecutionPlan> executionPlans,
            Collection<JobResult> recoveredDirtyJobResults,
            Collection<AbstractApplication> recoveredApplications,
            Collection<ApplicationResult> recoveredDirtyApplicationResults) {

        final DispatcherGatewayService dispatcherService =
                dispatcherGatewayServiceFactory.create(
                        DispatcherId.fromUuid(getLeaderSessionId()),
                        executionPlans,
                        recoveredDirtyJobResults,
                        recoveredApplications,
                        recoveredDirtyApplicationResults,
                        executionPlanStore,
                        jobResultStore,
                        applicationStore,
                        applicationResultStore);

        completeDispatcherSetup(dispatcherService);
    }

    private CompletableFuture<Void>
            createDispatcherBasedOnRecoveredExecutionPlansAndRecoveredDirtyJobResults() {
        final CompletableFuture<Collection<JobResult>> dirtyJobResultsFuture =
                CompletableFuture.supplyAsync(this::getDirtyJobResultsIfRunning, ioExecutor);

        final CompletableFuture<Tuple2<Collection<ExecutionPlan>, Collection<JobID>>>
                recoveredJobsAndTerminatedJobIdsFuture =
                        dirtyJobResultsFuture.thenApplyAsync(
                                dirtyJobResults -> {
                                    Set<JobID> terminatedJobIds =
                                            dirtyJobResults.stream()
                                                    .map(JobResult::getJobId)
                                                    .collect(Collectors.toSet());
                                    return Tuple2.of(
                                            recoverJobsIfRunning(terminatedJobIds),
                                            terminatedJobIds);
                                },
                                ioExecutor);

        final CompletableFuture<Collection<ApplicationResult>> dirtyApplicationResultsFuture =
                CompletableFuture.supplyAsync(
                        this::getDirtyApplicationResultsIfRunning, ioExecutor);

        final CompletableFuture<Collection<AbstractApplication>> recoveredApplicationsFuture =
                dirtyApplicationResultsFuture.thenCombineAsync(
                        recoveredJobsAndTerminatedJobIdsFuture,
                        (dirtyApplicationResults, recoveredJobsAndTerminatedJobIds) -> {
                            Set<JobID> recoveredJobIds =
                                    recoveredJobsAndTerminatedJobIds.f0.stream()
                                            .map(ExecutionPlan::getJobID)
                                            .collect(Collectors.toSet());
                            return recoverApplicationsIfRunning(
                                    dirtyApplicationResults.stream()
                                            .map(ApplicationResult::getApplicationId)
                                            .collect(Collectors.toSet()),
                                    recoveredJobIds,
                                    recoveredJobsAndTerminatedJobIds.f1);
                        },
                        ioExecutor);

        return CompletableFuture.allOf(
                        dirtyJobResultsFuture,
                        recoveredJobsAndTerminatedJobIdsFuture,
                        dirtyApplicationResultsFuture,
                        recoveredApplicationsFuture)
                .thenRun(
                        () ->
                                createDispatcherIfRunning(
                                        recoveredJobsAndTerminatedJobIdsFuture.join().f0,
                                        dirtyJobResultsFuture.join(),
                                        recoveredApplicationsFuture.join(),
                                        dirtyApplicationResultsFuture.join()))
                .handle(this::onErrorIfRunning);
    }

    private Collection<ExecutionPlan> recoverJobsIfRunning(Set<JobID> recoveredDirtyJobResults) {
        return supplyUnsynchronizedIfRunning(() -> recoverJobs(recoveredDirtyJobResults))
                .orElse(Collections.emptyList());
    }

    private Collection<ExecutionPlan> recoverJobs(Set<JobID> recoveredDirtyJobResults) {
        log.info("Recover all persisted job graphs that are not finished, yet.");
        final Collection<JobID> jobIds = getJobIds();
        final Collection<ExecutionPlan> recoveredExecutionPlans = new ArrayList<>();

        for (JobID jobId : jobIds) {
            if (!recoveredDirtyJobResults.contains(jobId)) {
                tryRecoverJob(jobId).ifPresent(recoveredExecutionPlans::add);
            } else {
                log.info(
                        "Skipping recovery of a job with job id {}, because it already reached a globally terminal state",
                        jobId);
            }
        }

        log.info("Successfully recovered {} persisted job graphs.", recoveredExecutionPlans.size());

        return recoveredExecutionPlans;
    }

    private Collection<JobID> getJobIds() {
        try {
            return executionPlanStore.getJobIds();
        } catch (Exception e) {
            throw new FlinkRuntimeException("Could not retrieve job ids of persisted jobs.", e);
        }
    }

    private Optional<ExecutionPlan> tryRecoverJob(JobID jobId) {
        log.info("Trying to recover job with job id {}.", jobId);
        try {
            final ExecutionPlan executionPlan = executionPlanStore.recoverExecutionPlan(jobId);
            if (executionPlan == null) {
                log.info(
                        "Skipping recovery of job with job id {}, because it already finished in a previous execution",
                        jobId);
            }
            return Optional.ofNullable(executionPlan);
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format("Could not recover job with job id %s.", jobId), e);
        }
    }

    private Collection<JobResult> getDirtyJobResultsIfRunning() {
        return supplyUnsynchronizedIfRunning(this::getDirtyJobResults)
                .orElse(Collections.emptyList());
    }

    private Collection<JobResult> getDirtyJobResults() {
        try {
            return jobResultStore.getDirtyResults();
        } catch (IOException e) {
            throw new FlinkRuntimeException(
                    "Could not retrieve JobResults of globally-terminated jobs from JobResultStore",
                    e);
        }
    }

    private Collection<ApplicationResult> getDirtyApplicationResultsIfRunning() {
        return supplyUnsynchronizedIfRunning(this::getDirtyApplicationResults)
                .orElse(Collections.emptyList());
    }

    private Collection<ApplicationResult> getDirtyApplicationResults() {
        try {
            return applicationResultStore.getDirtyResults();
        } catch (IOException e) {
            throw new FlinkRuntimeException(
                    "Could not retrieve ApplicationResults from ApplicationResultStore", e);
        }
    }

    private Collection<AbstractApplication> recoverApplicationsIfRunning(
            Set<ApplicationID> recoveredDirtyApplicationResults,
            Collection<JobID> recoveredJobIds,
            Collection<JobID> terminatedJobIds) {
        return supplyUnsynchronizedIfRunning(
                        () ->
                                recoverApplications(
                                        recoveredDirtyApplicationResults,
                                        recoveredJobIds,
                                        terminatedJobIds))
                .orElse(Collections.emptyList());
    }

    private Collection<AbstractApplication> recoverApplications(
            Set<ApplicationID> recoveredDirtyApplicationResults,
            Collection<JobID> recoveredJobIds,
            Collection<JobID> terminatedJobIds) {
        log.info("Recover all persisted applications that are not finished, yet.");
        final Collection<ApplicationID> applicationIds = getApplicationIds();
        final Collection<AbstractApplication> recoveredApplications = new ArrayList<>();

        for (ApplicationID applicationId : applicationIds) {
            if (!recoveredDirtyApplicationResults.contains(applicationId)) {
                tryRecoverApplication(applicationId, recoveredJobIds, terminatedJobIds)
                        .ifPresent(recoveredApplications::add);
            } else {
                log.info(
                        "Skipping recovery of an application with id {}, because it already reached a globally terminal state",
                        applicationId);
            }
        }

        log.info("Successfully recovered {} persisted applications.", recoveredApplications.size());

        return recoveredApplications;
    }

    private Collection<ApplicationID> getApplicationIds() {
        try {
            return applicationStore.getApplicationIds();
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    "Could not retrieve application ids of persisted applications.", e);
        }
    }

    private Optional<AbstractApplication> tryRecoverApplication(
            ApplicationID applicationId,
            Collection<JobID> recoveredJobIds,
            Collection<JobID> terminatedJobIds) {
        log.info("Trying to recover application with id {}.", applicationId);
        try {
            final ApplicationStoreEntry applicationStoreEntry =
                    applicationStore.recoverApplication(applicationId);
            if (applicationStoreEntry == null) {
                log.info(
                        "Skipping recovery of application with id {}, because it already finished in a previous execution",
                        applicationId);
                return Optional.empty();
            }

            return Optional.ofNullable(
                    applicationStoreEntry.getApplication(
                            blobServer, recoveredJobIds, terminatedJobIds));
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format("Could not recover application with id %s.", applicationId), e);
        }
    }

    @Override
    protected CompletableFuture<Void> onClose() {
        return CompletableFuture.runAsync(this::stopServices, ioExecutor);
    }

    private void stopServices() {
        try {
            executionPlanStore.stop();
            applicationStore.stop();
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }
    }

    // ------------------------------------------------------------
    // ExecutionPlanListener
    // ------------------------------------------------------------

    @Override
    public void onAddedExecutionPlan(JobID jobId) {
        runIfStateIs(State.RUNNING, () -> handleAddedExecutionPlan(jobId));
    }

    private void handleAddedExecutionPlan(JobID jobId) {
        log.debug(
                "Job {} has been added to the {} by another process.",
                jobId,
                executionPlanStore.getClass().getSimpleName());

        // serialize all ongoing recovery operations
        onGoingRecoveryOperation =
                onGoingRecoveryOperation
                        .thenApplyAsync(ignored -> recoverJobIfRunning(jobId), ioExecutor)
                        .thenCompose(
                                optionalExecutionPlan ->
                                        optionalExecutionPlan
                                                .flatMap(this::submitAddedJobIfRunning)
                                                .orElse(FutureUtils.completedVoidFuture()))
                        .handle(this::onErrorIfRunning);
    }

    private Optional<CompletableFuture<Void>> submitAddedJobIfRunning(ExecutionPlan executionPlan) {
        return supplyIfRunning(() -> submitAddedJob(executionPlan));
    }

    private CompletableFuture<Void> submitAddedJob(ExecutionPlan executionPlan) {
        final DispatcherGateway dispatcherGateway = getDispatcherGatewayInternal();

        return dispatcherGateway
                .submitJob(executionPlan, RpcUtils.INF_TIMEOUT)
                .thenApply(FunctionUtils.nullFn())
                .exceptionally(this::filterOutDuplicateJobSubmissionException);
    }

    private Void filterOutDuplicateJobSubmissionException(Throwable throwable) {
        final Throwable strippedException = ExceptionUtils.stripCompletionException(throwable);
        if (strippedException instanceof DuplicateJobSubmissionException) {
            final DuplicateJobSubmissionException duplicateJobSubmissionException =
                    (DuplicateJobSubmissionException) strippedException;

            log.debug(
                    "Ignore recovered job {} because the job is currently being executed.",
                    duplicateJobSubmissionException.getJobID(),
                    duplicateJobSubmissionException);

            return null;
        } else {
            throw new CompletionException(throwable);
        }
    }

    private DispatcherGateway getDispatcherGatewayInternal() {
        return Preconditions.checkNotNull(getDispatcherGateway().getNow(null));
    }

    private Optional<ExecutionPlan> recoverJobIfRunning(JobID jobId) {
        return supplyUnsynchronizedIfRunning(() -> tryRecoverJob(jobId)).flatMap(x -> x);
    }

    @Override
    public void onRemovedExecutionPlan(JobID jobId) {
        runIfStateIs(State.RUNNING, () -> handleRemovedExecutionPlan(jobId));
    }

    private void handleRemovedExecutionPlan(JobID jobId) {
        log.debug(
                "Job {} has been removed from the {} by another process.",
                jobId,
                executionPlanStore.getClass().getSimpleName());

        onGoingRecoveryOperation =
                onGoingRecoveryOperation
                        .thenCompose(
                                ignored ->
                                        removeExecutionPlanIfRunning(jobId)
                                                .orElse(FutureUtils.completedVoidFuture()))
                        .handle(this::onErrorIfRunning);
    }

    private Optional<CompletableFuture<Void>> removeExecutionPlanIfRunning(JobID jobId) {
        return supplyIfRunning(() -> removeExecutionPlan(jobId));
    }

    private CompletableFuture<Void> removeExecutionPlan(JobID jobId) {
        return getDispatcherService()
                .map(dispatcherService -> dispatcherService.onRemovedExecutionPlan(jobId))
                .orElseGet(FutureUtils::completedVoidFuture);
    }

    // ------------------------------------------------------------
    // ApplicationListener
    // ------------------------------------------------------------

    @Override
    public void onAddedApplication(ApplicationID applicationId) {
        runIfStateIs(State.RUNNING, () -> handleAddedApplication(applicationId));
    }

    private void handleAddedApplication(ApplicationID applicationId) {
        log.debug(
                "Application {} has been added to the {} by another process.",
                applicationId,
                applicationStore.getClass().getSimpleName());

        // serialize all ongoing recovery operations
        onGoingRecoveryOperation =
                onGoingRecoveryOperation
                        .thenApplyAsync(
                                ignored -> recoverApplicationIfRunning(applicationId), ioExecutor)
                        .thenCompose(
                                optionalApplication ->
                                        optionalApplication
                                                .flatMap(this::submitAddedApplicationIfRunning)
                                                .orElse(FutureUtils.completedVoidFuture()))
                        .handle(this::onErrorIfRunning);
    }

    private Optional<CompletableFuture<Void>> submitAddedApplicationIfRunning(
            AbstractApplication application) {
        return supplyIfRunning(() -> submitAddedApplication(application));
    }

    private CompletableFuture<Void> submitAddedApplication(AbstractApplication application) {
        final DispatcherGateway dispatcherGateway = getDispatcherGatewayInternal();

        return dispatcherGateway
                .submitApplication(application, RpcUtils.INF_TIMEOUT)
                .thenApply(FunctionUtils.nullFn())
                .exceptionally(this::filterOutDuplicateApplicationSubmissionException);
    }

    private Void filterOutDuplicateApplicationSubmissionException(Throwable throwable) {
        final Throwable strippedException = ExceptionUtils.stripCompletionException(throwable);
        if (strippedException instanceof DuplicateApplicationSubmissionException) {
            final DuplicateApplicationSubmissionException duplicateApplicationSubmissionException =
                    (DuplicateApplicationSubmissionException) strippedException;

            log.debug(
                    "Ignore recovered application {} because the application is currently being executed.",
                    duplicateApplicationSubmissionException.getApplicationId(),
                    duplicateApplicationSubmissionException);

            return null;
        } else {
            throw new CompletionException(throwable);
        }
    }

    private Optional<AbstractApplication> recoverApplicationIfRunning(ApplicationID applicationId) {
        return supplyUnsynchronizedIfRunning(
                        () ->
                                tryRecoverApplication(
                                        applicationId,
                                        Collections.emptyList(),
                                        Collections.emptyList()))
                .flatMap(x -> x);
    }

    @Override
    public void onRemovedApplication(ApplicationID applicationId) {
        runIfStateIs(State.RUNNING, () -> handleRemovedApplication(applicationId));
    }

    private void handleRemovedApplication(ApplicationID applicationId) {
        log.debug(
                "Application {} has been removed from the {} by another process.",
                applicationId,
                applicationStore.getClass().getSimpleName());

        onGoingRecoveryOperation =
                onGoingRecoveryOperation
                        .thenCompose(
                                ignored ->
                                        removeApplicationIfRunning(applicationId)
                                                .orElse(FutureUtils.completedVoidFuture()))
                        .handle(this::onErrorIfRunning);
    }

    private Optional<CompletableFuture<Void>> removeApplicationIfRunning(
            ApplicationID applicationId) {
        return supplyIfRunning(() -> removeApplication(applicationId));
    }

    private CompletableFuture<Void> removeApplication(ApplicationID applicationId) {
        return getDispatcherService()
                .map(dispatcherService -> dispatcherService.onRemovedApplication(applicationId))
                .orElseGet(FutureUtils::completedVoidFuture);
    }

    // ---------------------------------------------------------------
    // Factory methods
    // ---------------------------------------------------------------

    public static SessionDispatcherLeaderProcess create(
            UUID leaderSessionId,
            DispatcherGatewayServiceFactory dispatcherFactory,
            ExecutionPlanStore executionPlanStore,
            JobResultStore jobResultStore,
            ApplicationStore applicationStore,
            ApplicationResultStore applicationResultStore,
            BlobServer blobServer,
            Executor ioExecutor,
            FatalErrorHandler fatalErrorHandler) {
        return new SessionDispatcherLeaderProcess(
                leaderSessionId,
                dispatcherFactory,
                executionPlanStore,
                jobResultStore,
                applicationStore,
                applicationResultStore,
                blobServer,
                ioExecutor,
                fatalErrorHandler);
    }
}

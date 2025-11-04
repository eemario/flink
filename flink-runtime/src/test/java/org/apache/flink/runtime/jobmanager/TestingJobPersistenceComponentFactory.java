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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.runtime.application.ApplicationResultStore;
import org.apache.flink.runtime.application.ApplicationStore;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.util.Preconditions;

/**
 * {@code TestingJobPersistenceComponentFactory} implements {@link JobPersistenceComponentFactory}
 * for a given {@link ExecutionPlanStore} and {@link JobResultStore}.
 */
public class TestingJobPersistenceComponentFactory implements JobPersistenceComponentFactory {

    private final ExecutionPlanStore executionPlanStore;
    private final JobResultStore jobResultStore;
    private final ApplicationStore applicationStore;
    private final ApplicationResultStore applicationResultStore;

    public TestingJobPersistenceComponentFactory(
            ExecutionPlanStore executionPlanStore,
            JobResultStore jobResultStore,
            ApplicationStore applicationStore,
            ApplicationResultStore applicationResultStore) {
        this.executionPlanStore = Preconditions.checkNotNull(executionPlanStore);
        this.jobResultStore = Preconditions.checkNotNull(jobResultStore);
        this.applicationStore = Preconditions.checkNotNull(applicationStore);
        this.applicationResultStore = Preconditions.checkNotNull(applicationResultStore);
    }

    public TestingJobPersistenceComponentFactory(
            ExecutionPlanStore executionPlanStore, JobResultStore jobResultStore) {
        this(executionPlanStore, jobResultStore, null, null);
    }

    @Override
    public ExecutionPlanStore createExecutionPlanStore() {
        return executionPlanStore;
    }

    @Override
    public JobResultStore createJobResultStore() {
        return jobResultStore;
    }

    @Override
    public ApplicationStore createApplicationStore() {
        return applicationStore;
    }

    @Override
    public ApplicationResultStore createApplicationResultStore() {
        return applicationResultStore;
    }

    @Override
    public BlobServer createBlobServer() {
        return null;
    }
}

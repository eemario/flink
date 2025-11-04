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
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blob.PermanentBlobService;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link EmbeddedApplicationResultStore}. */
class EmbeddedApplicationResultStoreTest {

    private EmbeddedApplicationResultStore embeddedApplicationResultStore;

    private Executor ioExecutor;

    @BeforeEach
    void setUp() {
        ioExecutor = Executors.directExecutor();
        embeddedApplicationResultStore = new EmbeddedApplicationResultStore();
    }

    @Test
    void testCreateDirtyResultAndMarkAsClean() throws Exception {
        final ApplicationID applicationId = ApplicationID.generate();
        final AbstractApplication application = new TestingApplication(applicationId);
        final ApplicationResult applicationResult =
                new ApplicationResult(applicationId, ApplicationState.FINISHED);
        final ApplicationResultEntry applicationResultEntry =
                new ApplicationResultEntry(applicationResult);

        // Create dirty result
        embeddedApplicationResultStore.createDirtyResultAsync(applicationResultEntry).get();

        // Verify it's dirty
        assertThat(
                        embeddedApplicationResultStore
                                .hasDirtyApplicationResultEntryAsync(applicationId)
                                .get())
                .isTrue();
        assertThat(
                        embeddedApplicationResultStore
                                .hasCleanApplicationResultEntryAsync(applicationId)
                                .get())
                .isFalse();
        assertThat(embeddedApplicationResultStore.getDirtyResultCount()).isEqualTo(1);
        assertThat(embeddedApplicationResultStore.getCleanResultCount()).isEqualTo(0);

        // Mark as clean
        embeddedApplicationResultStore.markResultAsCleanAsync(applicationId).get();

        // Verify it's clean
        assertThat(
                        embeddedApplicationResultStore
                                .hasDirtyApplicationResultEntryAsync(applicationId)
                                .get())
                .isFalse();
        assertThat(
                        embeddedApplicationResultStore
                                .hasCleanApplicationResultEntryAsync(applicationId)
                                .get())
                .isTrue();
        assertThat(embeddedApplicationResultStore.getDirtyResultCount()).isEqualTo(0);
        assertThat(embeddedApplicationResultStore.getCleanResultCount()).isEqualTo(1);
    }

    @Test
    void testGetDirtyResults() throws Exception {
        final ApplicationID applicationId1 = ApplicationID.generate();
        final ApplicationID applicationId2 = ApplicationID.generate();

        final AbstractApplication application1 = new TestingApplication(applicationId1);
        final AbstractApplication application2 = new TestingApplication(applicationId2);

        final ApplicationResult applicationResult1 =
                new ApplicationResult(applicationId1, ApplicationState.FINISHED);
        final ApplicationResult applicationResult2 =
                new ApplicationResult(applicationId2, ApplicationState.FAILED);

        // Create dirty results
        embeddedApplicationResultStore
                .createDirtyResultAsync(new ApplicationResultEntry(applicationResult1))
                .get();
        embeddedApplicationResultStore
                .createDirtyResultAsync(new ApplicationResultEntry(applicationResult2))
                .get();

        // Get dirty results
        final Set<ApplicationResult> dirtyResults =
                embeddedApplicationResultStore.getDirtyResults();

        assertThat(dirtyResults).hasSize(2);
        assertThat(dirtyResults)
                .extracting(ApplicationResult::getApplicationId)
                .containsExactlyInAnyOrder(applicationId1, applicationId2);
    }

    @Test
    void testMarkNonExistentResultAsClean() {
        final ApplicationID applicationId = ApplicationID.generate();

        assertThatThrownBy(
                        () ->
                                embeddedApplicationResultStore
                                        .markResultAsCleanAsync(applicationId)
                                        .get())
                .hasCauseInstanceOf(java.util.NoSuchElementException.class);
    }

    @Test
    void testCreateDuplicateDirtyResult() throws Exception {
        final ApplicationID applicationId = ApplicationID.generate();
        final AbstractApplication application = new TestingApplication(applicationId);
        final ApplicationResult applicationResult =
                new ApplicationResult(applicationId, ApplicationState.FINISHED);
        final ApplicationResultEntry applicationResultEntry =
                new ApplicationResultEntry(applicationResult);

        // Create dirty result
        embeddedApplicationResultStore.createDirtyResultAsync(applicationResultEntry).get();

        // Try to create another dirty result with the same ID
        assertThatThrownBy(
                        () ->
                                embeddedApplicationResultStore
                                        .createDirtyResultAsync(applicationResultEntry)
                                        .get())
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    private static class TestingApplication extends AbstractApplication
            implements ApplicationStoreEntry {
        protected TestingApplication(ApplicationID applicationId) {
            super(applicationId);
        }

        @Override
        public java.util.concurrent.CompletableFuture<org.apache.flink.runtime.messages.Acknowledge>
                execute(
                        org.apache.flink.runtime.dispatcher.DispatcherGateway dispatcherGateway,
                        org.apache.flink.util.concurrent.ScheduledExecutor scheduledExecutor,
                        java.util.concurrent.Executor mainThreadExecutor,
                        org.apache.flink.runtime.rpc.FatalErrorHandler errorHandler) {
            return null;
        }

        @Override
        public void cancel() {}

        @Override
        public void dispose() {}

        @Override
        public String getName() {
            return "TestingApplication";
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
    }
}

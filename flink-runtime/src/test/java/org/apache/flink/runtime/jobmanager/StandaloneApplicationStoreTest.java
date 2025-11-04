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

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.runtime.application.ApplicationStore;
import org.apache.flink.runtime.application.ApplicationStoreEntry;

import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link StandaloneApplicationStore}. */
class StandaloneApplicationStoreTest {

    @Test
    void testGetApplicationIdsReturnsEmptyCollection() throws Exception {
        StandaloneApplicationStore store = new StandaloneApplicationStore();
        store.start(new NoOpApplicationListener());

        Collection<ApplicationID> applicationIds = store.getApplicationIds();
        assertThat(applicationIds).isEmpty();
    }

    @Test
    void testRecoverApplicationReturnsNull() throws Exception {
        StandaloneApplicationStore store = new StandaloneApplicationStore();
        store.start(new NoOpApplicationListener());

        ApplicationID applicationId = new ApplicationID();
        ApplicationStoreEntry application = store.recoverApplication(applicationId);
        assertThat(application).isNull();
    }

    private static class NoOpApplicationListener implements ApplicationStore.ApplicationListener {
        @Override
        public void onAddedApplication(ApplicationID applicationId) {
            // Nothing to do
        }

        @Override
        public void onRemovedApplication(ApplicationID applicationId) {
            // Nothing to do
        }
    }
}

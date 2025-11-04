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

import javax.annotation.Nullable;

import java.util.Collection;

/** {@link ApplicationStoreEntry} instances for recovery. */
public interface ApplicationStore extends ApplicationWriter {

    /** Starts the {@link ApplicationStore} service. */
    void start(ApplicationListener applicationListener) throws Exception;

    /** Stops the {@link ApplicationStore} service. */
    void stop() throws Exception;

    /**
     * Returns the {@link ApplicationStoreEntry} with the given {@link ApplicationID} or {@code
     * null} if no application was registered.
     */
    @Nullable
    ApplicationStoreEntry recoverApplication(ApplicationID applicationId) throws Exception;

    /**
     * Get all application ids of submitted applications to the submitted application store.
     *
     * @return Collection of submitted application ids
     * @throws Exception if the operation fails
     */
    Collection<ApplicationID> getApplicationIds() throws Exception;

    /**
     * A listener for {@link ApplicationStoreEntry} instances. This is used to react to races
     * between multiple running {@link ApplicationStore} instances (on multiple job managers).
     */
    interface ApplicationListener {

        /**
         * Callback for {@link ApplicationStoreEntry} instances added by a different {@link
         * ApplicationStore} instance.
         *
         * <p><strong>Important:</strong> It is possible to get false positives and be notified
         * about an application, which was added by this instance.
         *
         * @param applicationId The {@link ApplicationID} of the added application
         */
        void onAddedApplication(ApplicationID applicationId);

        /**
         * Callback for {@link ApplicationStoreEntry} instances removed by a different {@link
         * ApplicationStore} instance.
         *
         * @param applicationId The {@link ApplicationID} of the removed application
         */
        void onRemovedApplication(ApplicationID applicationId);
    }
}

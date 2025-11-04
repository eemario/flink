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
import org.apache.flink.util.concurrent.Executors;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** An embedded implementation of {@link ApplicationResultStore} for testing purposes. */
public class EmbeddedApplicationResultStore extends AbstractThreadsafeApplicationResultStore {

    private final Map<ApplicationID, ApplicationResultEntry> dirtyResults =
            new ConcurrentHashMap<>();
    private final Map<ApplicationID, ApplicationResultEntry> cleanResults =
            new ConcurrentHashMap<>();

    public EmbeddedApplicationResultStore() {
        super(Executors.directExecutor());
    }

    @Override
    protected void createDirtyResultInternal(ApplicationResultEntry applicationResultEntry)
            throws IOException {
        final ApplicationID applicationId = applicationResultEntry.getApplicationId();
        if (dirtyResults.containsKey(applicationId) || cleanResults.containsKey(applicationId)) {
            throw new IllegalStateException(
                    "Application result store already contains an entry for application "
                            + applicationId);
        }
        dirtyResults.put(applicationId, applicationResultEntry);
    }

    @Override
    protected void markResultAsCleanInternal(ApplicationID applicationId)
            throws IOException, NoSuchElementException {
        final ApplicationResultEntry entry = dirtyResults.remove(applicationId);
        if (entry == null) {
            if (cleanResults.containsKey(applicationId)) {
                // Already clean, nothing to do
                return;
            }
            throw new NoSuchElementException(
                    "Could not mark application "
                            + applicationId
                            + " as clean as it is not present in the store.");
        }
        cleanResults.put(applicationId, entry);
    }

    @Override
    protected boolean hasDirtyApplicationResultEntryInternal(ApplicationID applicationId)
            throws IOException {
        return dirtyResults.containsKey(applicationId);
    }

    @Override
    protected boolean hasCleanApplicationResultEntryInternal(ApplicationID applicationId)
            throws IOException {
        return cleanResults.containsKey(applicationId);
    }

    @Override
    protected Set<ApplicationResult> getDirtyResultsInternal() throws IOException {
        Set<ApplicationResult> results = new HashSet<>();
        for (ApplicationResultEntry entry : dirtyResults.values()) {
            if (entry.getApplicationResult() != null) {
                results.add(entry.getApplicationResult());
            }
        }
        return results;
    }

    /** Clears all stored results. */
    public void clear() {
        dirtyResults.clear();
        cleanResults.clear();
    }

    /** Gets the number of dirty results. */
    public int getDirtyResultCount() {
        return dirtyResults.size();
    }

    /** Gets the number of clean results. */
    public int getCleanResultCount() {
        return cleanResults.size();
    }
}

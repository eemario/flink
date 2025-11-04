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
import org.apache.flink.runtime.application.ApplicationStoreWatcher;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.flink.shaded.curator5.org.apache.curator.utils.ZKPaths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@link ApplicationStoreWatcher} implementation for ZooKeeper. */
public class ZooKeeperApplicationStoreWatcher implements ApplicationStoreWatcher {

    private static final Logger LOG =
            LoggerFactory.getLogger(ZooKeeperApplicationStoreWatcher.class);

    /**
     * Cache to monitor all children. This is used to detect races with other instances working on
     * the same state.
     */
    private final PathChildrenCache pathCache;

    private ApplicationStore.ApplicationListener applicationListener;

    private volatile boolean running;

    public ZooKeeperApplicationStoreWatcher(PathChildrenCache pathCache) {
        this.pathCache = checkNotNull(pathCache);
        this.pathCache.getListenable().addListener(new ApplicationsPathCacheListener());
        running = false;
    }

    @Override
    public void start(ApplicationStore.ApplicationListener applicationListener) throws Exception {
        this.applicationListener = checkNotNull(applicationListener);
        running = true;
        pathCache.start();
    }

    @Override
    public void stop() throws Exception {
        if (!running) {
            return;
        }
        running = false;

        LOG.info("Stopping ZooKeeperApplicationStoreWatcher ");
        pathCache.close();
    }

    /**
     * Monitors ZooKeeper for changes.
     *
     * <p>Detects modifications from other job managers in corner situations. The event
     * notifications fire for changes from this job manager as well.
     */
    private final class ApplicationsPathCacheListener implements PathChildrenCacheListener {

        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {

            if (LOG.isDebugEnabled()) {
                if (event.getData() != null) {
                    LOG.debug(
                            "Received {} event (path: {})",
                            event.getType(),
                            event.getData().getPath());
                } else {
                    LOG.debug("Received {} event", event.getType());
                }
            }

            switch (event.getType()) {
                case CHILD_ADDED:
                    {
                        ApplicationID applicationId = fromEvent(event);

                        LOG.debug(
                                "Received CHILD_ADDED event notification for application {}",
                                applicationId);

                        applicationListener.onAddedApplication(applicationId);
                    }
                    break;

                case CHILD_UPDATED:
                    {
                        // Nothing to do
                    }
                    break;

                case CHILD_REMOVED:
                    {
                        ApplicationID applicationId = fromEvent(event);

                        LOG.debug(
                                "Received CHILD_REMOVED event notification for application {}",
                                applicationId);

                        applicationListener.onRemovedApplication(applicationId);
                    }
                    break;

                case CONNECTION_SUSPENDED:
                    {
                        LOG.warn(
                                "ZooKeeper connection SUSPENDING. Changes to the submitted "
                                        + "applications are not monitored (temporarily).");
                    }
                    break;

                case CONNECTION_LOST:
                    {
                        LOG.warn(
                                "ZooKeeper connection LOST. Changes to the submitted applications "
                                        + "are not monitored (permanently).");
                    }
                    break;

                case CONNECTION_RECONNECTED:
                    {
                        LOG.info(
                                "ZooKeeper connection RECONNECTED. Changes to the submitted "
                                        + "applications are monitored again.");
                    }
                    break;

                case INITIALIZED:
                    {
                        LOG.info("ApplicationsPathCacheListener initialized");
                    }
                    break;
            }
        }

        /** Returns a ApplicationID for the event's path. */
        private ApplicationID fromEvent(PathChildrenCacheEvent event) {
            return ApplicationID.fromHexString(ZKPaths.getNodeFromPath(event.getData().getPath()));
        }
    }
}

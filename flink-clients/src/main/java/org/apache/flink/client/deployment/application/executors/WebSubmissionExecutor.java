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

package org.apache.flink.client.deployment.application.executors;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.runtime.application.SingleJobApplication;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.client.ClientUtils;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The {@link PipelineExecutor} to be used when executing a job from web submission. */
public class WebSubmissionExecutor extends EmbeddedExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(WebSubmissionExecutor.class);

    public WebSubmissionExecutor(
            Collection<JobID> submittedJobIds,
            DispatcherGateway dispatcherGateway,
            Configuration configuration,
            EmbeddedJobClientCreator jobClientCreator) {
        super(submittedJobIds, dispatcherGateway, configuration, jobClientCreator);
    }

    @Override
    protected CompletableFuture<JobID> submitJob(
            final Configuration configuration,
            final DispatcherGateway dispatcherGateway,
            final StreamGraph streamGraph,
            final Duration rpcTimeout) {
        checkNotNull(streamGraph);

        LOG.info("Submitting Job with JobId={}.", streamGraph.getJobID());

        return dispatcherGateway
                .getBlobServerPort(rpcTimeout)
                .thenApply(
                        blobServerPort ->
                                new InetSocketAddress(
                                        dispatcherGateway.getHostname(), blobServerPort))
                .thenCompose(
                        blobServerAddress -> {
                            try {
                                ClientUtils.extractAndUploadExecutionPlanFiles(
                                        streamGraph,
                                        () -> new BlobClient(blobServerAddress, configuration));
                                streamGraph.serializeUserDefinedInstances();
                            } catch (Exception e) {
                                throw new CompletionException(e);
                            }

                            streamGraph.setApplicationId(
                                    ApplicationID.fromHexString(
                                            streamGraph.getJobID().toHexString()));
                            SingleJobApplication application =
                                    new SingleJobApplication(streamGraph, rpcTimeout);
                            return dispatcherGateway.submitApplication(application, rpcTimeout);
                        })
                .thenApply(ack -> streamGraph.getJobID());
    }
}

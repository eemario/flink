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

import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/** Application for testing. */
public class TestingApplication extends AbstractApplication {
    private final Function<RunAsyncParams, CompletableFuture<Acknowledge>> runAsyncFunction;

    public TestingApplication(
            Function<RunAsyncParams, CompletableFuture<Acknowledge>> runAsyncFunction) {
        super(new ApplicationID());
        this.runAsyncFunction = runAsyncFunction;
    }

    @Override
    public CompletableFuture<Acknowledge> runAsync(
            DispatcherGateway dispatcherGateway,
            ScheduledExecutor scheduledExecutor,
            FatalErrorHandler errorHandler,
            Duration timeout) {

        RunAsyncParams params =
                new RunAsyncParams(dispatcherGateway, scheduledExecutor, errorHandler, timeout);
        return runAsyncFunction.apply(params);
    }

    @Override
    public void cancel() throws Exception {}

    @Override
    protected CompletableFuture<JobResult> unwrapJobResultException(
            CompletableFuture<JobResult> jobResult) {
        return null;
    }

    public static class RunAsyncParams {
        public final DispatcherGateway dispatcherGateway;
        public final ScheduledExecutor scheduledExecutor;
        public final FatalErrorHandler errorHandler;
        public final Duration timeout;

        public RunAsyncParams(
                DispatcherGateway dispatcherGateway,
                ScheduledExecutor scheduledExecutor,
                FatalErrorHandler errorHandler,
                Duration timeout) {
            this.dispatcherGateway = dispatcherGateway;
            this.scheduledExecutor = scheduledExecutor;
            this.errorHandler = errorHandler;
            this.timeout = timeout;
        }
    }
}

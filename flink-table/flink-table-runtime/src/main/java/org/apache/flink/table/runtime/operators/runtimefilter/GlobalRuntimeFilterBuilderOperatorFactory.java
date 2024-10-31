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

package org.apache.flink.table.runtime.operators.runtimefilter;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The factory class for {@link GlobalRuntimeFilterBuilderOperator}. */
public class GlobalRuntimeFilterBuilderOperatorFactory
        extends AbstractStreamOperatorFactory<RowData>
        implements CoordinatedOperatorFactory<RowData> {
    private final Set<String> runtimeFilteringDataListenerIDs = new HashSet<>();
    private final int estimatedRowCount;
    private final int maxRowCount;
    private final int maxInFilterRowCount;
    private final RowType filterRowType;

    public GlobalRuntimeFilterBuilderOperatorFactory(
            int estimatedRowCount,
            int maxRowCount,
            int maxInFilterRowCount,
            RowType filterRowType) {
        this.estimatedRowCount = estimatedRowCount;
        this.maxRowCount = maxRowCount;
        this.maxInFilterRowCount = maxInFilterRowCount;
        this.filterRowType = filterRowType;
    }

    @Override
    public <T extends StreamOperator<RowData>> T createStreamOperator(
            StreamOperatorParameters<RowData> parameters) {
        final OperatorID operatorId = parameters.getStreamConfig().getOperatorID();
        final OperatorEventDispatcher eventDispatcher = parameters.getOperatorEventDispatcher();
        OperatorEventGateway operatorEventGateway =
                eventDispatcher.getOperatorEventGateway(operatorId);

        GlobalRuntimeFilterBuilderOperator operator =
                new GlobalRuntimeFilterBuilderOperator(
                        parameters,
                        estimatedRowCount,
                        maxRowCount,
                        maxInFilterRowCount,
                        filterRowType,
                        operatorEventGateway);

        @SuppressWarnings("unchecked")
        final T castedOperator = (T) operator;

        return castedOperator;
    }

    public void registerRuntimeFilteringDataListenerID(String id) {
        this.runtimeFilteringDataListenerIDs.add(checkNotNull(id));
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return GlobalRuntimeFilterBuilderOperator.class;
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(
            String operatorName, OperatorID operatorID) {
        return new GlobalRuntimeFilterBuilderOperatorCoordinator.Provider(
                operatorID, new ArrayList<>(runtimeFilteringDataListenerIDs));
    }
}

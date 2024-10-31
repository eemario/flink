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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.connector.source.RuntimeFilteringData;
import org.apache.flink.table.connector.source.RuntimeFilteringEvent;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.operators.runtimefilter.util.BloomFilterRuntimeFilter;
import org.apache.flink.table.runtime.operators.runtimefilter.util.InFilterRuntimeFilter;
import org.apache.flink.table.runtime.operators.runtimefilter.util.RuntimeFilter;
import org.apache.flink.table.runtime.operators.runtimefilter.util.RuntimeFilterUtils;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.runtime.operators.runtimefilter.util.RuntimeFilterUtils.OVER_MAX_ROW_COUNT;
import static org.apache.flink.util.Preconditions.checkArgument;

/** Global runtime filter builder operator. */
public class GlobalRuntimeFilterBuilderOperator extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, BoundedOneInput {

    private static final Logger LOG =
            LoggerFactory.getLogger(GlobalRuntimeFilterBuilderOperator.class);

    /**
     * The maximum number of rows to build the bloom filter. Once the actual number of rows received
     * is greater than this value, we will give up building the bloom filter and directly output an
     * empty filter.
     */
    private final int estimatedRowCount;

    private final int maxRowCount;

    private final int maxInFilterRowCount;

    private final RowType filterRowType;

    private final OperatorEventGateway operatorEventGateway;

    private transient RuntimeFilter filter;
    private transient Collector<RowData> collector;
    private transient int globalRowCount;
    private transient TypeInformation<RowData> typeInfo;
    private transient RowDataSerializer serializer;

    public GlobalRuntimeFilterBuilderOperator(
            StreamOperatorParameters<RowData> parameters,
            int estimatedRowCount,
            int maxRowCount,
            int maxInFilterRowCount,
            RowType filterRowType,
            OperatorEventGateway operatorEventGateway) {
        super(parameters);
        checkArgument(maxRowCount > 0);
        checkArgument(maxRowCount >= maxInFilterRowCount);
        this.estimatedRowCount = estimatedRowCount;
        this.maxRowCount = maxRowCount;
        this.maxInFilterRowCount = maxInFilterRowCount;
        this.filterRowType = filterRowType;
        this.operatorEventGateway = operatorEventGateway;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.filter = null;
        this.collector = new StreamRecordCollector<>(output);
        this.globalRowCount = 0;
        this.typeInfo = InternalTypeInfo.of(filterRowType);
        this.serializer = new RowDataSerializer(filterRowType);
        //        this.serializer = typeInfo.createSerializer(new SerializerConfigImpl());
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData rowData = element.getValue();
        int localRowCount = rowData.getInt(0);

        if (globalRowCount == OVER_MAX_ROW_COUNT) {
            // Current global filter is already over-max-row-count, do nothing.
        } else if (localRowCount == OVER_MAX_ROW_COUNT
                || globalRowCount + localRowCount > maxRowCount) {
            // The received local filter is over-max-row-count, mark the global filter as
            // over-max-row-count.
            globalRowCount = OVER_MAX_ROW_COUNT;
            filter = null;
        } else {
            // merge the local filter
            globalRowCount += localRowCount;
            RuntimeFilter localFilter =
                    RuntimeFilterUtils.convertRowDataToRuntimeFilter(rowData, serializer);
            if (filter == null) {
                filter = localFilter;
            } else {
                if (filter instanceof InFilterRuntimeFilter
                        && (localFilter instanceof BloomFilterRuntimeFilter
                                || globalRowCount > maxInFilterRowCount)) {
                    filter =
                            RuntimeFilterUtils.convertInFilterToBloomFilter(
                                    estimatedRowCount, (InFilterRuntimeFilter) filter);
                }
                filter.merge(localFilter);
            }
        }
    }

    @Override
    public void endInput() throws Exception {
        sendEvent();

        collector.collect(RuntimeFilterUtils.convertRuntimeFilterToRowData(globalRowCount, filter));
    }

    private void sendEvent() throws Exception {
        final RuntimeFilteringData runtimeFilteringData;
        if (globalRowCount != OVER_MAX_ROW_COUNT) {
            if (filter instanceof InFilterRuntimeFilter) {
                InFilterRuntimeFilter inFilter = (InFilterRuntimeFilter) filter;
                List<byte[]> serializedData = new ArrayList<>();
                for (RowData rowData : inFilter.getInFilter()) {
                    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                        DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);
                        serializer.serialize(rowData, wrapper);
                        serializedData.add(baos.toByteArray());
                    }
                }
                runtimeFilteringData =
                        new RuntimeFilteringData(
                                typeInfo, filterRowType, serializedData, true, "bitmap");
            } else {
                // TODO support bloom filter
                runtimeFilteringData =
                        new RuntimeFilteringData(
                                typeInfo,
                                filterRowType,
                                Collections.emptyList(),
                                false,
                                "bloom-filter");
            }
        } else {
            LOG.info("The built filter is over max row count");
            runtimeFilteringData =
                    new RuntimeFilteringData(
                            typeInfo, filterRowType, Collections.emptyList(), false, null);
        }

        LOG.info("Sending runtime filtering event with {}", runtimeFilteringData);
        RuntimeFilteringEvent event = new RuntimeFilteringEvent(runtimeFilteringData);
        operatorEventGateway.sendEventToCoordinator(new SourceEventWrapper(event));
    }
}

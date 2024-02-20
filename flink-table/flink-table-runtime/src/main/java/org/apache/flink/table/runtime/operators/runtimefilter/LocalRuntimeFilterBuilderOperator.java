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

import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.operators.runtimefilter.util.InFilterRuntimeFilter;
import org.apache.flink.table.runtime.operators.runtimefilter.util.RuntimeFilter;
import org.apache.flink.table.runtime.operators.runtimefilter.util.RuntimeFilterUtils;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.runtime.operators.runtimefilter.util.RuntimeFilterUtils.OVER_MAX_ROW_COUNT;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Local runtime filter builder operator. */
public class LocalRuntimeFilterBuilderOperator extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, BoundedOneInput {

    private final GeneratedProjection buildProjectionCode;
    private final int estimatedRowCount;

    /**
     * The maximum number of rows to build the bloom filter. Once the actual number of rows received
     * is greater than this value, we will give up building the bloom filter and directly output an
     * empty filter.
     */
    private final int maxRowCount;

    private final int maxInFilterRowCount;
    private final RowDataSerializer rowDataSerializer;

    private transient Projection<RowData, BinaryRowData> buildSideProjection;
    private transient RuntimeFilter filter;
    private transient Collector<RowData> collector;
    private transient int actualRowCount;

    public LocalRuntimeFilterBuilderOperator(
            GeneratedProjection buildProjectionCode,
            int estimatedRowCount,
            int maxRowCount,
            int maxInFilterRowCount,
            RowDataSerializer rowDataSerializer) {
        checkArgument(estimatedRowCount > 0);
        checkArgument(maxRowCount >= estimatedRowCount);
        checkArgument(maxRowCount >= maxInFilterRowCount);
        this.buildProjectionCode = checkNotNull(buildProjectionCode);
        this.estimatedRowCount = estimatedRowCount;
        this.maxRowCount = maxRowCount;
        this.maxInFilterRowCount = maxInFilterRowCount;
        this.rowDataSerializer = rowDataSerializer;
    }

    @Override
    public void open() throws Exception {
        super.open();

        this.buildSideProjection = buildProjectionCode.newInstance(getUserCodeClassloader());
        this.filter = new InFilterRuntimeFilter(rowDataSerializer);
        this.collector = new StreamRecordCollector<>(output);
        this.actualRowCount = 0;
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        if (filter != null) {
            checkNotNull(buildSideProjection);
            if (filter.add(buildSideProjection.apply(element.getValue()).copy())) {
                actualRowCount++;
                if (filter instanceof InFilterRuntimeFilter
                        && actualRowCount > maxInFilterRowCount) {
                    InFilterRuntimeFilter inFilterRuntimeFilter = (InFilterRuntimeFilter) filter;
                    filter =
                            RuntimeFilterUtils.convertInFilterToBloomFilter(
                                    estimatedRowCount, inFilterRuntimeFilter);
                }
            }

            if (actualRowCount > maxRowCount) {
                // the actual row count is over the allowed max row count, we will output a
                // null/empty filter
                filter = null;
                actualRowCount = OVER_MAX_ROW_COUNT;
            }
        }
    }

    @Override
    public void endInput() throws Exception {
        collector.collect(RuntimeFilterUtils.convertRuntimeFilterToRowData(actualRowCount, filter));
    }
}

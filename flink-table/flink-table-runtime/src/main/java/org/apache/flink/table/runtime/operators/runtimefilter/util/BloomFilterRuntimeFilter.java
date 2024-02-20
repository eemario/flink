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

package org.apache.flink.table.runtime.operators.runtimefilter.util;

import org.apache.flink.runtime.operators.util.BloomFilter;
import org.apache.flink.table.data.RowData;

/** Runtime filter implementation with bloom filter. * */
public class BloomFilterRuntimeFilter implements RuntimeFilter {
    private BloomFilter filter;

    public BloomFilterRuntimeFilter(int estimatedRowCount) {
        this.filter = RuntimeFilterUtils.createOnHeapBloomFilter(estimatedRowCount);
    }

    public BloomFilterRuntimeFilter(BloomFilter bloomFilter) {
        this.filter = bloomFilter;
    }

    @Override
    public RuntimeFilterType getRuntimeFilterType() {
        return RuntimeFilterType.BLOOM_FILTER;
    }

    @Override
    public boolean add(RowData rowData) {
        filter.addHash(rowData.hashCode());
        return true;
    }

    @Override
    public boolean test(RowData rowData) {
        return filter.testHash(rowData.hashCode());
    }

    @Override
    public byte[] toBytes() {
        return BloomFilter.toBytes(filter);
    }

    @Override
    public void merge(RuntimeFilter runtimeFilter) {
        if (runtimeFilter instanceof InFilterRuntimeFilter) {
            InFilterRuntimeFilter inFilterRuntimeFilter = (InFilterRuntimeFilter) runtimeFilter;
            for (RowData rowData : inFilterRuntimeFilter.getInFilter()) {
                filter.addHash(rowData.hashCode());
            }
        } else if (runtimeFilter instanceof BloomFilterRuntimeFilter) {
            BloomFilterRuntimeFilter bloomFilterRuntimeFilter =
                    (BloomFilterRuntimeFilter) runtimeFilter;
            byte[] mergedSerializedBloomFilter =
                    BloomFilter.mergeSerializedBloomFilters(
                            toBytes(), bloomFilterRuntimeFilter.toBytes());
            filter = BloomFilter.fromBytes(mergedSerializedBloomFilter);
        } else {
            throw new RuntimeException(
                    "Unknown runtime filter type to merge with BloomFilterRuntimeFilter.");
        }
    }
}

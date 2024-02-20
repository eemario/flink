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

import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;

import java.io.IOException;
import java.util.HashSet;

/** Runtime filter implementation with in filter. * */
public class InFilterRuntimeFilter implements RuntimeFilter {
    private HashSet<RowData> filter;
    private RowDataSerializer rowDataSerializer;

    public InFilterRuntimeFilter(RowDataSerializer rowDataSerializer) {
        this.filter = new HashSet<>();
        this.rowDataSerializer = rowDataSerializer;
    }

    public InFilterRuntimeFilter(HashSet<RowData> inFilter, RowDataSerializer rowDataSerializer) {
        this.filter = inFilter;
        this.rowDataSerializer = rowDataSerializer;
    }

    public HashSet<RowData> getInFilter() {
        return filter;
    }

    @Override
    public RuntimeFilterType getRuntimeFilterType() {
        return RuntimeFilterType.IN_FILTER;
    }

    @Override
    public boolean add(RowData rowData) {
        return filter.add(rowData);
    }

    @Override
    public boolean test(RowData rowData) {
        return filter.contains(rowData);
    }

    @Override
    public byte[] toBytes() throws IOException {
        DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(256);
        dataOutputSerializer.writeInt(filter.size());
        for (RowData rowData : filter) {
            rowDataSerializer.serialize(rowData, dataOutputSerializer);
        }
        return dataOutputSerializer.getCopyOfBuffer();
    }

    @Override
    public void merge(RuntimeFilter runtimeFilter) {
        if (runtimeFilter instanceof InFilterRuntimeFilter) {
            InFilterRuntimeFilter inFilterRuntimeFilter = (InFilterRuntimeFilter) runtimeFilter;
            filter.addAll(inFilterRuntimeFilter.getInFilter());
        } else {
            throw new RuntimeException(
                    "Unknown runtime filter type to merge with InFilterRuntimeFilter.");
        }
    }
}

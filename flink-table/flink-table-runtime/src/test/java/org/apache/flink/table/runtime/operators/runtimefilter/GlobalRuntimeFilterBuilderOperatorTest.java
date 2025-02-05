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

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.runtime.operators.coordination.MockOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.connector.source.InFilterRuntimeFilterType;
import org.apache.flink.table.connector.source.RuntimeFilteringData;
import org.apache.flink.table.connector.source.RuntimeFilteringEvent;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.operators.runtimefilter.util.BloomFilterRuntimeFilter;
import org.apache.flink.table.runtime.operators.runtimefilter.util.InFilterRuntimeFilter;
import org.apache.flink.table.runtime.operators.runtimefilter.util.RuntimeFilter;
import org.apache.flink.table.runtime.operators.runtimefilter.util.RuntimeFilterUtils;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.table.runtime.operators.runtimefilter.util.RuntimeFilterUtils.OVER_MAX_ROW_COUNT;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Test for {@link GlobalRuntimeFilterBuilderOperator}. */
class GlobalRuntimeFilterBuilderOperatorTest {

    private MockOperatorEventGateway gateway;

    /**
     * Test the case that all input local runtime filters are bloom filters, and the merged global
     * filter is a bloom filter.
     */
    @Test
    void testBloomFilterBloomFilterInputAndBloomFilterOutput() throws Exception {
        ConcurrentLinkedQueue<Object> output;
        try (OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createGlobalRuntimeFilterBuilderOperatorHarness(10, 10, 4)) {
            testHarness.processElement(new StreamRecord<>(convertBloomFilterRuntimeFilter1()));
            testHarness.processElement(new StreamRecord<>(convertBloomFilterRuntimeFilter2()));
            testHarness.endInput();
            output = testHarness.getOutput();
        }

        assertThat(output.size()).isEqualTo(1);

        RowData outputRowData = ((StreamRecord<RowData>) output.poll()).getValue();
        assertThat(outputRowData.getArity()).isEqualTo(3);

        int globalCount = outputRowData.getInt(0);
        RowDataSerializer rowDataSerializer = new RowDataSerializer(new VarCharType());
        RuntimeFilter globalRuntimeFilter =
                RuntimeFilterUtils.convertRowDataToRuntimeFilter(outputRowData, rowDataSerializer);

        assertThat(globalRuntimeFilter instanceof BloomFilterRuntimeFilter).isTrue();
        assertThat(globalCount).isEqualTo(10);
        assertThat(runtimeFilterTestBloomFilter1(globalRuntimeFilter)).isTrue();
        assertThat(runtimeFilterTestBloomFilter2(globalRuntimeFilter)).isTrue();
        assertThat(runtimeFilterTestString(globalRuntimeFilter, "var11")).isFalse();
        assertThat(runtimeFilterTestString(globalRuntimeFilter, "var12")).isFalse();
        assertThat(runtimeFilterTestString(globalRuntimeFilter, "var13")).isFalse();
        assertThat(runtimeFilterTestString(globalRuntimeFilter, "var14")).isFalse();
        assertThat(runtimeFilterTestString(globalRuntimeFilter, "var15")).isFalse();

        assertThat(gateway.getEventsSent().size()).isEqualTo(1);
        OperatorEvent event = gateway.getEventsSent().get(0);
        assertThat(event).isInstanceOf(SourceEventWrapper.class);
        SourceEvent runtimeFilteringEvent = ((SourceEventWrapper) event).getSourceEvent();
        assertThat(runtimeFilteringEvent).isInstanceOf(RuntimeFilteringEvent.class);

        RuntimeFilteringData data = ((RuntimeFilteringEvent) runtimeFilteringEvent).getData();
        assertThat(data.isFiltering()).isFalse();
    }

    /**
     * Test the case that all input local runtime filters are in filters, and the merged global
     * filter is an in filter.
     */
    @Test
    void testInFilterInFilterInputAndInFilterOutput() throws Exception {
        ConcurrentLinkedQueue<Object> output;
        try (OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createGlobalRuntimeFilterBuilderOperatorHarness(10, 10, 4)) {
            testHarness.processElement(new StreamRecord<>(convertInFilterRuntimeFilter1()));
            testHarness.processElement(new StreamRecord<>(convertInFilterRuntimeFilter2()));
            testHarness.endInput();
            output = testHarness.getOutput();
        }

        assertThat(output.size()).isEqualTo(1);

        RowData outputRowData = ((StreamRecord<RowData>) output.poll()).getValue();
        assertThat(outputRowData.getArity()).isEqualTo(3);

        int globalCount = outputRowData.getInt(0);
        RowDataSerializer rowDataSerializer = new RowDataSerializer(new VarCharType());
        RuntimeFilter globalRuntimeFilter =
                RuntimeFilterUtils.convertRowDataToRuntimeFilter(outputRowData, rowDataSerializer);
        assertThat(globalRuntimeFilter instanceof InFilterRuntimeFilter).isTrue();
        assertThat(globalCount).isEqualTo(4);
        assertThat(runtimeFilterTestInFilter1(globalRuntimeFilter)).isTrue();
        assertThat(runtimeFilterTestInFilter2(globalRuntimeFilter)).isTrue();
        assertThat(runtimeFilterTestString(globalRuntimeFilter, "var11")).isFalse();
        assertThat(runtimeFilterTestString(globalRuntimeFilter, "var12")).isFalse();
        assertThat(runtimeFilterTestString(globalRuntimeFilter, "var13")).isFalse();
        assertThat(runtimeFilterTestString(globalRuntimeFilter, "var14")).isFalse();
        assertThat(runtimeFilterTestString(globalRuntimeFilter, "var15")).isFalse();

        assertThat(gateway.getEventsSent().size()).isEqualTo(1);
        OperatorEvent event = gateway.getEventsSent().get(0);
        assertThat(event).isInstanceOf(SourceEventWrapper.class);
        SourceEvent runtimeFilteringEvent = ((SourceEventWrapper) event).getSourceEvent();
        assertThat(runtimeFilteringEvent).isInstanceOf(RuntimeFilteringEvent.class);

        RuntimeFilteringData data = ((RuntimeFilteringEvent) runtimeFilteringEvent).getData();
        assertThat(data.isFiltering()).isTrue();
        assertThat(data.getData().size()).isEqualTo(4);
        assertThat(data.getData().contains(GenericRowData.of(StringData.fromString("var21"))));
        assertThat(data.getData().contains(GenericRowData.of(StringData.fromString("var22"))));
        assertThat(data.getData().contains(GenericRowData.of(StringData.fromString("var23"))));
        assertThat(data.getData().contains(GenericRowData.of(StringData.fromString("var24"))));
    }

    /**
     * Test the case that all input local runtime filters are in filters, but the merged global
     * filter is a bloom filter.
     */
    @Test
    void testInFilterInFilterInputAndBloomFilterOutput() throws Exception {
        ConcurrentLinkedQueue<Object> output;
        try (OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createGlobalRuntimeFilterBuilderOperatorHarness(10, 10, 3)) {
            testHarness.processElement(new StreamRecord<>(convertInFilterRuntimeFilter1()));
            testHarness.processElement(new StreamRecord<>(convertInFilterRuntimeFilter2()));
            testHarness.endInput();
            output = testHarness.getOutput();
        }

        assertThat(output.size()).isEqualTo(1);

        RowData outputRowData = ((StreamRecord<RowData>) output.poll()).getValue();
        assertThat(outputRowData.getArity()).isEqualTo(3);

        int globalCount = outputRowData.getInt(0);
        RowDataSerializer rowDataSerializer = new RowDataSerializer(new VarCharType());
        RuntimeFilter globalRuntimeFilter =
                RuntimeFilterUtils.convertRowDataToRuntimeFilter(outputRowData, rowDataSerializer);
        assertThat(globalRuntimeFilter instanceof BloomFilterRuntimeFilter).isTrue();
        assertThat(globalCount).isEqualTo(4);
        assertThat(runtimeFilterTestInFilter1(globalRuntimeFilter)).isTrue();
        assertThat(runtimeFilterTestInFilter2(globalRuntimeFilter)).isTrue();
        assertThat(runtimeFilterTestString(globalRuntimeFilter, "var11")).isFalse();
        assertThat(runtimeFilterTestString(globalRuntimeFilter, "var12")).isFalse();
        assertThat(runtimeFilterTestString(globalRuntimeFilter, "var13")).isFalse();
        assertThat(runtimeFilterTestString(globalRuntimeFilter, "var14")).isFalse();
        assertThat(runtimeFilterTestString(globalRuntimeFilter, "var15")).isFalse();

        assertThat(gateway.getEventsSent().size()).isEqualTo(1);
        OperatorEvent event = gateway.getEventsSent().get(0);
        assertThat(event).isInstanceOf(SourceEventWrapper.class);
        SourceEvent runtimeFilteringEvent = ((SourceEventWrapper) event).getSourceEvent();
        assertThat(runtimeFilteringEvent).isInstanceOf(RuntimeFilteringEvent.class);

        RuntimeFilteringData data = ((RuntimeFilteringEvent) runtimeFilteringEvent).getData();
        assertThat(data.isFiltering()).isFalse();
    }

    /**
     * Test the case that all input local runtime filters are in filters, but the merged global
     * filter is over-max-row-count.
     */
    @Test
    void testInFilterInFilterInputAndOverMaxRowCountOutput() throws Exception {
        ConcurrentLinkedQueue<Object> output;
        try (OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createGlobalRuntimeFilterBuilderOperatorHarness(2, 3, 3)) {
            testHarness.processElement(new StreamRecord<>(convertInFilterRuntimeFilter1()));
            testHarness.processElement(new StreamRecord<>(convertInFilterRuntimeFilter2()));
            testHarness.endInput();
            output = testHarness.getOutput();
        }

        assertThat(output.size()).isEqualTo(1);

        RowData outputRowData = ((StreamRecord<RowData>) output.poll()).getValue();
        assertThat(outputRowData.getArity()).isEqualTo(3);

        int globalCount = outputRowData.getInt(0);
        assertThat(globalCount).isEqualTo(OVER_MAX_ROW_COUNT);
        assertThat(outputRowData.isNullAt(1)).isTrue();
        assertThat(outputRowData.isNullAt(2)).isTrue();

        assertThat(gateway.getEventsSent().size()).isEqualTo(1);
        OperatorEvent event = gateway.getEventsSent().get(0);
        assertThat(event).isInstanceOf(SourceEventWrapper.class);
        SourceEvent runtimeFilteringEvent = ((SourceEventWrapper) event).getSourceEvent();
        assertThat(runtimeFilteringEvent).isInstanceOf(RuntimeFilteringEvent.class);

        RuntimeFilteringData data = ((RuntimeFilteringEvent) runtimeFilteringEvent).getData();
        assertThat(data.isFiltering()).isFalse();
    }

    /**
     * Test the case that one of the input local runtime filters is a bloom filter, and the merged
     * global filter is a bloom filter.
     */
    @Test
    void testInFilterBloomFilterInputAndBloomFilterOutput() throws Exception {
        ConcurrentLinkedQueue<Object> output;
        try (OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createGlobalRuntimeFilterBuilderOperatorHarness(10, 10, 4)) {
            testHarness.processElement(new StreamRecord<>(convertInFilterRuntimeFilter1()));
            testHarness.processElement(new StreamRecord<>(convertBloomFilterRuntimeFilter2()));
            testHarness.endInput();
            output = testHarness.getOutput();
        }

        assertThat(output.size()).isEqualTo(1);

        RowData outputRowData = ((StreamRecord<RowData>) output.poll()).getValue();
        assertThat(outputRowData.getArity()).isEqualTo(3);

        int globalCount = outputRowData.getInt(0);
        RowDataSerializer rowDataSerializer = new RowDataSerializer(new VarCharType());
        RuntimeFilter globalRuntimeFilter =
                RuntimeFilterUtils.convertRowDataToRuntimeFilter(outputRowData, rowDataSerializer);
        assertThat(globalRuntimeFilter instanceof BloomFilterRuntimeFilter).isTrue();
        assertThat(globalCount).isEqualTo(7);
        assertThat(runtimeFilterTestInFilter1(globalRuntimeFilter)).isTrue();
        assertThat(runtimeFilterTestBloomFilter2(globalRuntimeFilter)).isTrue();
        assertThat(runtimeFilterTestString(globalRuntimeFilter, "var11")).isFalse();
        assertThat(runtimeFilterTestString(globalRuntimeFilter, "var12")).isFalse();
        assertThat(runtimeFilterTestString(globalRuntimeFilter, "var13")).isFalse();
        assertThat(runtimeFilterTestString(globalRuntimeFilter, "var14")).isFalse();
        assertThat(runtimeFilterTestString(globalRuntimeFilter, "var15")).isFalse();

        assertThat(gateway.getEventsSent().size()).isEqualTo(1);
        OperatorEvent event = gateway.getEventsSent().get(0);
        assertThat(event).isInstanceOf(SourceEventWrapper.class);
        SourceEvent runtimeFilteringEvent = ((SourceEventWrapper) event).getSourceEvent();
        assertThat(runtimeFilteringEvent).isInstanceOf(RuntimeFilteringEvent.class);

        RuntimeFilteringData data = ((RuntimeFilteringEvent) runtimeFilteringEvent).getData();
        assertThat(data.isFiltering()).isFalse();
    }

    /**
     * Test the case that input local runtime filters are normal filters with different types, but
     * the merged global filter is over-max-row-count.
     */
    @Test
    void testInFilterBloomFilterInputAndOverMaxRowCountOutput() throws Exception {
        ConcurrentLinkedQueue<Object> output;
        try (OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createGlobalRuntimeFilterBuilderOperatorHarness(5, 6, 4)) {
            testHarness.processElement(new StreamRecord<>(convertInFilterRuntimeFilter1()));
            testHarness.processElement(new StreamRecord<>(convertBloomFilterRuntimeFilter2()));
            testHarness.endInput();
            output = testHarness.getOutput();
        }

        assertThat(output.size()).isEqualTo(1);

        RowData outputRowData = ((StreamRecord<RowData>) output.poll()).getValue();
        assertThat(outputRowData.getArity()).isEqualTo(3);

        int globalCount = outputRowData.getInt(0);
        assertThat(globalCount).isEqualTo(OVER_MAX_ROW_COUNT);
        assertThat(outputRowData.isNullAt(1)).isTrue();
        assertThat(outputRowData.isNullAt(2)).isTrue();

        assertThat(gateway.getEventsSent().size()).isEqualTo(1);
        OperatorEvent event = gateway.getEventsSent().get(0);
        assertThat(event).isInstanceOf(SourceEventWrapper.class);
        SourceEvent runtimeFilteringEvent = ((SourceEventWrapper) event).getSourceEvent();
        assertThat(runtimeFilteringEvent).isInstanceOf(RuntimeFilteringEvent.class);

        RuntimeFilteringData data = ((RuntimeFilteringEvent) runtimeFilteringEvent).getData();
        assertThat(data.isFiltering()).isFalse();
    }

    /**
     * Test the case that all input local runtime filters are bloom filters, but the merged global
     * filter is over-max-row-count.
     */
    @Test
    void testBloomFilterBloomFilterInputAndOverMaxRowCountOutput() throws Exception {
        ConcurrentLinkedQueue<Object> output;
        try (OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createGlobalRuntimeFilterBuilderOperatorHarness(9, 9, 4)) {
            testHarness.processElement(new StreamRecord<>(convertBloomFilterRuntimeFilter1()));
            testHarness.processElement(new StreamRecord<>(convertBloomFilterRuntimeFilter2()));
            testHarness.endInput();
            output = testHarness.getOutput();
        }

        assertThat(output.size()).isEqualTo(1);

        RowData outputRowData = ((StreamRecord<RowData>) output.poll()).getValue();
        assertThat(outputRowData.getArity()).isEqualTo(3);

        int globalCount = outputRowData.getInt(0);
        assertThat(globalCount).isEqualTo(OVER_MAX_ROW_COUNT);
        assertThat(outputRowData.isNullAt(1)).isTrue();
        assertThat(outputRowData.isNullAt(2)).isTrue();

        assertThat(gateway.getEventsSent().size()).isEqualTo(1);
        OperatorEvent event = gateway.getEventsSent().get(0);
        assertThat(event).isInstanceOf(SourceEventWrapper.class);
        SourceEvent runtimeFilteringEvent = ((SourceEventWrapper) event).getSourceEvent();
        assertThat(runtimeFilteringEvent).isInstanceOf(RuntimeFilteringEvent.class);

        RuntimeFilteringData data = ((RuntimeFilteringEvent) runtimeFilteringEvent).getData();
        assertThat(data.isFiltering()).isFalse();
    }

    /** Test the case that one of the input local runtime filters is over-max-row-count. */
    @Test
    void testOverMaxRowCountInput() throws Exception {
        ConcurrentLinkedQueue<Object> output;
        try (OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createGlobalRuntimeFilterBuilderOperatorHarness(10, 10, 4)) {
            testHarness.processElement(new StreamRecord<>(convertInFilterRuntimeFilter1()));
            testHarness.processElement(
                    new StreamRecord<>(GenericRowData.of(OVER_MAX_ROW_COUNT, null, null)));
            testHarness.endInput();
            output = testHarness.getOutput();
        }

        assertThat(output.size()).isEqualTo(1);

        RowData outputRowData = ((StreamRecord<RowData>) output.poll()).getValue();
        assertThat(outputRowData.getArity()).isEqualTo(3);

        int globalCount = outputRowData.getInt(0);
        assertThat(globalCount).isEqualTo(OVER_MAX_ROW_COUNT);
        assertThat(outputRowData.isNullAt(1)).isTrue();
        assertThat(outputRowData.isNullAt(2)).isTrue();

        assertThat(gateway.getEventsSent().size()).isEqualTo(1);
        OperatorEvent event = gateway.getEventsSent().get(0);
        assertThat(event).isInstanceOf(SourceEventWrapper.class);
        SourceEvent runtimeFilteringEvent = ((SourceEventWrapper) event).getSourceEvent();
        assertThat(runtimeFilteringEvent).isInstanceOf(RuntimeFilteringEvent.class);

        RuntimeFilteringData data = ((RuntimeFilteringEvent) runtimeFilteringEvent).getData();
        assertThat(data.isFiltering()).isFalse();
    }

    private static void runtimeFilterAddString(RuntimeFilter runtimeFilter, String string) {
        RowDataSerializer rowDataSerializer = new RowDataSerializer(new VarCharType());
        runtimeFilter.add(
                rowDataSerializer.toBinaryRow(GenericRowData.of(StringData.fromString(string))));
    }

    private static boolean runtimeFilterTestString(RuntimeFilter runtimeFilter, String string) {
        RowDataSerializer rowDataSerializer = new RowDataSerializer(new VarCharType());
        return runtimeFilter.test(
                rowDataSerializer.toBinaryRow(GenericRowData.of(StringData.fromString(string))));
    }

    private static RowData convertBloomFilterRuntimeFilter1() throws IOException {
        final BloomFilterRuntimeFilter bloomFilterRuntimeFilter1 = new BloomFilterRuntimeFilter(10);
        runtimeFilterAddString(bloomFilterRuntimeFilter1, "var1");
        runtimeFilterAddString(bloomFilterRuntimeFilter1, "var2");
        runtimeFilterAddString(bloomFilterRuntimeFilter1, "var3");
        runtimeFilterAddString(bloomFilterRuntimeFilter1, "var4");
        runtimeFilterAddString(bloomFilterRuntimeFilter1, "var5");
        return RuntimeFilterUtils.convertRuntimeFilterToRowData(5, bloomFilterRuntimeFilter1);
    }

    private static RowData convertBloomFilterRuntimeFilter2() throws IOException {
        final BloomFilterRuntimeFilter bloomFilterRuntimeFilter2 = new BloomFilterRuntimeFilter(10);
        runtimeFilterAddString(bloomFilterRuntimeFilter2, "var6");
        runtimeFilterAddString(bloomFilterRuntimeFilter2, "var7");
        runtimeFilterAddString(bloomFilterRuntimeFilter2, "var8");
        runtimeFilterAddString(bloomFilterRuntimeFilter2, "var9");
        runtimeFilterAddString(bloomFilterRuntimeFilter2, "var10");
        return RuntimeFilterUtils.convertRuntimeFilterToRowData(5, bloomFilterRuntimeFilter2);
    }

    private static RowData convertInFilterRuntimeFilter1() throws IOException {
        RowDataSerializer rowDataSerializer = new RowDataSerializer(new VarCharType());
        final InFilterRuntimeFilter inFilterRuntimeFilter1 =
                new InFilterRuntimeFilter(rowDataSerializer);
        runtimeFilterAddString(inFilterRuntimeFilter1, "var21");
        runtimeFilterAddString(inFilterRuntimeFilter1, "var22");
        return RuntimeFilterUtils.convertRuntimeFilterToRowData(2, inFilterRuntimeFilter1);
    }

    private static RowData convertInFilterRuntimeFilter2() throws IOException {
        RowDataSerializer rowDataSerializer = new RowDataSerializer(new VarCharType());
        final InFilterRuntimeFilter inFilterRuntimeFilter2 =
                new InFilterRuntimeFilter(rowDataSerializer);
        runtimeFilterAddString(inFilterRuntimeFilter2, "var23");
        runtimeFilterAddString(inFilterRuntimeFilter2, "var24");
        return RuntimeFilterUtils.convertRuntimeFilterToRowData(2, inFilterRuntimeFilter2);
    }

    private static boolean runtimeFilterTestBloomFilter1(RuntimeFilter runtimeFilter) {
        return runtimeFilterTestString(runtimeFilter, "var1")
                && runtimeFilterTestString(runtimeFilter, "var2")
                && runtimeFilterTestString(runtimeFilter, "var3")
                && runtimeFilterTestString(runtimeFilter, "var4")
                && runtimeFilterTestString(runtimeFilter, "var5");
    }

    private static boolean runtimeFilterTestBloomFilter2(RuntimeFilter runtimeFilter) {
        return runtimeFilterTestString(runtimeFilter, "var6")
                && runtimeFilterTestString(runtimeFilter, "var7")
                && runtimeFilterTestString(runtimeFilter, "var8")
                && runtimeFilterTestString(runtimeFilter, "var9")
                && runtimeFilterTestString(runtimeFilter, "var10");
    }

    private static boolean runtimeFilterTestInFilter1(RuntimeFilter runtimeFilter) {
        return runtimeFilterTestString(runtimeFilter, "var21")
                && runtimeFilterTestString(runtimeFilter, "var22");
    }

    private static boolean runtimeFilterTestInFilter2(RuntimeFilter runtimeFilter) {
        return runtimeFilterTestString(runtimeFilter, "var23")
                && runtimeFilterTestString(runtimeFilter, "var24");
    }

    private OneInputStreamOperatorTestHarness<RowData, RowData>
            createGlobalRuntimeFilterBuilderOperatorHarness(
                    int estimatedRowCount, int maxRowCount, int maxInFilterRowCount)
                    throws Exception {
        // uses OneInputStreamOperatorTestHarness because we need to mock the OperatorEventGateway
        // for the tests
        gateway = new MockOperatorEventGateway();
        RowType filterRowType = RowType.of(new VarCharType());
        final GlobalRuntimeFilterBuilderOperator operator =
                new GlobalRuntimeFilterBuilderOperator(
                        null,
                        estimatedRowCount,
                        maxRowCount,
                        maxInFilterRowCount,
                        filterRowType,
                        gateway,
                        Collections.singleton(new InFilterRuntimeFilterType()));
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                new OneInputStreamOperatorTestHarness<>(operator);
        testHarness.setup();
        testHarness.open();
        return testHarness;
    }
}

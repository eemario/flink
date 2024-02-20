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

import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarnessBuilder;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.operators.runtimefilter.util.BloomFilterRuntimeFilter;
import org.apache.flink.table.runtime.operators.runtimefilter.util.InFilterRuntimeFilter;
import org.apache.flink.table.runtime.operators.runtimefilter.util.RuntimeFilter;
import org.apache.flink.table.runtime.operators.runtimefilter.util.RuntimeFilterUtils;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Queue;

import static org.apache.flink.table.runtime.operators.runtimefilter.util.RuntimeFilterUtils.OVER_MAX_ROW_COUNT;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Test for {@link GlobalRuntimeFilterBuilderOperator}. */
class GlobalRuntimeFilterBuilderOperatorTest {

    /**
     * Test the case that all input local runtime filters are bloom filters, and the merged global
     * filter is a bloom filter.
     */
    @Test
    void testBloomFilterBloomFilterInputAndBloomFilterOutput() throws Exception {
        try (StreamTaskMailboxTestHarness<RowData> testHarness =
                createGlobalRuntimeFilterBuilderOperatorHarness(10, 10, 4)) {
            // process elements
            testHarness.processElement(new StreamRecord<>(convertBloomFilterRuntimeFilter1()));
            testHarness.processElement(new StreamRecord<>(convertBloomFilterRuntimeFilter2()));
            testHarness.processEvent(new EndOfData(StopMode.DRAIN), 0);

            // test the output
            Queue<Object> outputs = testHarness.getOutput();
            assertThat(outputs.size()).isEqualTo(1);

            RowData outputRowData = ((StreamRecord<RowData>) outputs.poll()).getValue();
            assertThat(outputRowData.getArity()).isEqualTo(3);

            int globalCount = outputRowData.getInt(0);
            RowDataSerializer rowDataSerializer = new RowDataSerializer(new VarCharType());
            RuntimeFilter globalRuntimeFilter =
                    RuntimeFilterUtils.convertRowDataToRuntimeFilter(
                            outputRowData, rowDataSerializer);
            assertThat(globalRuntimeFilter instanceof BloomFilterRuntimeFilter).isTrue();
            assertThat(globalCount).isEqualTo(10);
            assertThat(runtimeFilterTestBloomFilter1(globalRuntimeFilter)).isTrue();
            assertThat(runtimeFilterTestBloomFilter2(globalRuntimeFilter)).isTrue();
            assertThat(runtimeFilterTestString(globalRuntimeFilter, "var11")).isFalse();
            assertThat(runtimeFilterTestString(globalRuntimeFilter, "var12")).isFalse();
            assertThat(runtimeFilterTestString(globalRuntimeFilter, "var13")).isFalse();
            assertThat(runtimeFilterTestString(globalRuntimeFilter, "var14")).isFalse();
            assertThat(runtimeFilterTestString(globalRuntimeFilter, "var15")).isFalse();
        }
    }

    /**
     * Test the case that all input local runtime filters are in filters, and the merged global
     * filter is an in filter.
     */
    @Test
    void testInFilterInFilterInputAndInFilterOutput() throws Exception {
        try (StreamTaskMailboxTestHarness<RowData> testHarness =
                createGlobalRuntimeFilterBuilderOperatorHarness(10, 10, 4)) {
            // process elements
            testHarness.processElement(new StreamRecord<>(convertInFilterRuntimeFilter1()));
            testHarness.processElement(new StreamRecord<>(convertInFilterRuntimeFilter2()));
            testHarness.processEvent(new EndOfData(StopMode.DRAIN), 0);

            // test the output
            Queue<Object> outputs = testHarness.getOutput();
            assertThat(outputs.size()).isEqualTo(1);

            RowData outputRowData = ((StreamRecord<RowData>) outputs.poll()).getValue();
            assertThat(outputRowData.getArity()).isEqualTo(3);

            int globalCount = outputRowData.getInt(0);
            RowDataSerializer rowDataSerializer = new RowDataSerializer(new VarCharType());
            RuntimeFilter globalRuntimeFilter =
                    RuntimeFilterUtils.convertRowDataToRuntimeFilter(
                            outputRowData, rowDataSerializer);
            assertThat(globalRuntimeFilter instanceof InFilterRuntimeFilter).isTrue();
            assertThat(globalCount).isEqualTo(4);
            assertThat(runtimeFilterTestInFilter1(globalRuntimeFilter)).isTrue();
            assertThat(runtimeFilterTestInFilter2(globalRuntimeFilter)).isTrue();
            assertThat(runtimeFilterTestString(globalRuntimeFilter, "var11")).isFalse();
            assertThat(runtimeFilterTestString(globalRuntimeFilter, "var12")).isFalse();
            assertThat(runtimeFilterTestString(globalRuntimeFilter, "var13")).isFalse();
            assertThat(runtimeFilterTestString(globalRuntimeFilter, "var14")).isFalse();
            assertThat(runtimeFilterTestString(globalRuntimeFilter, "var15")).isFalse();
        }
    }

    /**
     * Test the case that all input local runtime filters are in filters, but the merged global
     * filter is a bloom filter.
     */
    @Test
    void testInFilterInFilterInputAndBloomFilterOutput() throws Exception {
        try (StreamTaskMailboxTestHarness<RowData> testHarness =
                createGlobalRuntimeFilterBuilderOperatorHarness(10, 10, 3)) {
            // process elements
            testHarness.processElement(new StreamRecord<>(convertInFilterRuntimeFilter1()));
            testHarness.processElement(new StreamRecord<>(convertInFilterRuntimeFilter2()));
            testHarness.processEvent(new EndOfData(StopMode.DRAIN), 0);

            // test the output
            Queue<Object> outputs = testHarness.getOutput();
            assertThat(outputs.size()).isEqualTo(1);

            RowData outputRowData = ((StreamRecord<RowData>) outputs.poll()).getValue();
            assertThat(outputRowData.getArity()).isEqualTo(3);

            int globalCount = outputRowData.getInt(0);
            RowDataSerializer rowDataSerializer = new RowDataSerializer(new VarCharType());
            RuntimeFilter globalRuntimeFilter =
                    RuntimeFilterUtils.convertRowDataToRuntimeFilter(
                            outputRowData, rowDataSerializer);
            assertThat(globalRuntimeFilter instanceof BloomFilterRuntimeFilter).isTrue();
            assertThat(globalCount).isEqualTo(4);
            assertThat(runtimeFilterTestInFilter1(globalRuntimeFilter)).isTrue();
            assertThat(runtimeFilterTestInFilter2(globalRuntimeFilter)).isTrue();
            assertThat(runtimeFilterTestString(globalRuntimeFilter, "var11")).isFalse();
            assertThat(runtimeFilterTestString(globalRuntimeFilter, "var12")).isFalse();
            assertThat(runtimeFilterTestString(globalRuntimeFilter, "var13")).isFalse();
            assertThat(runtimeFilterTestString(globalRuntimeFilter, "var14")).isFalse();
            assertThat(runtimeFilterTestString(globalRuntimeFilter, "var15")).isFalse();
        }
    }

    /**
     * Test the case that all input local runtime filters are in filters, but the merged global
     * filter is over-max-row-count.
     */
    @Test
    void testInFilterInFilterInputAndOverMaxRowCountOutput() throws Exception {
        try (StreamTaskMailboxTestHarness<RowData> testHarness =
                createGlobalRuntimeFilterBuilderOperatorHarness(2, 3, 3)) {
            // process elements
            testHarness.processElement(new StreamRecord<>(convertInFilterRuntimeFilter1()));
            testHarness.processElement(new StreamRecord<>(convertInFilterRuntimeFilter2()));
            testHarness.processEvent(new EndOfData(StopMode.DRAIN), 0);

            // test the output
            Queue<Object> outputs = testHarness.getOutput();
            assertThat(outputs.size()).isEqualTo(1);

            RowData outputRowData = ((StreamRecord<RowData>) outputs.poll()).getValue();
            assertThat(outputRowData.getArity()).isEqualTo(3);

            int globalCount = outputRowData.getInt(0);
            assertThat(globalCount).isEqualTo(OVER_MAX_ROW_COUNT);
            assertThat(outputRowData.isNullAt(1)).isTrue();
            assertThat(outputRowData.isNullAt(2)).isTrue();
        }
    }

    /**
     * Test the case that one of the input local runtime filters is a bloom filter, and the merged
     * global filter is a bloom filter.
     */
    @Test
    void testInFilterBloomFilterInputAndBloomFilterOutput() throws Exception {
        try (StreamTaskMailboxTestHarness<RowData> testHarness =
                createGlobalRuntimeFilterBuilderOperatorHarness(10, 10, 4)) {
            // process elements
            testHarness.processElement(new StreamRecord<>(convertInFilterRuntimeFilter1()));
            testHarness.processElement(new StreamRecord<>(convertBloomFilterRuntimeFilter2()));
            testHarness.processEvent(new EndOfData(StopMode.DRAIN), 0);

            // test the output
            Queue<Object> outputs = testHarness.getOutput();
            assertThat(outputs.size()).isEqualTo(1);

            RowData outputRowData = ((StreamRecord<RowData>) outputs.poll()).getValue();
            assertThat(outputRowData.getArity()).isEqualTo(3);

            int globalCount = outputRowData.getInt(0);
            RowDataSerializer rowDataSerializer = new RowDataSerializer(new VarCharType());
            RuntimeFilter globalRuntimeFilter =
                    RuntimeFilterUtils.convertRowDataToRuntimeFilter(
                            outputRowData, rowDataSerializer);
            assertThat(globalRuntimeFilter instanceof BloomFilterRuntimeFilter).isTrue();
            assertThat(globalCount).isEqualTo(7);
            assertThat(runtimeFilterTestInFilter1(globalRuntimeFilter)).isTrue();
            assertThat(runtimeFilterTestBloomFilter2(globalRuntimeFilter)).isTrue();
            assertThat(runtimeFilterTestString(globalRuntimeFilter, "var11")).isFalse();
            assertThat(runtimeFilterTestString(globalRuntimeFilter, "var12")).isFalse();
            assertThat(runtimeFilterTestString(globalRuntimeFilter, "var13")).isFalse();
            assertThat(runtimeFilterTestString(globalRuntimeFilter, "var14")).isFalse();
            assertThat(runtimeFilterTestString(globalRuntimeFilter, "var15")).isFalse();
        }
    }

    /**
     * Test the case that input local runtime filters are normal filters with different types, but
     * the merged global filter is over-max-row-count.
     */
    @Test
    void testInFilterBloomFilterInputAndOverMaxRowCountOutput() throws Exception {
        try (StreamTaskMailboxTestHarness<RowData> testHarness =
                createGlobalRuntimeFilterBuilderOperatorHarness(5, 6, 4)) {
            // process elements
            testHarness.processElement(new StreamRecord<>(convertInFilterRuntimeFilter1()));
            testHarness.processElement(new StreamRecord<>(convertBloomFilterRuntimeFilter2()));
            testHarness.processEvent(new EndOfData(StopMode.DRAIN), 0);

            // test the output
            Queue<Object> outputs = testHarness.getOutput();
            assertThat(outputs.size()).isEqualTo(1);

            RowData outputRowData = ((StreamRecord<RowData>) outputs.poll()).getValue();
            assertThat(outputRowData.getArity()).isEqualTo(3);

            int globalCount = outputRowData.getInt(0);
            assertThat(globalCount).isEqualTo(OVER_MAX_ROW_COUNT);
            assertThat(outputRowData.isNullAt(1)).isTrue();
            assertThat(outputRowData.isNullAt(2)).isTrue();
        }
    }

    /**
     * Test the case that all input local runtime filters are bloom filters, but the merged global
     * filter is over-max-row-count.
     */
    @Test
    void testBloomFilterBloomFilterInputAndOverMaxRowCountOutput() throws Exception {
        try (StreamTaskMailboxTestHarness<RowData> testHarness =
                createGlobalRuntimeFilterBuilderOperatorHarness(9, 9, 4)) {
            // process elements
            testHarness.processElement(new StreamRecord<>(convertBloomFilterRuntimeFilter1()));
            testHarness.processElement(new StreamRecord<>(convertBloomFilterRuntimeFilter2()));
            testHarness.processEvent(new EndOfData(StopMode.DRAIN), 0);

            // test the output
            Queue<Object> outputs = testHarness.getOutput();
            assertThat(outputs.size()).isEqualTo(1);

            RowData outputRowData = ((StreamRecord<RowData>) outputs.poll()).getValue();
            assertThat(outputRowData.getArity()).isEqualTo(3);

            int globalCount = outputRowData.getInt(0);
            assertThat(globalCount).isEqualTo(OVER_MAX_ROW_COUNT);
            assertThat(outputRowData.isNullAt(1)).isTrue();
            assertThat(outputRowData.isNullAt(2)).isTrue();
        }
    }

    /** Test the case that one of the input local runtime filters is over-max-row-count. */
    @Test
    void testOverMaxRowCountInput() throws Exception {
        try (StreamTaskMailboxTestHarness<RowData> testHarness =
                createGlobalRuntimeFilterBuilderOperatorHarness(10, 10, 4)) {
            // process elements
            testHarness.processElement(new StreamRecord<>(convertInFilterRuntimeFilter1()));
            testHarness.processElement(
                    new StreamRecord<RowData>(GenericRowData.of(OVER_MAX_ROW_COUNT, null, null)));
            testHarness.processEvent(new EndOfData(StopMode.DRAIN), 0);

            // test the output
            Queue<Object> outputs = testHarness.getOutput();
            assertThat(outputs.size()).isEqualTo(1);

            RowData outputRowData = ((StreamRecord<RowData>) outputs.poll()).getValue();
            assertThat(outputRowData.getArity()).isEqualTo(3);

            int globalCount = outputRowData.getInt(0);
            assertThat(globalCount).isEqualTo(OVER_MAX_ROW_COUNT);
            assertThat(outputRowData.isNullAt(1)).isTrue();
            assertThat(outputRowData.isNullAt(2)).isTrue();
        }
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

    private static StreamTaskMailboxTestHarness<RowData>
            createGlobalRuntimeFilterBuilderOperatorHarness(
                    int estimatedRowCount, int maxRowCount, int maxInFilterRowCount)
                    throws Exception {
        RowDataSerializer rowDataSerializer = new RowDataSerializer(new VarCharType());
        final GlobalRuntimeFilterBuilderOperator operator =
                new GlobalRuntimeFilterBuilderOperator(
                        estimatedRowCount, maxRowCount, maxInFilterRowCount, rowDataSerializer);
        return new StreamTaskMailboxTestHarnessBuilder<>(
                        OneInputStreamTask::new,
                        InternalTypeInfo.ofFields(new IntType(), new IntType(), new BinaryType()))
                .setupOutputForSingletonOperatorChain(operator)
                .addInput(InternalTypeInfo.ofFields(new IntType(), new IntType(), new BinaryType()))
                .build();
    }
}

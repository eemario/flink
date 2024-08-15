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

package org.apache.flink.incremental;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Transformation;

import javax.annotation.Nullable;

import java.util.List;

/**
 * Represents the result of planning performed by the planner. This class encapsulates the {@link
 * Transformation}s generated during the planning process, along with other information such as
 * {@link SourceOffsets} relevant to incremental processing.
 *
 * @see Transformation
 * @see SourceOffsets
 */
@Internal
public class PlanningResult {
    private final List<Transformation<?>> transformations;
    @Nullable private final SourceOffsets sourceOffsets;
    private final boolean isIncremental;

    public PlanningResult(
            List<Transformation<?>> transformations,
            @Nullable SourceOffsets sourceOffsets,
            boolean isIncremental) {
        this.transformations = transformations;
        this.sourceOffsets = sourceOffsets;
        this.isIncremental = isIncremental;
    }

    public List<Transformation<?>> getTransformations() {
        return transformations;
    }

    @Nullable
    public SourceOffsets getSourceOffsets() {
        return sourceOffsets;
    }

    public boolean isIncremental() {
        return isIncremental;
    }
}

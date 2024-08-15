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

import org.apache.flink.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Represents the offset of a source during incremental processing.
 *
 * <p>This class is used to store and retrieve the serialized payload of an offset, which is
 * specific to different sources. The serialized form allows for persistent storage and retrieval of
 * the offsets during incremental processing.
 *
 * <p>When the payload is null, it represents a special EARLIEST offset, indicating the time point
 * before all data.
 */
@PublicEvolving
public final class Offset implements Serializable {
    @Nullable private final byte[] payload;

    /**
     * Constructs a new Offset representing the EARLIEST offset. This is a special offset indicating
     * a time point before all data.
     */
    public Offset() {
        this.payload = null;
    }

    /**
     * Constructs a new Offset with the specified serialized payload.
     *
     * @param serialized The serialized payload representing the offset.
     */
    public Offset(@Nullable byte[] serialized) {
        this.payload = serialized;
    }

    /**
     * Retrieves the serialized payload of the offset.
     *
     * @return The serialized payload of the offset, or null if it represents the EARLIEST offset.
     */
    @Nullable
    public byte[] getPayload() {
        return payload;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj != null && obj.getClass() == getClass()) {
            Offset that = (Offset) obj;
            return Arrays.equals(that.payload, this.payload);
        } else {
            return false;
        }
    }
}

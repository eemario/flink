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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * This class is used to record the {@link Offset}s of table sources during the incremental
 * processing.
 */
@Internal
public class SourceOffsets implements Serializable {
    private final Map<String, Offset> sourceOffsets = new HashMap<>();

    public void setOffset(String sourceName, Offset offset) {
        sourceOffsets.put(sourceName, offset);
    }

    public Offset getOffset(String sourceName) {
        return sourceOffsets.get(sourceName);
    }

    public boolean containsOffset(String sourceName) {
        return sourceOffsets.containsKey(sourceName);
    }

    public Map<String, Offset> getOffsets() {
        return sourceOffsets;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj != null && obj.getClass() == getClass()) {
            SourceOffsets that = (SourceOffsets) obj;
            if (that.sourceOffsets.size() != this.sourceOffsets.size()) {
                return false;
            }
            for (Map.Entry<String, Offset> entry : this.sourceOffsets.entrySet()) {
                if (!that.sourceOffsets.containsKey(entry.getKey())
                        || !entry.getValue().equals(that.sourceOffsets.get(entry.getKey()))) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }
}

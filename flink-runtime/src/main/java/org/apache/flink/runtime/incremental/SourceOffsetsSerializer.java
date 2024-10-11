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

package org.apache.flink.runtime.incremental;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.incremental.SourceOffsets;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

/**
 * The serializer for {@link SourceOffsets} instance that uses Flink's {@link InstantiationUtil} for
 * serialization and deserialization.
 */
public class SourceOffsetsSerializer implements SimpleVersionedSerializer<SourceOffsets> {
    private static final int VERSION = 1;
    public static final SourceOffsetsSerializer INSTANCE = new SourceOffsetsSerializer();

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(SourceOffsets sourceOffsets) throws IOException {
        return InstantiationUtil.serializeObject(sourceOffsets);
    }

    @Override
    public SourceOffsets deserialize(int version, byte[] bytes) throws IOException {
        try {
            return InstantiationUtil.deserializeObject(bytes, ClassLoader.getSystemClassLoader());
        } catch (ClassNotFoundException exception) {
            throw new IOException("Deserialize SourceOffsets failed.", exception);
        }
    }
}

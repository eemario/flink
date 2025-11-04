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

package org.apache.flink.runtime.application;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.runtime.rest.messages.json.ApplicationResultDeserializer;
import org.apache.flink.runtime.rest.messages.json.ApplicationResultSerializer;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ApplicationResult}. */
class ApplicationResultTest {

    @Test
    void testSerializationDeserialization() throws IOException {
        final ApplicationID applicationId = ApplicationID.generate();
        final ApplicationState applicationState = ApplicationState.FINISHED;

        final ApplicationResult originalResult =
                new ApplicationResult(applicationId, applicationState);

        final ObjectMapper mapper = createObjectMapper();

        final String serialized = mapper.writeValueAsString(originalResult);
        final ApplicationResult deserializedResult =
                mapper.readValue(serialized, ApplicationResult.class);

        assertThat(deserializedResult.getApplicationId()).isEqualTo(applicationId);
        assertThat(deserializedResult.getApplicationState()).isEqualTo(applicationState);
    }

    private ObjectMapper createObjectMapper() {
        final SimpleModule module = new SimpleModule();
        module.addSerializer(ApplicationResult.class, new ApplicationResultSerializer());
        module.addDeserializer(ApplicationResult.class, new ApplicationResultDeserializer());

        final ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();
        mapper.registerModule(module);
        return mapper;
    }
}

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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.LinkedList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/** A class that holds history records of incremental processing. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class History {
    private static final String FIELD_NAME_RETAINED_HISTORY_RECORDS = "retainedHistoryRecords";

    @JsonIgnore @Nullable private Integer sizeLimit;

    @JsonProperty(FIELD_NAME_RETAINED_HISTORY_RECORDS)
    private List<HistoryRecord> retainedHistoryRecords;

    public History(int sizeLimit) {
        checkArgument(sizeLimit > 0, "Max number of history records must be greater than 0");
        this.sizeLimit = sizeLimit;
        this.retainedHistoryRecords = new LinkedList<>();
    }

    @JsonCreator
    public History(
            @JsonProperty(FIELD_NAME_RETAINED_HISTORY_RECORDS)
                    List<HistoryRecord> retainedHistoryRecords) {
        this.retainedHistoryRecords = retainedHistoryRecords;
    }

    public List<HistoryRecord> getRetainedHistoryRecords() {
        return retainedHistoryRecords;
    }

    public void setRetainedHistoryRecords(List<HistoryRecord> retainedHistoryRecords) {
        if (sizeLimit != null) {
            while (retainedHistoryRecords.size() > sizeLimit) {
                retainedHistoryRecords.remove(0);
            }
        }
        this.retainedHistoryRecords = retainedHistoryRecords;
    }

    public void addHistoryRecord(HistoryRecord historyRecord) {
        retainedHistoryRecords.add(historyRecord);
        if (sizeLimit != null && retainedHistoryRecords.size() > sizeLimit) {
            retainedHistoryRecords.remove(0);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj != null && obj.getClass() == getClass()) {
            History that = (History) obj;
            return that.retainedHistoryRecords.equals(this.retainedHistoryRecords);
        } else {
            return false;
        }
    }
}

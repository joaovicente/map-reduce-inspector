/*
 * Copyright 2016 Joao Vicente
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apm4all.hadoop;

import com.fasterxml.jackson.databind.JsonNode;

public class JobSummary {
    private JsonNode jobSummaryJsonNode;
    public JobSummary(JsonNode jobSummaryJsonNode) {
        this.jobSummaryJsonNode = jobSummaryJsonNode;
    }

    public JsonNode asJsonNode()   {
        return jobSummaryJsonNode;
    }

    public String toString()   {
        return jobSummaryJsonNode.toString();
    }

    public String getValue(Key key)    {
        return jobSummaryJsonNode.path(key.toString()).textValue();
    }

    public enum Key {
        ID("id"),
        NAME("name"),
        QUEUE("queue"),
        USER("user"),
        STATE("state");

        private final String customName;
        Key(String name) { this.customName = name; }
        @Override public String toString() {return customName;}
    }
}

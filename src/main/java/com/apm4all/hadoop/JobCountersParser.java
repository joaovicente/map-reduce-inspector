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
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class JobCountersParser {
    String jsonString;

    JobCountersParser(String json)  {
        this.jsonString = json;
    }

    public JobCounters parse() throws IOException {
        JobCounters jobCounters = new JobCounters();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readValue(jsonString.getBytes(), JsonNode.class);

        for (JsonNode counterGroups : jsonNode.path("jobCounters").path("counterGroup")) {
            String groupName = counterGroups.path("counterGroupName").textValue();
            for (JsonNode counter : counterGroups.path("counter")) {
                jobCounters.addCounterValue(
                        groupName, "MAP", counter.path("name").textValue(),
                        counter.get("mapCounterValue").asLong());
                jobCounters.addCounterValue(
                        groupName, "REDUCE", counter.path("name").textValue(),
                        counter.get("reduceCounterValue").asLong());
                jobCounters.addCounterValue(
                        groupName, "TOTAL", counter.path("name").textValue(),
                        counter.get("totalCounterValue").asLong());
            }
        }
        return jobCounters;
    }
}

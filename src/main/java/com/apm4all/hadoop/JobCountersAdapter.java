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

import com.apm4all.hadoop.JobCounters.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.regex.Pattern;

public class JobCountersAdapter {
    private final ObjectNode rootNode;
    private boolean prettyPrinted;

    public static class Builder {
        // Required parameter
        private final JobCounters jobCounters;
        // Optional parameter
        private boolean prettyPrinted = false;
        // Adapted output (JSON tree)
        private ObjectMapper mapper;
        private ObjectNode rootNode;

        public Builder(JobCounters jobCounters) {
            this.jobCounters = jobCounters;
            this.mapper = new ObjectMapper();
            this.rootNode = mapper.createObjectNode();
        }

        public Builder prettyPrint()    {
            this.prettyPrinted = true;
            return this;
        }

        public Builder with(String group, String category, String name)   {

            try {
                rootNode
                        .with(JobCounters.groupToStringReplaceDots(group))
                        .with(category)
                        .put(name, jobCounters.getCounterValue(group, category, name));
            } catch (NoSuchElementException e) {
                // Not all counters will be available for all jobs
                // e.g. SHUFFLED_MAPS will not be available on jobs which do not require reducers
                // So, if counters are not available, they will simply not being populated
            }

            return this;
        }

        public Builder with(JobCounters.Group group, JobCounters.Category category, JobCounters.Name name)   {

            try {
                rootNode
                        .with(group.toStringReplaceDots())
                        .with(category.toString())
                        .put(name.toString(), jobCounters.getCounterValue(group, category, name));
            } catch (NoSuchElementException e) {
                // Not all counters will be available for all jobs
                // e.g. SHUFFLED_MAPS will not be available on jobs which do not require reducers
                // So, if counters are not available, they will simply not being populated
            }

            return this;
        }

        public Builder withTotalsOfGroupsMatching(String regexPattern) {
            // Return all TOTAL counters for which the group matches the provided regexPattern
            Set<String> groupNames = jobCounters.getGroupNames();
            Set<String> matchingGroupNames =new HashSet<String>();
            String categoryName = Category.TOTAL.toString();
            Pattern pattern = Pattern.compile(regexPattern);

            // Iterate through groups for matches
            for (String groupName : groupNames) {
                Set<String> counterNames = jobCounters.getCounterNames(groupName, categoryName);
                if (pattern.matches(regexPattern, groupName)) {
                    for (String counterName : counterNames) {
                        // Iterate through TOTAL children appending each one found
                        rootNode
                                .with(JobCounters.groupToStringReplaceDots(groupName))
                                .with(Category.TOTAL.toString())
                                .put(counterName, jobCounters.getCounterValue(groupName, categoryName, counterName));
                    }
                }
            }
            return this;
        }

        public JobCountersAdapter build()   {
            return new JobCountersAdapter(this);
        }

    }

    public JobCountersAdapter(Builder builder) {
        rootNode = builder.rootNode;
        prettyPrinted = builder.prettyPrinted;
    }

    public ObjectNode asObjectNode()    {
        return rootNode;
    }

    public JsonNode asJsonNode()    {
        JsonNode jsonNode = null;
        try {
            jsonNode = new ObjectMapper().readTree(rootNode.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return jsonNode;
    }

    public String asString()    {
        String output = null;
        if (prettyPrinted) {
            ObjectMapper mapper = new ObjectMapper();
            mapper.enable(SerializationFeature.INDENT_OUTPUT);
            ObjectWriter writer = mapper.writer().withDefaultPrettyPrinter();
            try {
                output = writer.writeValueAsString(rootNode);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
        else {
            output = rootNode.toString();
        }
        return output;
    }
}

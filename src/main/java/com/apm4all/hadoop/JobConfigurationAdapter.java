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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashSet;

public class JobConfigurationAdapter {
    private final ObjectNode rootNode;
    private boolean prettyPrinted;

    public static class Builder {
        // Required parameter
        private final JobConfiguration jobConfiguration;
        // Optional parameter
        private boolean prettyPrinted = false;
        // Others
        private final ObjectMapper mapper;
        private final ObjectNode rootNode;
        private final HashSet<String> includeKeys;

        public Builder(JobConfiguration jobConfiguration) {
            this.jobConfiguration = jobConfiguration;
            this.mapper = new ObjectMapper();
            this.rootNode = mapper.createObjectNode();
            this.includeKeys = new HashSet<>();
        }

        public Builder prettyPrint()    {
            this.prettyPrinted = true;
            return this;
        }

        public Builder includeKeyPattern(String regexPattern)    {
            // TODO: implement pattern filtering
            return this;
        }

        public Builder includeKey(String keyName)    {
            includeKeys.add(keyName);
            return this;
        }

        private boolean allow(String key) {
            return (includeKeys.contains(key) || includeKeys.isEmpty());
        }

        public JobConfigurationAdapter build()   {
            for (String key : jobConfiguration.getKeys()) {
                if(allow(key)) {
                    rootNode.put(key, jobConfiguration.get(key));
                }
            }
            return new JobConfigurationAdapter(this);
        }
    }

    private JobConfigurationAdapter(Builder builder) {
        rootNode = builder.rootNode;
        prettyPrinted = builder.prettyPrinted;
    }

    public JsonNode asJsonNode()    {
        return rootNode;
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

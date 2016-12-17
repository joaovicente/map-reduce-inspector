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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JobConfigurationTest {
    @Test
    public void parsedFromFile() throws Exception {
        JobConfigurationFileCollector collector = new JobConfigurationFileCollector(Constants.TEST_JOB_ID);
        String jsonString = collector.getJsonString();
        JobConfigurationParser  parser = new JobConfigurationParser(jsonString);
        JobConfiguration jobConfiguration = parser.parse();

        assertEquals("1.12.37.33", jobConfiguration.get("cascading.app.version"));
    }
}
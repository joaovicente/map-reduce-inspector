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
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class JobSummaryAdapterTest {
    JobSummary jobSummary;

    @Before
    public void setUp() throws Exception {
        JobSummaryFileCollector collector = new JobSummaryFileCollector(Constants.TEST_JOB_ID);
        String jsonString = collector.getJsonString();
        JobSummaryParser jobSummaryParser = new JobSummaryParser(jsonString);
        jobSummary = jobSummaryParser.parse();
    }

    @Test
    public void standardUsage() throws Exception {
        JobSummaryAdapter adapter = new JobSummaryAdapter.Builder(jobSummary)
                .prettyPrint()
                .build();

        JsonNode jsonNode = adapter.asJsonNode();

        assertEquals(1480046730687L, jsonNode
                .path("submitTime")
                .asLong());

//        System.out.println("### Job Summary (adapted) ###");
//        System.out.print(adapter.asString());
    }

    @Test
    public void asString_prettyPrinted() throws Exception {
        JobSummaryAdapter adapter = new JobSummaryAdapter.Builder(jobSummary)
                .prettyPrint()
                .build();

        assertNotEquals(0, adapter.asString().length());
        assertTrue(adapter.asString().toString().contains("\n"));
    }

    @Test
    public void asString_notPrettyPrinted() throws Exception {
        JobSummaryAdapter adapter = new JobSummaryAdapter.Builder(jobSummary)
//                .prettyPrint()
                .build();

        assertNotEquals(0, adapter.asString().toString().length());
        assertFalse(adapter.asString().contains("\n"));
    }
}

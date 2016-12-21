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

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JobSummaryTest {
    JobSummary jobSummary;

    @Before
    public void setUp() throws Exception {
        JobSummaryFileCollector collector = new JobSummaryFileCollector(Constants.TEST_JOB_ID);
        String jsonString = collector.getJsonString();
        JobSummaryParser  parser = new JobSummaryParser(jsonString);
        jobSummary = parser.parse();
    }

    @Test
    public void parsedFromFile() throws Exception {
        assertTrue(jobSummary.toString().length() > 0);
    }

    @Test
    public void getValues() {
        assertEquals("[04C96799FAB5430C8049F9FAA2AD32BE/4384509E1D674BCFB1F1EEA857CC6059] MY_FLOW(1/5)", jobSummary.getValue(JobSummary.Key.NAME));
        assertEquals("job_1478805790803_8364", jobSummary.getValue(JobSummary.Key.ID));
        assertEquals("root.default", jobSummary.getValue(JobSummary.Key.QUEUE));
        assertEquals("apm4all", jobSummary.getValue(JobSummary.Key.USER));
        assertEquals("SUCCEEDED", jobSummary.getValue(JobSummary.Key.STATE));
    }
}
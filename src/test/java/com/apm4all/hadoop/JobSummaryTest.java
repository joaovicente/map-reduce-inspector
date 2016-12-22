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
        assertEquals(1480046730687L, jobSummary.getSubmitTime());
        assertEquals(1480046735503L, jobSummary.getStartTime());
        assertEquals(1480057717790L, jobSummary.getFinishTime());
        assertEquals("job_1478805790803_8364", jobSummary.getId());
        assertEquals("[04C96799FAB5430C8049F9FAA2AD32BE/4384509E1D674BCFB1F1EEA857CC6059] MY_FLOW(1/5)", jobSummary.getName());
        assertEquals("root.default", jobSummary.getQueue());
        assertEquals("apm4all", jobSummary.getUser());
        assertEquals("SUCCEEDED", jobSummary.getState());
        assertEquals(8713, jobSummary.getMapsTotal());
        assertEquals(8713, jobSummary.getMapsCompleted());
        assertEquals(368, jobSummary.getReducesTotal());
        assertEquals(368, jobSummary.getReducesCompleted());
        assertEquals(false, jobSummary.getUberized());
        assertEquals("", jobSummary.getDiagnostics());
        assertEquals(713761, jobSummary.getAvgMapTime());
        assertEquals(2345178, jobSummary.getAvgReduceTime());
        assertEquals(546189, jobSummary.getAvgShuffleTime());
        assertEquals(7965, jobSummary.getAvgMergeTime());
        assertEquals(0, jobSummary.getFailedReduceAttempts());
        assertEquals(0, jobSummary.getKilledReduceAttempts());
        assertEquals(368, jobSummary.getSuccessfulReduceAttempts());
        assertEquals(2, jobSummary.getFailedMapAttempts());
        assertEquals(0, jobSummary.getKilledMapAttempts());
        assertEquals(8713, jobSummary.getSuccessfulMapAttempts());
    }
}
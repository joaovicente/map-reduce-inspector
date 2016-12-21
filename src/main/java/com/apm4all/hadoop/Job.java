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


public class Job {
    private JobSummary jobSummary;
    private JobConfiguration jobConfiguration;
    private JobCounters jobCounters;

    public Job(String jobHistoryUrl, String jobId) {
        JobSummaryUrlReader summaryReader = new JobSummaryUrlReader(jobHistoryUrl, jobId);
        parseSummary(summaryReader.getJsonString());
        JobCountersUrlReader countersReader = new JobCountersUrlReader(jobHistoryUrl, jobId);
        parseCounters(countersReader.getJsonString());
        JobConfigurationUrlReader configurationReader = new JobConfigurationUrlReader(jobHistoryUrl, jobId);
        parseConfiguration(configurationReader.getJsonString());
    }

    public Job()   {
        // Only used for testing
        JobSummaryFileCollector summaryCollector = new JobSummaryFileCollector(Constants.TEST_JOB_ID);
        parseSummary(summaryCollector.getJsonString());
        JobCountersFileCollector countersCollector = new JobCountersFileCollector(Constants.TEST_JOB_ID);
        parseCounters(countersCollector.getJsonString());
        JobConfigurationFileCollector configurationCollector = new JobConfigurationFileCollector(Constants.TEST_JOB_ID);
        parseConfiguration(configurationCollector.getJsonString());
    }

    private void parseConfiguration(String jsonString) {
        JobConfigurationParser jobConfigurationParser = new JobConfigurationParser(jsonString);
        jobConfiguration = jobConfigurationParser.parse();
    }

    private void parseCounters(String jsonString) {
        JobCountersParser jobCountersParser = new JobCountersParser(jsonString);
        jobCounters = jobCountersParser.parse();
    }

    private void parseSummary(String jsonString) {
        JobSummaryParser jobSummaryParser = new JobSummaryParser(jsonString);
        jobSummary = jobSummaryParser.parse();
    }

    public JobSummary getSummary() {
        return jobSummary;
    }

    public JobCounters getCounters() {
        return jobCounters;
    }

    public JobConfiguration getConfiguration() {
        return jobConfiguration;
    }
}

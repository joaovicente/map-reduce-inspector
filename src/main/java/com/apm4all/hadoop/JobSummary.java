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

    public long getSubmitTime() { return jobSummaryJsonNode.path(Key.SUBMIT_TIME.toString()).longValue(); }
    public long getStartTime() { return jobSummaryJsonNode.path(Key.START_TIME.toString()).longValue(); }
    public long getFinishTime() { return jobSummaryJsonNode.path(Key.FINISH_TIME.toString()).longValue(); }
    public String getId() { return jobSummaryJsonNode.path(Key.ID.toString()).textValue(); }
    public String getName() { return jobSummaryJsonNode.path(Key.NAME.toString()).textValue(); }
    public String getQueue()  {
        return jobSummaryJsonNode.path(Key.QUEUE.toString()).textValue();
    }
    public String getUser()   {
        return jobSummaryJsonNode.path(Key.USER.toString()).textValue();
    }
    public String getState()   {return jobSummaryJsonNode.path(Key.STATE.toString()).textValue(); }
    public int getMapsTotal() {return jobSummaryJsonNode.path(Key.MAPS_TOTAL.toString()).intValue(); }
    public int getMapsCompleted() {return jobSummaryJsonNode.path(Key.MAPS_COMPLETED.toString()).intValue(); }
    public int getReducesTotal() {return jobSummaryJsonNode.path(Key.REDUCES_TOTAL.toString()).intValue(); }
    public int getReducesCompleted() {return jobSummaryJsonNode.path(Key.REDUCES_COMPLETED.toString()).intValue(); }
    public boolean getUberized() {return jobSummaryJsonNode.path(Key.UBERIZED.toString()).booleanValue(); }
    public String getDiagnostics() {return jobSummaryJsonNode.path(Key.DIAGNOSTICS.toString()).textValue(); }
    public int getAvgMapTime() {return jobSummaryJsonNode.path(Key.AVG_MAP_TIME.toString()).intValue(); }
    public int getAvgReduceTime() {return jobSummaryJsonNode.path(Key.AVG_REDUCE_TIME.toString()).intValue(); }
    public int getAvgShuffleTime() {return jobSummaryJsonNode.path(Key.AVG_SHUFFLE_TIME.toString()).intValue(); }
    public int getAvgMergeTime() {return jobSummaryJsonNode.path(Key.AVG_MERGE_TIME.toString()).intValue(); }
    public int getFailedReduceAttempts() {return jobSummaryJsonNode.path(Key.FAILED_REDUCE_ATTEMPTS.toString()).intValue(); }
    public int getKilledReduceAttempts() {return jobSummaryJsonNode.path(Key.KILLED_REDUCE_ATTEMPTS.toString()).intValue(); }
    public int getSuccessfulReduceAttempts() {return jobSummaryJsonNode.path(Key.SUCCESSFUL_REDUCE_ATTEMPTS.toString()).intValue(); }
    public int getFailedMapAttempts() {return jobSummaryJsonNode.path(Key.FAILED_MAP_ATTEMPTS.toString()).intValue(); }
    public int getKilledMapAttempts() {return jobSummaryJsonNode.path(Key.KILLED_MAP_ATTEMPTS.toString()).intValue(); }
    public int getSuccessfulMapAttempts() {return jobSummaryJsonNode.path(Key.SUCCESSFUL_MAP_ATTEMPTS.toString()).intValue(); }

    public enum Key {
        SUBMIT_TIME("submitTime"),
        START_TIME("startTime"),
        FINISH_TIME("finishTime"),
        ID("id"),
        NAME("name"),
        QUEUE("queue"),
        USER("user"),
        STATE("state"),
        MAPS_TOTAL("mapsTotal"),
        MAPS_COMPLETED("mapsCompleted"),
        REDUCES_TOTAL("reducesTotal"),
        REDUCES_COMPLETED("reducesCompleted"),
        UBERIZED("uberized"),
        DIAGNOSTICS("diagnostics"),
        AVG_MAP_TIME("avgMapTime"),
        AVG_REDUCE_TIME("avgReduceTime"),
        AVG_SHUFFLE_TIME("avgShuffleTime"),
        AVG_MERGE_TIME("avgMergeTime"),
        FAILED_REDUCE_ATTEMPTS("failedReduceAttempts"),
        KILLED_REDUCE_ATTEMPTS("killedReduceAttempts"),
        SUCCESSFUL_REDUCE_ATTEMPTS("successfulReduceAttempts"),
        FAILED_MAP_ATTEMPTS("failedMapAttempts"),
        KILLED_MAP_ATTEMPTS("killedMapAttempts"),
        SUCCESSFUL_MAP_ATTEMPTS("successfulMapAttempts");

        private final String customName;
        Key(String name) { this.customName = name; }
        @Override public String toString() {return customName;}
    }
}

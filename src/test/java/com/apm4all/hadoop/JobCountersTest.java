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
import com.apm4all.hadoop.JobCounters.*;
import java.util.NoSuchElementException;
import java.util.Set;
import static org.junit.Assert.*;

public class JobCountersTest {
    private static final String TEST_COUNTER = "TEST_COUNTER";
    private static final String TEST_CATEGORY = "TEST_CATEGORY";
    private static final String TEST_GROUP = "TEST_GROUP";
    JobCounters jobCounters;

    @Before
    public void setUp() throws Exception {
        jobCounters = new JobCounters();
    }

    @Test
    public void getCounterValueFromFile() throws Exception {
        JobCountersFileCollector collector = new JobCountersFileCollector(Constants.TEST_JOB_ID);
        String jsonString = collector.getJsonString();
        JobCountersParser jobCountersParser = new JobCountersParser(jsonString);
        jobCounters = jobCountersParser.parse();

        assertEquals(7520365647763L,
                jobCounters.getCounterValue(
                        "org.apache.hadoop.mapreduce.FileSystemCounter", "MAP", "FILE_BYTES_READ"
                ));

        assertEquals(7520365647763L,
                jobCounters.getCounterValue(
                        Group.ORG_APACHE_HADOOP_MAPREDUCE_FILESYSTEMCOUNTER, Category.MAP, Name.FILE_BYTES_READ
                ));

        assertEquals(7416486354412L,
                jobCounters.getCounterValue(
                        Group.ORG_APACHE_HADOOP_MAPREDUCE_FILESYSTEMCOUNTER, Category.REDUCE, Name.FILE_BYTES_READ
                ));
        assertEquals(14936852002175L,
                jobCounters.getCounterValue(Group.ORG_APACHE_HADOOP_MAPREDUCE_FILESYSTEMCOUNTER, Category.TOTAL, Name.FILE_BYTES_READ
                ));

        assertEquals(0L,
                jobCounters.getCounterValue(
                        Group.ORG_APACHE_HADOOP_MAPREDUCE_JOBCOUNTER, Category.MAP, Name.NUM_FAILED_MAPS
                ));

        assertEquals(0L,
                jobCounters.getCounterValue(
                        Group.ORG_APACHE_HADOOP_MAPREDUCE_JOBCOUNTER, Category.REDUCE, Name.NUM_FAILED_MAPS
                ));
        assertEquals(2L,
                jobCounters.getCounterValue(
                        Group.ORG_APACHE_HADOOP_MAPREDUCE_JOBCOUNTER, Category.TOTAL, Name.NUM_FAILED_MAPS
                ));

        assertEquals(59888915216L,
                jobCounters.getCounterValue(
                        Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.MAP, Name.MAP_INPUT_RECORDS
                ));

        assertEquals(0L,
                jobCounters.getCounterValue(
                        Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.REDUCE, Name.MAP_INPUT_RECORDS
                ));
        assertEquals(59888915216L,
                jobCounters.getCounterValue(
                        Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.TOTAL, Name.MAP_INPUT_RECORDS
                ));

        assertEquals(0L,
                jobCounters.getCounterValue(
                        Group.SHUFFLE_ERRORS, Category.MAP, Name.BAD_ID
                ));

        assertEquals(0L,
                jobCounters.getCounterValue(
                        Group.SHUFFLE_ERRORS, Category.REDUCE, Name.BAD_ID
                ));
        assertEquals(0L,
                jobCounters.getCounterValue(
                        Group.SHUFFLE_ERRORS, Category.TOTAL, Name.BAD_ID
                ));

        assertEquals(12895686969101602L,
                jobCounters.getCounterValue(
                        Group.CASCADING_FLOW_SLICECOUNTERS, Category.MAP, Name.PROCESS_BEGIN_TIME
                ));
        assertEquals(544659921458939L,
                jobCounters.getCounterValue(
                        Group.CASCADING_FLOW_SLICECOUNTERS, Category.REDUCE, Name.PROCESS_BEGIN_TIME
                ));
        assertEquals(13440346890560541L,
                jobCounters.getCounterValue(
                        Group.CASCADING_FLOW_SLICECOUNTERS, Category.TOTAL, Name.PROCESS_BEGIN_TIME
                ));

        assertEquals(59888915216L,
                jobCounters.getCounterValue(
                        Group.CASCADING_FLOW_STEPCOUNTERS, Category.MAP, Name.TUPLES_READ
                ));
        assertEquals(0L,
                jobCounters.getCounterValue(
                        Group.CASCADING_FLOW_STEPCOUNTERS, Category.REDUCE, Name.TUPLES_READ
                ));
        assertEquals(59888915216L,
                jobCounters.getCounterValue(
                        Group.CASCADING_FLOW_STEPCOUNTERS, Category.TOTAL, Name.TUPLES_READ
                ));

        assertEquals(0L,
                jobCounters.getCounterValue(
                        Group.ORG_APACHE_HADOOP_MAPREDUCE_LIB_INPUT_FILEINPUTFORMATCOUNTER, Category.MAP, Name.BYTES_READ
                ));
        assertEquals(0L,
                jobCounters.getCounterValue(
                        Group.ORG_APACHE_HADOOP_MAPREDUCE_LIB_INPUT_FILEINPUTFORMATCOUNTER, Category.REDUCE, Name.BYTES_READ
                ));
        assertEquals(0L,
                jobCounters.getCounterValue(
                        Group.ORG_APACHE_HADOOP_MAPREDUCE_LIB_INPUT_FILEINPUTFORMATCOUNTER, Category.TOTAL, Name.BYTES_READ
                ));


        assertEquals(0L,
                jobCounters.getCounterValue(
                        Group.ORG_APACHE_HADOOP_MAPREDUCE_LIB_OUTPUT_FILEOUTPUTFORMATCOUNTER, Category.MAP, Name.BYTES_WRITTEN
                ));

        assertEquals(55406440319L,
                jobCounters.getCounterValue(
                        Group.ORG_APACHE_HADOOP_MAPREDUCE_LIB_OUTPUT_FILEOUTPUTFORMATCOUNTER, Category.REDUCE, Name.BYTES_WRITTEN
                ));
        assertEquals(55406440319L,
                jobCounters.getCounterValue(
                        Group.ORG_APACHE_HADOOP_MAPREDUCE_LIB_OUTPUT_FILEOUTPUTFORMATCOUNTER, Category.TOTAL, Name.BYTES_WRITTEN
                ));
    }

    @Test
    public void getCounterValue_usingStrings() throws Exception {
        jobCounters.addCounterValue(TEST_GROUP, TEST_CATEGORY, TEST_COUNTER, Long.MAX_VALUE);
        assertEquals(Long.MAX_VALUE, jobCounters.getCounterValue(TEST_GROUP, TEST_CATEGORY, TEST_COUNTER));
    }

    @Test
    public void getCounterValue_severalDistributedCounters() throws Exception {
        jobCounters.addCounterValue("GRPA", "MAP", "CNT1", 1L);
        jobCounters.addCounterValue("GRPA", "REDUCE", "CNT1", 2L);
        jobCounters.addCounterValue("GRPA", "TOTAL", "CNT1", 3L);
        jobCounters.addCounterValue("GRPB", "MAP", "CNT2", 4L);
        jobCounters.addCounterValue("GRPB", "REDUCE", "CNT2", 5L);
        jobCounters.addCounterValue("GRPB", "TOTAL", "CNT2", 6L);
        jobCounters.addCounterValue("GRPA", "MAP", "CNT3", 1L);
        jobCounters.addCounterValue("GRPA", "REDUCE", "CNT3", 2L);
        jobCounters.addCounterValue("GRPA", "TOTAL", "CNT3", 3L);
        jobCounters.addCounterValue("GRPB", "MAP", "CNT4", 4L);
        jobCounters.addCounterValue("GRPB", "REDUCE", "CNT4", 5L);
        jobCounters.addCounterValue("GRPB", "TOTAL", "CNT4", 6L);

        assertEquals(1L, jobCounters.getCounterValue("GRPA", "MAP", "CNT1"));
        assertEquals(2L, jobCounters.getCounterValue("GRPA", "REDUCE", "CNT1"));
        assertEquals(3L, jobCounters.getCounterValue("GRPA", "TOTAL", "CNT1"));
        assertEquals(4L, jobCounters.getCounterValue("GRPB", "MAP", "CNT2"));
        assertEquals(5L, jobCounters.getCounterValue("GRPB", "REDUCE", "CNT2"));
        assertEquals(6L, jobCounters.getCounterValue("GRPB", "TOTAL", "CNT2"));
        assertEquals(1L, jobCounters.getCounterValue("GRPA", "MAP", "CNT3"));
        assertEquals(2L, jobCounters.getCounterValue("GRPA", "REDUCE", "CNT3"));
        assertEquals(3L, jobCounters.getCounterValue("GRPA", "TOTAL", "CNT3"));
        assertEquals(4L, jobCounters.getCounterValue("GRPB", "MAP", "CNT4"));
        assertEquals(5L, jobCounters.getCounterValue("GRPB", "REDUCE", "CNT4"));
        assertEquals(6L, jobCounters.getCounterValue("GRPB", "TOTAL", "CNT4"));
    }

    @Test
    public void getGroupNames() {
        jobCounters.addCounterValue("GRPA", "MAP", "CNT1", 1L);
        jobCounters.addCounterValue("GRPB", "MAP", "CNT1", 1L);
        Set<String> groupNames = jobCounters.getGroupNames();
        assertEquals(2, groupNames.size());
        assertTrue("GRPA", groupNames.contains("GRPA"));
        assertTrue("GRPB", groupNames.contains("GRPB"));
    }

    @Test
    public void getCounterNames_invalidgroupNotFound() {
        assertEquals(0, jobCounters.getCounterNames("N/A","N/A").size());
    }

    @Test
    public void getCounterNames() {
        jobCounters.addCounterValue("GRPA", "TOTAL", "CNT1", 1L);
        jobCounters.addCounterValue("GRPA", "TOTAL", "CNT2", 2L);
        jobCounters.addCounterValue("GRPA", "MAP", "CNT3", 3L);
        Set<String> counterNames = jobCounters.getCounterNames("GRPA", "TOTAL");
        assertEquals(2, counterNames.size());
        assertTrue("GRPA", counterNames.contains("CNT1"));
        assertTrue("GRPA", counterNames.contains("CNT2"));
    }

    @Test(expected=NoSuchElementException.class)
    public void getCounterValue_groupNotFound() {
        long counter = jobCounters.getCounterValue("GRPA", "MAP", "CNT1");
        System.out.println(counter);
    }

    @Test(expected=NoSuchElementException.class)
    public void getCounterValue_categoryNotFound() {
        jobCounters.addCounterValue("GRPA", "NULL", "CNT1", 1L);
        long counter = jobCounters.getCounterValue("GRPA", "MAP", "CNT1");
        System.out.println(counter);
    }

    @Test(expected=NoSuchElementException.class)
    public void getCounterValue_counterNotFound() {
        jobCounters.addCounterValue("GRPA", "MAP", "CNT2", 1L);
        long counter = jobCounters.getCounterValue("GRPA", "MAP", "CNT1");
        System.out.println(counter);
    }
}
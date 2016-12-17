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

public class JobConfigurationAdapterTest {
    JobConfiguration jobConfiguration;

    @Before
    public void setUp() throws Exception {
        JobConfigurationFileCollector collector = new JobConfigurationFileCollector(Constants.TEST_JOB_ID);
        String jsonString = collector.getJsonString();
        JobConfigurationParser jobConfigurationParser = new JobConfigurationParser(jsonString);
        jobConfiguration = jobConfigurationParser.parse();
    }

    @Test
    public void standardUsage() throws Exception {
        JobConfigurationAdapter adapter = new JobConfigurationAdapter.Builder(jobConfiguration)
                .prettyPrint()
                .includeKey("mapreduce.input.fileinputformat.split.minsize")
                .includeKey("mapreduce.map.memory.mb")
                .includeKey("mapreduce.task.io.sort.mb")
                .includeKey("mapreduce.job.reduces")
                .includeKey("mapreduce.job.reduce.slowstart.completedmaps")
                .includeKey("hbase.client.scanner.caching")
                .includeKey("hbase.client.prefetch.limit")
                .includeKey("hbase.client.write.buffer")
                .includeKey("hbase.regionserver.handler.count")
                .includeKey("hbase.hregion.memstore.flush.size")
                .includeKey("mapreduce.reduce.merge.inmem.threshold")
                .includeKey("mapreduce.reduce.shuffle.input.buffer.percent")
                .includeKey("mapreduce.reduce.cpu.vcores")
                .includeKey("mapreduce.map.java.opts")
                .includeKey("mapreduce.reduce.memory.mb")
                .includeKey("mapreduce.reduce.java.opts")
                .includeKey("hbase.client.scanner.max.result.size")
                .includeKey("mapreduce.reduce.shuffle.memory.limit.percent")
                .build();

        JsonNode jsonNode = adapter.asJsonNode();

        assertEquals( "268435456", jsonNode
                .path("hbase.hregion.memstore.flush.size")
                .asText());

        System.out.println();
        System.out.println("### Job Configuration (adapted) ###");
        System.out.println(adapter.asString());
    }

    @Test
    public void asString_prettyPrinted() throws Exception {
        JobConfigurationAdapter adapter = new JobConfigurationAdapter.Builder(jobConfiguration)
                .prettyPrint()
                .build();

        assertNotEquals(0, adapter.asString().length());
        assertTrue(adapter.asString().toString().contains("\n"));
    }

    @Test
    public void asString_notPrettyPrinted() throws Exception {
        JobConfigurationAdapter adapter = new JobConfigurationAdapter.Builder(jobConfiguration)
//                .prettyPrint()
                .build();

        assertNotEquals(0, adapter.asString().toString().length());
        assertFalse(adapter.asString().contains("\n"));
    }
}

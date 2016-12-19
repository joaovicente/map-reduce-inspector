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

import com.apm4all.hadoop.JobCounters.*;


public class JobTest {

    @Test
    public void jobAdaptation()  {
        Job job = new Job();
        JobSummaryAdapter summaryAdapter = new JobSummaryAdapter.Builder(job.getSummary())
                .build();

        JobCountersAdapter countersAdapter = new JobCountersAdapter.Builder(job.getCounters())
                .with(Group.CASCADING_FLOW_STEPCOUNTERS, Category.MAP, Name.TUPLES_READ)
                .with(Group.CASCADING_FLOW_STEPCOUNTERS, Category.REDUCE, Name.TUPLES_WRITTEN)
                .with(Group.CASCADING_FLOW_SLICECOUNTERS, Category.MAP, Name.TUPLES_READ)
                .with(Group.CASCADING_FLOW_SLICECOUNTERS, Category.MAP, Name.TUPLES_WRITTEN)
                .with(Group.CASCADING_FLOW_SLICECOUNTERS, Category.REDUCE, Name.TUPLES_READ)
                .with(Group.CASCADING_FLOW_SLICECOUNTERS, Category.REDUCE, Name.TUPLES_WRITTEN)
                .with(Group.CASCADING_FLOW_SLICECOUNTERS, Category.TOTAL, Name.TUPLES_READ)
                .with(Group.CASCADING_FLOW_SLICECOUNTERS, Category.TOTAL, Name.TUPLES_WRITTEN)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_FILESYSTEMCOUNTER, Category.MAP, Name.FILE_BYTES_READ)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_FILESYSTEMCOUNTER, Category.MAP, Name.FILE_BYTES_WRITTEN)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_FILESYSTEMCOUNTER, Category.MAP, Name.HDFS_BYTES_READ)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_FILESYSTEMCOUNTER, Category.REDUCE, Name.FILE_BYTES_READ)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_FILESYSTEMCOUNTER, Category.REDUCE, Name.FILE_BYTES_WRITTEN)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_FILESYSTEMCOUNTER, Category.REDUCE, Name.HDFS_BYTES_WRITTEN)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_FILESYSTEMCOUNTER, Category.TOTAL, Name.FILE_BYTES_READ)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_FILESYSTEMCOUNTER, Category.TOTAL, Name.FILE_BYTES_WRITTEN)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_FILESYSTEMCOUNTER, Category.TOTAL, Name.HDFS_BYTES_READ)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_FILESYSTEMCOUNTER, Category.TOTAL, Name.HDFS_BYTES_WRITTEN)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.MAP, Name.MAP_INPUT_RECORDS)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.MAP, Name.MAP_OUTPUT_RECORDS)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.MAP, Name.FAILED_SHUFFLE)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.MAP, Name.CPU_MILLISECONDS)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.MAP, Name.PHYSICAL_MEMORY_BYTES)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.MAP, Name.VIRTUAL_MEMORY_BYTES)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.MAP, Name.COMMITTED_HEAP_BYTES)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.MAP, Name.GC_TIME_MILLIS)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.MAP, Name.SPILLED_RECORDS)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.REDUCE, Name.SHUFFLED_MAPS)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.REDUCE, Name.REDUCE_INPUT_RECORDS)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.REDUCE, Name.REDUCE_OUTPUT_RECORDS)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.REDUCE, Name.MERGED_MAP_OUTPUTS)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.REDUCE, Name.FAILED_SHUFFLE)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.REDUCE, Name.CPU_MILLISECONDS)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.REDUCE, Name.PHYSICAL_MEMORY_BYTES)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.REDUCE, Name.VIRTUAL_MEMORY_BYTES)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.REDUCE, Name.COMMITTED_HEAP_BYTES)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.REDUCE, Name.GC_TIME_MILLIS)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.REDUCE, Name.SPILLED_RECORDS)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.REDUCE, Name.REDUCE_INPUT_GROUPS)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.REDUCE, Name.REDUCE_INPUT_RECORDS)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.REDUCE, Name.REDUCE_OUTPUT_RECORDS)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.TOTAL, Name.CPU_MILLISECONDS)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.TOTAL, Name.PHYSICAL_MEMORY_BYTES)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.TOTAL, Name.VIRTUAL_MEMORY_BYTES)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.TOTAL, Name.COMMITTED_HEAP_BYTES)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.TOTAL, Name.GC_TIME_MILLIS)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER, Category.TOTAL, Name.SPILLED_RECORDS)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_JOBCOUNTER, Category.TOTAL, Name.DATA_LOCAL_MAPS)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_JOBCOUNTER, Category.TOTAL, Name.TOTAL_LAUNCHED_MAPS)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_JOBCOUNTER, Category.TOTAL, Name.TOTAL_LAUNCHED_REDUCES)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_JOBCOUNTER, Category.TOTAL, Name.RACK_LOCAL_MAPS)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_JOBCOUNTER, Category.TOTAL, Name.MILLIS_MAPS)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_JOBCOUNTER, Category.TOTAL, Name.TOTAL_LAUNCHED_MAPS)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_JOBCOUNTER, Category.TOTAL, Name.MB_MILLIS_MAPS)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_JOBCOUNTER, Category.TOTAL, Name.VCORES_MILLIS_MAPS )
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_JOBCOUNTER, Category.TOTAL, Name.MILLIS_REDUCES)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_JOBCOUNTER, Category.TOTAL, Name.TOTAL_LAUNCHED_REDUCES)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_JOBCOUNTER, Category.TOTAL, Name.MB_MILLIS_REDUCES)
                .with(Group.ORG_APACHE_HADOOP_MAPREDUCE_JOBCOUNTER, Category.TOTAL, Name.VCORES_MILLIS_REDUCES)
                .with(Group.SHUFFLE_ERRORS, Category.TOTAL, Name.BAD_ID)
                .with(Group.SHUFFLE_ERRORS, Category.TOTAL, Name.CONNECTION)
                .with(Group.SHUFFLE_ERRORS, Category.TOTAL, Name.IO_ERROR)
                .with(Group.SHUFFLE_ERRORS, Category.TOTAL, Name.WRONG_LENGTH)
                .with(Group.SHUFFLE_ERRORS, Category.TOTAL, Name.WRONG_MAP)
                .with(Group.SHUFFLE_ERRORS, Category.TOTAL, Name.WRONG_REDUCE)
                // Capture any counter of groups matching "com.apm4all.hadoop.*" or "com.apm4all.someOther.*"
                .withTotalsOfGroupsMatching(
                        "com\\.apm4all\\.hadoop\\..*" + "|" +
                        "com\\.apm4all\\.someOther\\..*")
                .build();

        JobConfigurationAdapter configurationAdapter = new JobConfigurationAdapter.Builder(job.getConfiguration())
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


        System.out.println("\n### summary (adapted) ###");
        System.out.println(summaryAdapter.asString());
        System.out.println("\n### counters (adapted) ###");
        System.out.println(countersAdapter.asString());
        System.out.println("\n### configuration (adapted) ###");
        System.out.println(configurationAdapter.asString());
    }

}
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.*;

public class JobCounters {
    // mapToGroups holds a hierarchy of groups.categories.names.values
    private ObjectMapper mapper = new ObjectMapper();
    private ObjectNode rootNode = mapper.createObjectNode();

    public long getCounterValue(Group group, Category category, Name name) {
		return getCounterValue( group.toString(), category.toString(), name.toString());
    }

    public long getCounterValue(String groupName, String categoryName, String counterName) {
		long counterValue;
		try {
			counterValue = rootNode
                    .with(groupName)
                    .with(categoryName)
                    .get(counterName)
                    .asLong();
		} catch (NullPointerException e) {
			throw(new NoSuchElementException("Could not find counter " + groupName + ":" + categoryName + ":" + counterName));
		}
		return counterValue;
	}

    public void addCounterValue(String groupName, String categoryName, String counterName, long value) {
        rootNode
            .with(groupName)
            .with(categoryName)
            .put(counterName, value);
    }

	public Set<String> getGroupNames()	{
		Iterator<String> itr = rootNode.fieldNames();
		HashSet<String> groupNames = new HashSet<String>();
		while (itr.hasNext()) {
			groupNames.add(itr.next());
		}
		return groupNames;
	}

	public Set<String> getCounterNames(String groupName, String categoryName)	{
		HashSet<String> counterNames = new HashSet<>();
		Iterator<String> itr = rootNode
                .path(groupName)
                .path(categoryName)
                .fieldNames();
		while (itr.hasNext()) {
            counterNames.add(itr.next());
        }
		return counterNames;
	}

	public static String groupToStringReplaceDots(String groupName) {
		return groupName.replaceAll("\\.",":").replaceAll(" ","_");
	}

	public enum Category {
        MAP,
        REDUCE,
        TOTAL;
    }

    public enum Group {
        ORG_APACHE_HADOOP_MAPREDUCE_FILESYSTEMCOUNTER                   ("org.apache.hadoop.mapreduce.FileSystemCounter"),
        ORG_APACHE_HADOOP_MAPREDUCE_JOBCOUNTER                          ("org.apache.hadoop.mapreduce.JobCounter"),
        ORG_APACHE_HADOOP_MAPREDUCE_TASKCOUNTER                         ("org.apache.hadoop.mapreduce.TaskCounter"),
        SHUFFLE_ERRORS                                                  ("Shuffle Errors"),
        CASCADING_FLOW_SLICECOUNTERS                                    ("cascading.flow.SliceCounters"),
        CASCADING_FLOW_STEPCOUNTERS                                     ("cascading.flow.StepCounters"),
        ORG_APACHE_HADOOP_MAPREDUCE_LIB_INPUT_FILEINPUTFORMATCOUNTER    ("org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter"),
        ORG_APACHE_HADOOP_MAPREDUCE_LIB_OUTPUT_FILEOUTPUTFORMATCOUNTER  ("org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter");

        private final String customName;
        Group(String name) { this.customName = name; }
        @Override public String toString() {return customName;}
        public String toStringReplaceDots() { return groupToStringReplaceDots(customName);}
    }

    public enum Name {
        // org.apache.hadoop.mapreduce.FileSystemCounter
        FILE_BYTES_READ                 ("FILE_BYTES_READ"),
		FILE_BYTES_WRITTEN				("FILE_BYTES_WRITTEN"),
		FILE_READ_OPS				    ("FILE_READ_OPS"),
		FILE_LARGE_READ_OPS				("FILE_LARGE_READ_OPS"),
		FILE_WRITE_OPS				    ("FILE_WRITE_OPS"),
		HDFS_BYTES_READ				    ("HDFS_BYTES_READ"),
		HDFS_BYTES_WRITTEN				("HDFS_BYTES_WRITTEN"),
		HDFS_READ_OPS				    ("HDFS_READ_OPS"),
		HDFS_LARGE_READ_OPS				("HDFS_LARGE_READ_OPS"),
		HDFS_WRITE_OPS				    ("HDFS_WRITE_OPS"),
        // org.apache.hadoop.mapreduce.JobCounter
		NUM_FAILED_MAPS				    ("NUM_FAILED_MAPS"),
		TOTAL_LAUNCHED_MAPS				("TOTAL_LAUNCHED_MAPS"),
		TOTAL_LAUNCHED_REDUCES			("TOTAL_LAUNCHED_REDUCES"),
		OTHER_LOCAL_MAPS				("OTHER_LOCAL_MAPS"),
		DATA_LOCAL_MAPS				    ("DATA_LOCAL_MAPS"),
		RACK_LOCAL_MAPS				    ("RACK_LOCAL_MAPS"),
		SLOTS_MILLIS_MAPS				("SLOTS_MILLIS_MAPS"),
		SLOTS_MILLIS_REDUCES			("SLOTS_MILLIS_REDUCES"),
		MILLIS_MAPS				        ("MILLIS_MAPS"),
		MILLIS_REDUCES				    ("MILLIS_REDUCES"),
		VCORES_MILLIS_MAPS				("VCORES_MILLIS_MAPS"),
		VCORES_MILLIS_REDUCES			("VCORES_MILLIS_REDUCES"),
		MB_MILLIS_MAPS				    ("MB_MILLIS_MAPS"),
		MB_MILLIS_REDUCES				("MB_MILLIS_REDUCES"),
        // org.apache.hadoop.mapreduce.TaskCounter
		MAP_INPUT_RECORDS				("MAP_INPUT_RECORDS"),
		MAP_OUTPUT_RECORDS				("MAP_OUTPUT_RECORDS"),
		MAP_OUTPUT_BYTES				("MAP_OUTPUT_BYTES"),
		MAP_OUTPUT_MATERIALIZED_BYTES   ("MAP_OUTPUT_MATERIALIZED_BYTES"),
		SPLIT_RAW_BYTES				    ("SPLIT_RAW_BYTES"),
		COMBINE_INPUT_RECORDS		    ("COMBINE_INPUT_RECORDS"),
		COMBINE_OUTPUT_RECORDS		    ("COMBINE_OUTPUT_RECORDS"),
		REDUCE_INPUT_GROUPS		        ("REDUCE_INPUT_GROUPS"),
		REDUCE_SHUFFLE_BYTES		    ("REDUCE_SHUFFLE_BYTES"),
		REDUCE_INPUT_RECORDS		    ("REDUCE_INPUT_RECORDS"),
		REDUCE_OUTPUT_RECORDS		    ("REDUCE_OUTPUT_RECORDS"),
		SPILLED_RECORDS				    ("SPILLED_RECORDS"),
		SHUFFLED_MAPS				    ("SHUFFLED_MAPS"),
		FAILED_SHUFFLE				    ("FAILED_SHUFFLE"),
		MERGED_MAP_OUTPUTS			    ("MERGED_MAP_OUTPUTS"),
		GC_TIME_MILLIS				    ("GC_TIME_MILLIS"),
		CPU_MILLISECONDS			    ("CPU_MILLISECONDS"),
		PHYSICAL_MEMORY_BYTES		    ("PHYSICAL_MEMORY_BYTES"),
		VIRTUAL_MEMORY_BYTES		    ("VIRTUAL_MEMORY_BYTES"),
		COMMITTED_HEAP_BYTES		    ("COMMITTED_HEAP_BYTES"),
        // Shuffle Errors
		BAD_ID				            ("BAD_ID"),
		CONNECTION				        ("CONNECTION"),
		IO_ERROR				        ("IO_ERROR"),
		WRONG_LENGTH				    ("WRONG_LENGTH"),
		WRONG_MAP				        ("WRONG_MAP"),
		WRONG_REDUCE				    ("WRONG_REDUCE"),
        // cascading.flow.SliceCounters
		PROCESS_BEGIN_TIME			    ("Process_Begin_Time"),
		PROCESS_END_TIME			    ("Process_End_Time"),
		READ_DURATION				    ("Read_Duration"),
		TUPLES_READ				        ("Tuples_Read"),
		TUPLES_WRITTEN				    ("Tuples_Written"),
        WRITE_DURATION				    ("Write_Duration"),
        // cascading.flow.StepCounters (also has "Tuples_Read" and Tuples_Written")
        // org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter
		BYTES_READ				        ("BYTES_READ"),
        // org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter
		BYTES_WRITTEN				    ("BYTES_WRITTEN");

        private final String customName;
        Name(String name) { this.customName = name; }
        @Override public String toString() {return customName;}
    }

}

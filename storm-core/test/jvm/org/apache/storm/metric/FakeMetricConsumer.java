/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.metric;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;

public class FakeMetricConsumer implements IMetricsConsumer {

    public static final Table<String, String, Multimap<Integer, Object>> BUFFER = HashBasedTable.create();

    public static Map<Integer, Collection<Object>> getTaskIdToBuckets(String componentName, String metricName) {
        synchronized (BUFFER) {
            Multimap<Integer, Object> taskIdToBuckets = BUFFER.get(componentName, metricName);
            if (taskIdToBuckets == null) {
                return null;
            }
            return taskIdToBuckets.asMap().entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey(), entry -> new ArrayList<>(entry.getValue())));
        }
    }

    @Override
    public void prepare(Map<String, Object> topoConf, Object registrationArgument, TopologyContext context, IErrorReporter errorReporter) {
        synchronized (BUFFER) {
            BUFFER.clear();
        }
    }

    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        synchronized (BUFFER) {
            for (DataPoint dp : dataPoints) {
                for (Map.Entry<String, Object> entry : expandComplexDataPoint(dp).entrySet()) {
                    String metricName = entry.getKey();
                    Multimap<Integer, Object> taskIdToBucket = BUFFER.get(taskInfo.srcComponentId, metricName);
                    if (null == taskIdToBucket) {
                        taskIdToBucket = ArrayListMultimap.create();
                        taskIdToBucket.put(taskInfo.srcTaskId, entry.getValue());
                    } else {
                        taskIdToBucket.get(taskInfo.srcTaskId).add(entry.getValue());
                    }
                    BUFFER.put(taskInfo.srcComponentId, metricName, taskIdToBucket);
                }
            }
        }
    }

    @Override
    public void cleanup() {
        synchronized (BUFFER) {
            BUFFER.clear();
        }
    }

    private Map<String, Object> expandComplexDataPoint(DataPoint dp) {
        Map<String, Object> expanded = new HashMap<>();
        if (dp.value instanceof Map) {
            for (Map.Entry entry : ((Map<Object, Object>) dp.value).entrySet()) {
                expanded.put(dp.name + "/" + entry.getKey(), entry.getValue());
            }
        } else {
            expanded.put(dp.name, dp.value);
        }
        return expanded;
    }
}

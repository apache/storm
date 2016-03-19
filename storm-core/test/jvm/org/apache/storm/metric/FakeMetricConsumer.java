/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.metric;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;

import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class FakeMetricConsumer implements IMetricsConsumer {

    public static final Table<String, String, Multimap<Integer, Object>> buffer = HashBasedTable.create();

    @Override
    public void prepare(Map stormConf, Object registrationArgument, TopologyContext context, IErrorReporter errorReporter) {
        synchronized (buffer) {
            buffer.clear();
        }
    }

    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        synchronized (buffer) {
            for (DataPoint dp : dataPoints) {
                for (Map.Entry<String, Object> entry : expandComplexDataPoint(dp).entrySet()) {
                    String metricName = entry.getKey();
                    Multimap<Integer, Object> taskIdToBucket = buffer.get(taskInfo.srcComponentId, metricName);
                    if (null == taskIdToBucket) {
                        taskIdToBucket = ArrayListMultimap.create();
                        taskIdToBucket.put(taskInfo.srcTaskId, entry.getValue());
                    } else {
                        taskIdToBucket.get(taskInfo.srcTaskId).add(entry.getValue());
                    }
                    buffer.put(taskInfo.srcComponentId, metricName, taskIdToBucket);
                }
            }
        }
    }

    @Override
    public void cleanup() {
        synchronized (buffer) {
            buffer.clear();
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

    public static Map<Integer, Collection<Object>> getTaskIdToBuckets(String componentName, String metricName) {
        synchronized (buffer) {
            Multimap<Integer, Object> taskIdToBuckets = buffer.get(componentName, metricName);
            return (null != taskIdToBuckets) ? taskIdToBuckets.asMap() : null;
        }
    }
}

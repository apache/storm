/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.ui.model;

import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.ui.utils.UIMetricUtils;
import com.alibaba.jstorm.utils.JStormUtils;

import java.util.*;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class UITaskMetric extends UIComponentMetric {

    private String taskId;

    private static List<String> METRIC_QUEUE = Arrays.asList(MetricDef.EXECUTE_QUEUE,
            MetricDef.DESERIALIZE_QUEUE, MetricDef.SERIALIZE_QUEUE);

    private static final String FORMAT = "##0.00";

    public static final String[] HEAD = {MetricDef.EMMITTED_NUM, MetricDef.ACKED_NUM, MetricDef.FAILED_NUM, MetricDef.SEND_TPS,
            MetricDef.RECV_TPS, MetricDef.PROCESS_LATENCY,  MetricDef.DESERIALIZE_TIME, MetricDef.SERIALIZE_TIME, MetricDef.EXECUTE_TIME,
            MetricDef.COLLECTOR_EMIT_TIME, MetricDef.ACKER_TIME, MetricDef.DESERIALIZE_QUEUE, MetricDef.SERIALIZE_QUEUE,
            MetricDef.EXECUTE_QUEUE };

    public UITaskMetric(String componentName, int taskId) {
        super(componentName);
        this.taskId = taskId+"";
    }


    public void mergeValue() {
        super.mergeValue();
        // the queue value should be displayed in percent
        for (String q : METRIC_QUEUE){
            if (metrics.containsKey(q)){
                metrics.put(q, UIMetricUtils.format(JStormUtils.parseDouble(metrics.get(q)) * 100, FORMAT));
            }
        }
        for (Map.Entry<String,String> entry : subMetrics.entrySet()){
            String metricName = entry.getKey().split("@")[0];
            String value = entry.getValue();
            if(METRIC_QUEUE.contains(metricName)){
                String v = UIMetricUtils.format(JStormUtils.parseDouble(value)*100,FORMAT);
                subMetrics.put(entry.getKey(), v);
            }
        }
    }

    public String getTaskId() {
        return taskId;
    }


}

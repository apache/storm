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
package com.alibaba.jstorm.task;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.common.metric.MetricRegistry;
import com.alibaba.jstorm.common.metric.window.Metric;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.MetricDef;

public class TaskBaseMetric implements Serializable {
	private static final Logger LOG = LoggerFactory.getLogger(TaskBaseMetric.class);
    private static final long serialVersionUID = -7157987126460293444L;
    protected MetricRegistry metrics;
    private int taskId;

    public TaskBaseMetric(int taskId) {
        metrics = JStormMetrics.registerTask(taskId);
        this.taskId = taskId;
    }

    public void update(String name, Number value, int type) {
        Metric metric = metrics.getMetric(name);
        if (metric == null) {
            metric = JStormMetrics.Builder.mkInstance(type);
            try {
            	/**
            	 * Here use one hack method to handle competition register metric
            	 * if duplicated metric, just skip it.
            	 * 
            	 * this will improve performance
            	 */
            	JStormMetrics.registerTaskMetric(metric, taskId, name);
            }catch(Exception e) {
            	LOG.warn("Duplicated metrics of {}, taskId:{}", name, taskId);
            	return ;
            }
            
        }
        metric.update(value);
    }

    public void send_tuple(String stream, int num_out_tasks) {
        if (num_out_tasks <= 0) {
            return;
        }

        String emmitedName =
                MetricRegistry.name(MetricDef.EMMITTED_NUM, stream);
        update(emmitedName, Double.valueOf(num_out_tasks),
                JStormMetrics.Builder.COUNTER);

        String sendTpsName = MetricRegistry.name(MetricDef.SEND_TPS, stream);
        update(sendTpsName, Double.valueOf(num_out_tasks),
                JStormMetrics.Builder.METER);
    }

    public void recv_tuple(String component, String stream) {

        String name =
                MetricRegistry.name(MetricDef.RECV_TPS, component, stream);
        update(name, Double.valueOf(1), JStormMetrics.Builder.METER);

    }

    public void bolt_acked_tuple(String component, String stream,
            Double latency_ms) {

        if (latency_ms == null) {
            return;
        }

        String ackNumName =
                MetricRegistry.name(MetricDef.ACKED_NUM, component, stream);
        update(ackNumName, Double.valueOf(1), JStormMetrics.Builder.COUNTER);

        String processName =
                MetricRegistry.name(MetricDef.PROCESS_LATENCY, component,
                        stream);
        update(processName, latency_ms,
                JStormMetrics.Builder.HISTOGRAM);
    }

    public void bolt_failed_tuple(String component, String stream) {

        String failNumName =
                MetricRegistry.name(MetricDef.FAILED_NUM, component, stream);
        update(failNumName, Double.valueOf(1), JStormMetrics.Builder.COUNTER);
    }

    public void spout_acked_tuple(String stream, long st) {

        String ackNumName =
                MetricRegistry.name(MetricDef.ACKED_NUM,
                        Common.ACKER_COMPONENT_ID, stream);
        update(ackNumName, Double.valueOf(1), JStormMetrics.Builder.COUNTER);

        String processName =
                MetricRegistry.name(MetricDef.PROCESS_LATENCY,
                        Common.ACKER_COMPONENT_ID, stream);
        update(processName, Double.valueOf(st), JStormMetrics.Builder.HISTOGRAM);

    }

    public void spout_failed_tuple(String stream) {
        String failNumName =
                MetricRegistry.name(MetricDef.FAILED_NUM,
                        Common.ACKER_COMPONENT_ID, stream);
        update(failNumName, Double.valueOf(1), JStormMetrics.Builder.COUNTER);

    }
}

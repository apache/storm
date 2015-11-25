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

import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.common.metric.AsmMetric;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.metric.MetricType;
import com.alibaba.jstorm.metric.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TaskBaseMetric implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(JStormMetrics.class);

    private static final long serialVersionUID = -7157987126460293444L;
    private String topologyId;
    private String componentId;
    private int taskId;
    private boolean enableMetrics;

    /**
     * local metric name cache to avoid frequent metric name concatenation streamId + name ==> full metric name
     */
    private static final ConcurrentMap<String, AsmMetric> metricCache = new ConcurrentHashMap<String, AsmMetric>();

    public TaskBaseMetric(String topologyId, String componentId, int taskId, boolean enableMetrics) {
        this.topologyId = topologyId;
        this.componentId = componentId;
        this.taskId = taskId;
        this.enableMetrics = enableMetrics;
        logger.info("init task base metric, tp id:{}, comp id:{}, task id:{}", topologyId, componentId, taskId);
    }

    public void update(final String streamId, final String name, final Number value, final MetricType metricType,
                       boolean mergeTopology) {
        String key = taskId + streamId + name;
        AsmMetric existingMetric = metricCache.get(key);
        if (existingMetric == null) {
            String fullName = MetricUtils.streamMetricName(topologyId, componentId, taskId, streamId, name, metricType);
            existingMetric = JStormMetrics.getStreamMetric(fullName);
            if (existingMetric == null) {
                existingMetric = AsmMetric.Builder.build(metricType);
                JStormMetrics.registerStreamMetric(fullName, existingMetric, mergeTopology);
            }
            metricCache.putIfAbsent(key, existingMetric);
        }

        existingMetric.update(value);
    }

    public void update(final String streamId, final String name, final Number value, final MetricType metricType) {
        update(streamId, name, value, metricType, true);
    }

    public void send_tuple(String stream, int num_out_tasks) {
        if (enableMetrics && num_out_tasks > 0) {
            update(stream, MetricDef.EMMITTED_NUM, num_out_tasks, MetricType.COUNTER);
            update(stream, MetricDef.SEND_TPS, num_out_tasks, MetricType.METER);
        }
    }

    public void recv_tuple(String component, String stream) {
        if (enableMetrics) {
            update(stream, AsmMetric.mkName(component, MetricDef.RECV_TPS), 1, MetricType.METER);
//            update(stream, MetricDef.RECV_TPS, 1, MetricType.METER);
        }
    }

    public void bolt_acked_tuple(String component, String stream, Long latency, Long lifeCycle) {
        if (enableMetrics) {
//            update(stream, AsmMetric.mkName(component, MetricDef.ACKED_NUM), 1, MetricType.COUNTER);
//            update(stream, AsmMetric.mkName(component, MetricDef.PROCESS_LATENCY), latency_ms, MetricType.HISTOGRAM);
            update(stream, MetricDef.ACKED_NUM, 1, MetricType.COUNTER);
            update(stream, MetricDef.PROCESS_LATENCY, latency, MetricType.HISTOGRAM, false);

            if (lifeCycle > 0) {
                update(stream, AsmMetric.mkName(component, MetricDef.TUPLE_LIEF_CYCLE), lifeCycle, MetricType.HISTOGRAM, false);
            }
        }
    }

    public void bolt_failed_tuple(String component, String stream) {
        if (enableMetrics) {
            //update(stream, AsmMetric.mkName(component, MetricDef.FAILED_NUM), 1, MetricType.COUNTER);
            update(stream, MetricDef.FAILED_NUM, 1, MetricType.COUNTER);
        }
    }

    public void spout_acked_tuple(String stream, long st, Long lifeCycle) {
        if (enableMetrics) {
            update(stream, MetricDef.ACKED_NUM, 1, MetricType.COUNTER);
            update(stream, MetricDef.PROCESS_LATENCY, st, MetricType.HISTOGRAM, true);

            if (lifeCycle > 0) {
                update(stream, AsmMetric.mkName(Common.ACKER_COMPONENT_ID, MetricDef.TUPLE_LIEF_CYCLE), lifeCycle, MetricType.HISTOGRAM, false);
            }
        }
    }

    public void spout_failed_tuple(String stream) {
        if (enableMetrics) {
            update(stream, MetricDef.FAILED_NUM, 1, MetricType.COUNTER);
        }
    }
}

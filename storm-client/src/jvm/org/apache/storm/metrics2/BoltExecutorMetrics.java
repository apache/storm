/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.metrics2;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.storm.generated.BoltStats;
import org.apache.storm.generated.ExecutorSpecificStats;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.stats.ClientStatsUtil;
import org.apache.storm.task.WorkerTopologyContext;

public class BoltExecutorMetrics extends ExecutorMetrics {
    private final ConcurrentMap<String, WindowedMeter> executedByStream = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, WindowedHistogram> processLatencies = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, WindowedHistogram> executeLatencies = new ConcurrentHashMap<>();

    public BoltExecutorMetrics(WorkerTopologyContext context, StormMetricRegistry metricRegistry,
                               Map<String, Object> topoConf) {
        super(context, metricRegistry, topoConf);
    }

    public void boltFailedTuple(String componentId, String streamId, int taskId) {
        String key = createKey(componentId, streamId);
        WindowedMeter meter = this.failedByStream.get(key);
        if (meter == null) {
            String name = createMetricName("__fail-count", componentId, streamId);
            meter = createTaskMeter(name, componentId, streamId, taskId);
            this.failedByStream.put(key, meter);
        }
        meter.incBy(this.rate);
    }

    public void boltAckedTuple(String componentId, String streamId, int taskId, long latencyMs) {
        String key = createKey(componentId, streamId);
        WindowedMeter meter = this.ackedByStream.get(key);
        if (meter == null) {
            String name = createMetricName("__ack-count", componentId, streamId);
            meter = createTaskMeter(name, componentId, streamId, taskId);
            this.ackedByStream.put(key, meter);
        }
        meter.incBy(this.rate);

        WindowedHistogram histogram = this.processLatencies.get(key);
        if (histogram == null) {
            String name = createMetricName("__process-latency", componentId, streamId);
            histogram = createTaskHistogram(name, componentId, streamId, taskId);
            this.processLatencies.put(key, histogram);
        }
        histogram.record(latencyMs);
    }

    public void boltExecuteTuple(String componentId, String streamId, int taskId, long latencyMs) {
        String key = createKey(componentId, streamId);
        WindowedMeter meter = this.executedByStream.get(key);
        if (meter == null) {
            String name = createMetricName("__execute-count", componentId, streamId);
            meter = createTaskMeter(name, componentId, streamId, taskId);
            this.executedByStream.put(key, meter);
        }
        meter.incBy(this.rate);

        WindowedHistogram histogram = this.executeLatencies.get(key);
        if (histogram == null) {
            String name = createMetricName("__execute-latency", componentId, streamId);
            histogram = createTaskHistogram(name, componentId, streamId, taskId);
            this.executeLatencies.put(key, histogram);
        }
        histogram.record(latencyMs);
    }

    /**
     * Creates a metric name that should not have a duplicate short name for a given taskId.  Since multiple metrics
     * can be created for a task (such as execute-count), append the component/stream to get a unique name.
     *
     * @param prefix desired metric name
     * @param componentId componentId
     * @param streamId streamId
     */
    private String createMetricName(String prefix, String componentId, String streamId) {
        return prefix + "-" + componentId + ":" + streamId;
    }

    Map<String, Map<String, Double>> getProcessLatencyTimeCounts() {
        Map<String, Map<String, Double>> ret = getHistogramTimeCounts(processLatencies);
        return ret;
    }

    Map<String, Map<String, Double>> getExecuteLatencyTimeCounts() {
        Map<String, Map<String, Double>> ret = getHistogramTimeCounts(executeLatencies);
        return ret;
    }

    private String createKey(String componentId, String streamId) {
        return "[" + componentId + ", " + streamId + "]";
    }

    Map<String, Map<String, Long>> getExecutedTimeCounts() {
        Map<String, Map<String, Long>> ret = getMeterTimeCounts(executedByStream);
        return ret;
    }

    @Override
    public void cleanup() {
        cleanupMeters(executedByStream);
        cleanupHistograms(processLatencies);
        cleanupHistograms(executeLatencies);
        super.cleanup();
    }

    @Override
    public ExecutorStats renderStats() {
        ExecutorStats ret = new ExecutorStats();
        // common stats
        ret.set_emitted(getEmitTimeCounts());
        ret.set_transferred(getTransferTimeCounts());
        ret.set_rate(this.rate);

        // bolt stats
        BoltStats boltStats = new BoltStats(
                ClientStatsUtil.windowSetConverter(getAckTimeCounts(), ClientStatsUtil.TO_GSID, ClientStatsUtil.IDENTITY),
                ClientStatsUtil.windowSetConverter(getFailTimeCounts(), ClientStatsUtil.TO_GSID, ClientStatsUtil.IDENTITY),
                ClientStatsUtil.windowSetConverter(getProcessLatencyTimeCounts(), ClientStatsUtil.TO_GSID, ClientStatsUtil.IDENTITY),
                ClientStatsUtil.windowSetConverter(getExecutedTimeCounts(), ClientStatsUtil.TO_GSID, ClientStatsUtil.IDENTITY),
                ClientStatsUtil.windowSetConverter(getExecuteLatencyTimeCounts(), ClientStatsUtil.TO_GSID, ClientStatsUtil.IDENTITY));
        ret.set_specific(ExecutorSpecificStats.bolt(boltStats));

        return ret;
    }
}

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
import org.apache.storm.daemon.Task;
import org.apache.storm.generated.ExecutorSpecificStats;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.SpoutStats;
import org.apache.storm.task.WorkerTopologyContext;

public class SpoutExecutorMetrics extends ExecutorMetrics {
    private final ConcurrentMap<String, WindowedHistogram> completeLatencies = new ConcurrentHashMap<>();

    public SpoutExecutorMetrics(WorkerTopologyContext context, StormMetricRegistry metricRegistry,
                               Map<String, Object> topoConf) {
        super(context, metricRegistry, topoConf);
    }

    public void spoutAckedTuple(String componentId, String streamId, int taskId, long latencyMs) {
        WindowedMeter meter = this.ackedByStream.get(streamId);
        if (meter == null) {
            String name = createMetricName("__ack-count", streamId);
            meter = createTaskMeter(name, componentId, streamId, taskId);
            this.ackedByStream.put(streamId, meter);
        }
        meter.incBy(this.rate);

        WindowedHistogram histogram = this.completeLatencies.get(streamId);
        if (histogram == null) {
            String name = createMetricName("__complete-latency", streamId);
            histogram = createTaskHistogram(name, componentId, streamId, taskId);
            this.completeLatencies.put(streamId, histogram);
        }
        histogram.record(latencyMs);
    }

    public void spoutFailedTuple(String componentId, String streamId, int taskId) {
        WindowedMeter meter = this.failedByStream.get(streamId);
        if (meter == null) {
            String name = createMetricName("__fail-count", streamId);
            meter = createTaskMeter(name, componentId, streamId, taskId);
            this.failedByStream.put(streamId, meter);
        }
        meter.incBy(this.rate);
    }

    Map<String, Map<String, Double>> getCompleteLatencyTimeCounts() {
        Map<String, Map<String, Double>> ret = getHistogramTimeCounts(completeLatencies);
        return ret;
    }

    @Override
    public void cleanup() {
        cleanupHistograms(completeLatencies);
        super.cleanup();
    }

    @Override
    public ExecutorStats renderStats() {
        ExecutorStats ret = new ExecutorStats();
        // common fields
        ret.set_emitted(getEmitTimeCounts());
        ret.set_transferred(getTransferTimeCounts());
        ret.set_rate(this.rate);

        // spout stats
        SpoutStats spoutStats = new SpoutStats(
                getAckTimeCounts(), getFailTimeCounts(), getCompleteLatencyTimeCounts());
        ret.set_specific(ExecutorSpecificStats.spout(spoutStats));
        return ret;
    }
}

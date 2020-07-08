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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.storm.Config;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ObjectReader;

public abstract class ExecutorMetrics {
    protected final ConcurrentMap<String, WindowedMeter> ackedByStream = new ConcurrentHashMap<>();
    protected final ConcurrentMap<String, WindowedMeter> failedByStream = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, WindowedMeter> emittedByStream = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, WindowedMeter> transferredByStream = new ConcurrentHashMap<>();

    private final StormMetricRegistry metricRegistry;
    private final String topologyId;
    private final int workerPort;
    protected final int numBuckets;
    protected final int rate;

    public ExecutorMetrics(WorkerTopologyContext context, StormMetricRegistry metricRegistry, Map<String, Object> topoConf) {
        this.metricRegistry = metricRegistry;
        this.topologyId = context.getStormId();
        this.workerPort = context.getThisWorkerPort();
        this.numBuckets = ObjectReader.getInt(topoConf.get(Config.NUM_STAT_BUCKETS));
        this.rate = ConfigUtils.samplingRate(topoConf);
    }

    public void emittedTuple(String componentId, String streamId, int taskId) {
        WindowedMeter meter = this.emittedByStream.get(streamId);
        if (meter == null) {
            String name = createMetricName("__emit-count", streamId);
            meter = createTaskMeter(name, componentId, streamId, taskId);
            this.emittedByStream.put(streamId, meter);
        }
        meter.incBy(this.rate);
    }

    public void transferredTuples(String componentId, String streamId, int taskId, int amount) {
        WindowedMeter meter = this.transferredByStream.get(streamId);
        if (meter == null) {
            String name = createMetricName("__transfer-count", streamId);
            meter = createTaskMeter(name, componentId, streamId, taskId);
            this.transferredByStream.put(streamId, meter);
        }
        meter.incBy(amount * this.rate);
    }

    /**
     * Creates a metric name that should not have a duplicate short name for a given taskId.  Since multiple metrics
     * can be created for a task (such as execute-count), append the stream to get a unique name.
     *
     * @param prefix desired metric name
     * @param streamId streamId
     */
    protected String createMetricName(String prefix, String streamId) {
        return prefix + "-" + streamId;
    }

    protected WindowedMeter createTaskMeter(String name, String componentId, String streamId, int taskId) {
        return new WindowedMeter(name, metricRegistry, topologyId, componentId, streamId, taskId, workerPort, numBuckets);
    }

    protected WindowedHistogram createTaskHistogram(String name, String componentId, String streamId, int taskId) {
        return new WindowedHistogram(name, metricRegistry, topologyId, componentId, streamId, taskId, workerPort, numBuckets);
    }

    protected Map<String, Map<String, Long>> getAckTimeCounts() {
        Map<String, Map<String, Long>> ret = getMeterTimeCounts(ackedByStream);
        return ret;
    }

    protected Map<String, Map<String, Long>> getEmitTimeCounts() {
        Map<String, Map<String, Long>> ret = getMeterTimeCounts(emittedByStream);
        return ret;
    }

    protected Map<String, Map<String, Long>> getFailTimeCounts() {
        Map<String, Map<String, Long>> ret = getMeterTimeCounts(failedByStream);
        return ret;
    }

    protected Map<String, Map<String, Long>> getTransferTimeCounts() {
        Map<String, Map<String, Long>> ret = getMeterTimeCounts(transferredByStream);
        return ret;
    }

    protected Map<String, Map<String, Long>> getMeterTimeCounts(Map<String, WindowedMeter> map) {
        Map<String, Map<String, Long>> ret = new HashMap<>();
        for (Map.Entry<String, WindowedMeter> entry : map.entrySet()) {
            String metricName = entry.getKey();
            Map<String, Long> toFlip = entry.getValue().getTimeCounts();
            for (Map.Entry<String, Long> subEntry : toFlip.entrySet()) {
                String time = subEntry.getKey();
                Map<String, Long> tmp = ret.get(time);
                if (tmp == null) {
                    tmp = new HashMap<>();
                    ret.put(time, tmp);
                }
                tmp.put(metricName, subEntry.getValue());
            }
        }
        return ret;
    }

    protected Map<String, Map<String, Double>> getHistogramTimeCounts(Map<String, WindowedHistogram> map) {
        Map<String, Map<String, Double>> ret = new HashMap<>();
        for (Map.Entry<String, WindowedHistogram> entry : map.entrySet()) {
            String metricName = entry.getKey();
            Map<String, Double> toFlip = entry.getValue().getTimeLatAvg();
            for (Map.Entry<String, Double> subEntry : toFlip.entrySet()) {
                String time = subEntry.getKey();
                Map<String, Double> tmp = ret.get(time);
                if (tmp == null) {
                    tmp = new HashMap<>();
                    ret.put(time, tmp);
                }
                tmp.put(metricName, subEntry.getValue());
            }
        }
        return ret;
    }

    public void cleanup() {
        cleanupMeters(ackedByStream);
        cleanupMeters(failedByStream);
        cleanupMeters(emittedByStream);
        cleanupMeters(transferredByStream);
    }

    protected void cleanupMeters(Map<String, WindowedMeter> map) {
        for (WindowedMeter meter : map.values()) {
            meter.close();
        }
    }

    protected void cleanupHistograms(Map<String, WindowedHistogram> map) {
        for (WindowedHistogram histogram : map.values()) {
            histogram.close();
        }
    }

    public abstract ExecutorStats renderStats();
}

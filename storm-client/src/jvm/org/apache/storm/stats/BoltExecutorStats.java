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

package org.apache.storm.stats;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.daemon.Task;
import org.apache.storm.generated.BoltStats;
import org.apache.storm.generated.ExecutorSpecificStats;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.metric.internal.MultiCountStat;
import org.apache.storm.metric.internal.MultiLatencyStat;
import org.apache.storm.shade.com.google.common.collect.Lists;

@SuppressWarnings("unchecked")
public class BoltExecutorStats extends CommonStats {
    MultiCountStat executedStats;
    MultiLatencyStat processLatencyStats;
    MultiLatencyStat executeLatencyStats;

    public BoltExecutorStats(int rate, int numStatBuckets) {
        super(rate, numStatBuckets);
        this.executedStats = new MultiCountStat(numStatBuckets);
        this.processLatencyStats = new MultiLatencyStat(numStatBuckets);
        this.executeLatencyStats = new MultiLatencyStat(numStatBuckets);
    }

    public MultiCountStat getExecuted() {
        return executedStats;
    }

    public MultiLatencyStat getProcessLatencies() {
        return processLatencyStats;
    }

    public MultiLatencyStat getExecuteLatencies() {
        return executeLatencyStats;
    }

    @Override
    public void cleanupStats() {
        executedStats.close();
        processLatencyStats.close();
        executeLatencyStats.close();
        super.cleanupStats();
    }

    public void boltExecuteTuple(String component, String stream, long latencyMs, long workerUptimeSecs,
                                 Task firstExecutorTask) {
        List key = Lists.newArrayList(component, stream);
        this.getExecuted().incBy(key, this.rate);
        this.getExecuteLatencies().record(key, latencyMs);

        // Calculate capacity:  This is really for the whole executor, but we will use the executor's first task
        // for reporting the metric.
        double capacity = calculateCapacity(workerUptimeSecs);
        firstExecutorTask.getTaskMetrics().setCapacity(capacity);
    }

    private double calculateCapacity(long workerUptimeSecs) {
        if (workerUptimeSecs > 0) {
            Map<String, Double> execAvg = valueStat(this.getExecuteLatencies()).get(MultiCountStat.TEN_MIN_IN_SECONDS_STR);
            Map<String, Long> exec = valueStat(this.getExecuted()).get(MultiCountStat.TEN_MIN_IN_SECONDS_STR);

            Set<Object> allKeys = new HashSet<>();
            if (execAvg != null) {
                allKeys.addAll(execAvg.keySet());
            }
            if (exec != null) {
                allKeys.addAll(exec.keySet());
            }

            double totalAvg = 0;
            for (Object k : allKeys) {
                double avg = getOr0(execAvg, k).doubleValue();
                long cnt = getOr0(exec, k).longValue();
                totalAvg += avg * cnt;
            }

            return totalAvg / (Math.min(workerUptimeSecs, MultiCountStat.TEN_MIN_IN_SECONDS) * 1000);
        }
        return 0.0;
    }

    private static Number getOr0(Map m, Object k) {
        if (m == null) {
            return 0;
        }

        Number n = (Number) m.get(k);
        if (n == null) {
            return 0;
        }
        return n;
    }

    public void boltAckedTuple(String component, String stream, long latencyMs) {
        List key = Lists.newArrayList(component, stream);
        this.getAcked().incBy(key, this.rate);
        this.getProcessLatencies().record(key, latencyMs);
    }

    public void boltFailedTuple(String component, String stream) {
        List key = Lists.newArrayList(component, stream);
        this.getFailed().incBy(key, this.rate);
    }

    @Override
    public ExecutorStats renderStats() {
        ExecutorStats ret = new ExecutorStats();
        // common stats
        ret.set_emitted(valueStat(getEmitted()));
        ret.set_transferred(valueStat(getTransferred()));
        ret.set_rate(this.rate);

        // bolt stats
        BoltStats boltStats = new BoltStats(
            ClientStatsUtil.windowSetConverter(valueStat(getAcked()), ClientStatsUtil.TO_GSID, ClientStatsUtil.IDENTITY),
            ClientStatsUtil.windowSetConverter(valueStat(getFailed()), ClientStatsUtil.TO_GSID, ClientStatsUtil.IDENTITY),
            ClientStatsUtil.windowSetConverter(valueStat(processLatencyStats), ClientStatsUtil.TO_GSID, ClientStatsUtil.IDENTITY),
            ClientStatsUtil.windowSetConverter(valueStat(executedStats), ClientStatsUtil.TO_GSID, ClientStatsUtil.IDENTITY),
            ClientStatsUtil.windowSetConverter(valueStat(executeLatencyStats), ClientStatsUtil.TO_GSID, ClientStatsUtil.IDENTITY));
        ret.set_specific(ExecutorSpecificStats.bolt(boltStats));

        return ret;
    }
}

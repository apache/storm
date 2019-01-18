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

import com.codahale.metrics.Counter;
import java.util.List;
import org.apache.storm.generated.BoltStats;
import org.apache.storm.generated.ExecutorSpecificStats;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.metric.internal.MultiCountStatAndMetric;
import org.apache.storm.metric.internal.MultiLatencyStatAndMetric;
import org.apache.storm.shade.com.google.common.collect.Lists;

@SuppressWarnings("unchecked")
public class BoltExecutorStats extends CommonStats {
    MultiCountStatAndMetric executedStats;
    MultiLatencyStatAndMetric processLatencyStats;
    MultiLatencyStatAndMetric executeLatencyStats;

    public BoltExecutorStats(int rate, int numStatBuckets) {
        super(rate, numStatBuckets);
        this.executedStats = new MultiCountStatAndMetric(numStatBuckets);
        this.processLatencyStats = new MultiLatencyStatAndMetric(numStatBuckets);
        this.executeLatencyStats = new MultiLatencyStatAndMetric(numStatBuckets);
    }

    public MultiCountStatAndMetric getExecuted() {
        return executedStats;
    }

    public MultiLatencyStatAndMetric getProcessLatencies() {
        return processLatencyStats;
    }

    public MultiLatencyStatAndMetric getExecuteLatencies() {
        return executeLatencyStats;
    }

    @Override
    public void cleanupStats() {
        executedStats.close();
        processLatencyStats.close();
        executeLatencyStats.close();
        super.cleanupStats();
    }

    public void boltExecuteTuple(String component, String stream, long latencyMs) {
        List key = Lists.newArrayList(component, stream);
        this.getExecuted().incBy(key, this.rate);
        this.getExecuteLatencies().record(key, latencyMs);
    }

    public void boltAckedTuple(String component, String stream, long latencyMs, Counter ackedCounter) {
        List key = Lists.newArrayList(component, stream);
        this.getAcked().incBy(key, this.rate);
        ackedCounter.inc(this.rate);
        this.getProcessLatencies().record(key, latencyMs);
    }

    public void boltFailedTuple(String component, String stream, long latencyMs, Counter failedCounter) {
        List key = Lists.newArrayList(component, stream);
        this.getFailed().incBy(key, this.rate);
        failedCounter.inc(this.rate);
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

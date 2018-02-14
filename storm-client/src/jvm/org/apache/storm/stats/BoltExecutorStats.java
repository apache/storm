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
package org.apache.storm.stats;

import com.google.common.collect.Lists;

import org.apache.storm.generated.BoltStats;
import org.apache.storm.generated.ExecutorSpecificStats;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.metric.internal.MultiCountStatAndMetric;
import org.apache.storm.metric.internal.MultiLatencyStatAndMetric;

import java.util.List;

@SuppressWarnings("unchecked")
public class BoltExecutorStats extends CommonStats {

    MultiCountStatAndMetric   ackedStats;
    MultiCountStatAndMetric   failedStats;
    MultiCountStatAndMetric   executedStats;
    MultiLatencyStatAndMetric processLatencyStats;
    MultiLatencyStatAndMetric executeLatencyStats;

    public BoltExecutorStats(int rate,int numStatBuckets) {
        super(rate,numStatBuckets);
        this.ackedStats = new MultiCountStatAndMetric(numStatBuckets);
        this.failedStats = new MultiCountStatAndMetric(numStatBuckets);
        this.executedStats = new MultiCountStatAndMetric(numStatBuckets);
        this.processLatencyStats = new MultiLatencyStatAndMetric(numStatBuckets);
        this.executeLatencyStats = new MultiLatencyStatAndMetric(numStatBuckets);
    }

    public MultiCountStatAndMetric getAcked() {
        return ackedStats;
    }

    public MultiCountStatAndMetric getFailed() {
        return failedStats;
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
        ackedStats.close();
        failedStats.close();
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

    public void boltAckedTuple(String component, String stream, long latencyMs) {
        List key = Lists.newArrayList(component, stream);
        this.getAcked().incBy(key, this.rate);
        this.getProcessLatencies().record(key, latencyMs);
    }

    public void boltFailedTuple(String component, String stream, long latencyMs) {
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
                StatsUtil.windowSetConverter(valueStat(ackedStats), StatsUtil.TO_GSID, StatsUtil.IDENTITY),
                StatsUtil.windowSetConverter(valueStat(failedStats), StatsUtil.TO_GSID, StatsUtil.IDENTITY),
                StatsUtil.windowSetConverter(valueStat(processLatencyStats), StatsUtil.TO_GSID, StatsUtil.IDENTITY),
                StatsUtil.windowSetConverter(valueStat(executedStats), StatsUtil.TO_GSID, StatsUtil.IDENTITY),
                StatsUtil.windowSetConverter(valueStat(executeLatencyStats), StatsUtil.TO_GSID, StatsUtil.IDENTITY));
        ret.set_specific(ExecutorSpecificStats.bolt(boltStats));

        return ret;
    }
}

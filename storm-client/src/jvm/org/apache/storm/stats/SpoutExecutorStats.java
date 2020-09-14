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

import org.apache.storm.generated.ExecutorSpecificStats;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.SpoutStats;
import org.apache.storm.metric.internal.MultiLatencyStat;

@SuppressWarnings("unchecked")
public class SpoutExecutorStats extends CommonStats {
    private final MultiLatencyStat completeLatencyStats;

    public SpoutExecutorStats(int rate, int numStatBuckets) {
        super(rate, numStatBuckets);
        this.completeLatencyStats = new MultiLatencyStat(numStatBuckets);
    }

    public MultiLatencyStat getCompleteLatencies() {
        return completeLatencyStats;
    }

    @Override
    public void cleanupStats() {
        completeLatencyStats.close();
        super.cleanupStats();
    }

    public void spoutAckedTuple(String stream, long latencyMs) {
        this.getAcked().incBy(stream, this.rate);
        this.getCompleteLatencies().record(stream, latencyMs);
    }

    public void spoutFailedTuple(String stream) {
        this.getFailed().incBy(stream, this.rate);
    }

    @Override
    public ExecutorStats renderStats() {
        ExecutorStats ret = new ExecutorStats();
        // common fields
        ret.set_emitted(valueStat(getEmitted()));
        ret.set_transferred(valueStat(getTransferred()));
        ret.set_rate(this.rate);

        // spout stats
        SpoutStats spoutStats = new SpoutStats(
            valueStat(getAcked()), valueStat(getFailed()), valueStat(completeLatencyStats));
        ret.set_specific(ExecutorSpecificStats.spout(spoutStats));

        return ret;
    }
}

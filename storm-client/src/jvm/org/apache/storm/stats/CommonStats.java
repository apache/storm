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

import java.util.Map;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.metric.internal.MultiCountStat;
import org.apache.storm.metric.internal.MultiLatencyStat;

@SuppressWarnings("unchecked")
public abstract class CommonStats {
    protected final int rate;
    private final MultiCountStat emittedStats;
    private final MultiCountStat transferredStats;
    private final MultiCountStat ackedStats;
    private final MultiCountStat failedStats;

    public CommonStats(int rate, int numStatBuckets) {
        this.rate = rate;
        this.emittedStats = new MultiCountStat(numStatBuckets);
        this.transferredStats = new MultiCountStat(numStatBuckets);
        this.ackedStats = new MultiCountStat(numStatBuckets);
        this.failedStats = new MultiCountStat(numStatBuckets);
    }

    public MultiCountStat getFailed() {
        return failedStats;
    }

    public MultiCountStat getAcked() {
        return ackedStats;
    }

    public int getRate() {
        return this.rate;
    }

    public MultiCountStat getEmitted() {
        return emittedStats;
    }

    public MultiCountStat getTransferred() {
        return transferredStats;
    }

    public void emittedTuple(String stream) {
        this.getEmitted().incBy(stream, this.rate);
    }

    public void transferredTuples(String stream, int amount) {
        this.getTransferred().incBy(stream, this.rate * amount);
    }

    public void cleanupStats() {
        emittedStats.close();
        transferredStats.close();
        ackedStats.close();
        failedStats.close();
    }

    protected Map<String, Map<String, Long>> valueStat(MultiCountStat metric) {
        return metric.getTimeCounts();
    }

    protected Map<String, Map<String, Double>> valueStat(MultiLatencyStat metric) {
        return metric.getTimeLatAvg();
    }

    public abstract ExecutorStats renderStats();

}

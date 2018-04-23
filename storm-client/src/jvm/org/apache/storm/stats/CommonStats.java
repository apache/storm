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
import java.util.Map;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.metric.internal.MultiCountStatAndMetric;
import org.apache.storm.metric.internal.MultiLatencyStatAndMetric;

@SuppressWarnings("unchecked")
public abstract class CommonStats {
    protected final int rate;
    private final MultiCountStatAndMetric emittedStats;
    private final MultiCountStatAndMetric transferredStats;
    private final MultiCountStatAndMetric ackedStats;
    private final MultiCountStatAndMetric failedStats;

    public CommonStats(int rate, int numStatBuckets) {
        this.rate = rate;
        this.emittedStats = new MultiCountStatAndMetric(numStatBuckets);
        this.transferredStats = new MultiCountStatAndMetric(numStatBuckets);
        this.ackedStats = new MultiCountStatAndMetric(numStatBuckets);
        this.failedStats = new MultiCountStatAndMetric(numStatBuckets);
    }

    public MultiCountStatAndMetric getFailed() {
        return failedStats;
    }

    public MultiCountStatAndMetric getAcked() {
        return ackedStats;
    }

    public int getRate() {
        return this.rate;
    }

    public MultiCountStatAndMetric getEmitted() {
        return emittedStats;
    }

    public MultiCountStatAndMetric getTransferred() {
        return transferredStats;
    }

    public void emittedTuple(String stream, Counter emittedCounter) {
        this.getEmitted().incBy(stream, this.rate);
        emittedCounter.inc(this.rate);
    }

    public void transferredTuples(String stream, int amount, Counter transferredCounter) {
        this.getTransferred().incBy(stream, this.rate * amount);
        transferredCounter.inc(amount);
    }

    public void cleanupStats() {
        emittedStats.close();
        transferredStats.close();
        ackedStats.close();
        failedStats.close();
    }

    protected Map<String, Map<String, Long>> valueStat(MultiCountStatAndMetric metric) {
        return metric.getTimeCounts();
    }

    protected Map<String, Map<String, Double>> valueStat(MultiLatencyStatAndMetric metric) {
        return metric.getTimeLatAvg();
    }

    public abstract ExecutorStats renderStats();

}

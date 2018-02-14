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

import java.util.Map;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.metric.internal.MultiCountStatAndMetric;
import org.apache.storm.metric.internal.MultiLatencyStatAndMetric;

@SuppressWarnings("unchecked")
public abstract class CommonStats {

    private final MultiCountStatAndMetric emittedStats;
    private final MultiCountStatAndMetric transferredStats;

    protected final int rate;

    public CommonStats(int rate,int numStatBuckets) {
        this.rate = rate;
        this.emittedStats = new MultiCountStatAndMetric(numStatBuckets);
        this.transferredStats = new MultiCountStatAndMetric(numStatBuckets);
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

    public void emittedTuple(String stream) {
        this.getEmitted().incBy(stream, this.rate);
    }

    public void transferredTuples(String stream, int amount) {
        this.getTransferred().incBy(stream, this.rate * amount);
    }

    public void cleanupStats() {
        emittedStats.close();
        transferredStats.close();
    }

    protected Map valueStat(MultiCountStatAndMetric metric) {
        return metric.getTimeCounts();
    }

    protected Map valueStat(MultiLatencyStatAndMetric metric) {
        return metric.getTimeLatAvg();
    }

    public abstract ExecutorStats renderStats();

}

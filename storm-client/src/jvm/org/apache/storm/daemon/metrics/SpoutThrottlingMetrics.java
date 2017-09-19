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

package org.apache.storm.daemon.metrics;

import org.apache.storm.metric.api.CountMetric;

public class SpoutThrottlingMetrics extends BuiltinMetrics {
    private final CountMetric skippedMaxSpoutMs = new CountMetric();
    private final CountMetric skippedThrottleMs = new CountMetric();
    private final CountMetric skippedInactiveMs = new CountMetric();

    public SpoutThrottlingMetrics() {
        metricMap.put("skipped-max-spout-ms", skippedMaxSpoutMs);
        metricMap.put("skipped-throttle-ms", skippedThrottleMs);
        metricMap.put("skipped-inactive-ms", skippedInactiveMs);
    }

    public void skippedMaxSpoutMs(long ms) {
        this.skippedMaxSpoutMs.incrBy(ms);
    }

    public void skippedThrottleMs(long ms) {
        this.skippedThrottleMs.incrBy(ms);
    }

    public void skippedInactiveMs(long ms) {
        this.skippedInactiveMs.incrBy(ms);
    }
}

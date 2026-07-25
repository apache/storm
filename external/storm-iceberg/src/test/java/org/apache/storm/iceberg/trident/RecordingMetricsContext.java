/*
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

package org.apache.storm.iceberg.trident;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.metric.api.CombinedMetric;
import org.apache.storm.metric.api.ICombiner;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.IReducer;
import org.apache.storm.metric.api.ReducedMetric;
import org.apache.storm.task.IMetricsContext;

/**
 * Minimal {@link IMetricsContext} that keeps the registered metrics in maps so tests can assert on
 * them. Like the real registry, registering the same name twice returns the same instance.
 */
class RecordingMetricsContext implements IMetricsContext {

    private final Map<String, Counter> counters = new HashMap<>();
    private final Map<String, Timer> timers = new HashMap<>();

    long counter(String name) {
        Counter counter = counters.get(name);
        return counter == null ? 0L : counter.getCount();
    }

    long timerCount(String name) {
        Timer timer = timers.get(name);
        return timer == null ? 0L : timer.getCount();
    }

    @Override
    public Counter registerCounter(String name) {
        return counters.computeIfAbsent(name, n -> new Counter());
    }

    @Override
    public Timer registerTimer(String name) {
        return timers.computeIfAbsent(name, n -> new Timer());
    }

    @Override
    public Histogram registerHistogram(String name) {
        throw new UnsupportedOperationException("not used by IcebergState");
    }

    @Override
    public Meter registerMeter(String name) {
        throw new UnsupportedOperationException("not used by IcebergState");
    }

    @Override
    public <T> Gauge<T> registerGauge(String name, Gauge<T> gauge) {
        throw new UnsupportedOperationException("not used by IcebergState");
    }

    @Override
    public void registerMetricSet(String prefix, MetricSet set) {
        throw new UnsupportedOperationException("not used by IcebergState");
    }

    @Override
    @Deprecated
    public <T extends IMetric> T registerMetric(String name, T metric, int timeBucketSizeInSecs) {
        throw new UnsupportedOperationException("not used by IcebergState");
    }

    @Override
    @Deprecated
    public ReducedMetric registerMetric(String name, IReducer reducer, int timeBucketSizeInSecs) {
        throw new UnsupportedOperationException("not used by IcebergState");
    }

    @Override
    @Deprecated
    public CombinedMetric registerMetric(String name, ICombiner combiner, int timeBucketSizeInSecs) {
        throw new UnsupportedOperationException("not used by IcebergState");
    }
}

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

package org.apache.storm.task;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import org.apache.storm.metric.api.CombinedMetric;
import org.apache.storm.metric.api.ICombiner;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.IReducer;
import org.apache.storm.metric.api.ReducedMetric;


public interface IMetricsContext {
    /**
     * Register metric.
     * @deprecated in favor of metrics v2 (the non-deprecated methods on this class)
     */
    @Deprecated
    <T extends IMetric> T registerMetric(String name, T metric, int timeBucketSizeInSecs);

    /**
     * Register metric.
     * @deprecated in favor of metrics v2 (the non-deprecated methods on this class)
     */
    @Deprecated
    ReducedMetric registerMetric(String name, IReducer reducer, int timeBucketSizeInSecs);

    /**
     * Register metric.
     * @deprecated in favor of metrics v2 (the non-deprecated methods on this class)
     */
    @Deprecated
    CombinedMetric registerMetric(String name, ICombiner combiner, int timeBucketSizeInSecs);
    
    Timer registerTimer(String name);

    Histogram registerHistogram(String name);

    Meter registerMeter(String name);

    Counter registerCounter(String name);

    <T> Gauge<T> registerGauge(String name, Gauge<T> gauge);

    void registerMetricSet(String prefix, MetricSet set);
}

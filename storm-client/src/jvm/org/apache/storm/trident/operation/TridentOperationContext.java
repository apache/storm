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

package org.apache.storm.trident.operation;

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
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTupleView.ProjectionFactory;
import org.apache.storm.tuple.Fields;

public class TridentOperationContext implements IMetricsContext {
    TridentTuple.Factory factory;
    TopologyContext topoContext;

    public TridentOperationContext(TopologyContext topoContext, TridentTuple.Factory factory) {
        this.factory = factory;
        this.topoContext = topoContext;
    }

    public TridentOperationContext(TridentOperationContext parent, TridentTuple.Factory factory) {
        this(parent.topoContext, factory);
    }

    public ProjectionFactory makeProjectionFactory(Fields fields) {
        return new ProjectionFactory(factory, fields);
    }

    public int numPartitions() {
        return topoContext.getComponentTasks(topoContext.getThisComponentId()).size();
    }

    public int getPartitionIndex() {
        return topoContext.getThisTaskIndex();
    }

    @Override
    public <T extends IMetric> T registerMetric(String name, T metric, int timeBucketSizeInSecs) {
        return topoContext.registerMetric(name, metric, timeBucketSizeInSecs);
    }

    @Override
    public ReducedMetric registerMetric(String name, IReducer reducer, int timeBucketSizeInSecs) {
        return topoContext.registerMetric(name, new ReducedMetric(reducer), timeBucketSizeInSecs);
    }

    @Override
    public CombinedMetric registerMetric(String name, ICombiner combiner, int timeBucketSizeInSecs) {
        return topoContext.registerMetric(name, new CombinedMetric(combiner), timeBucketSizeInSecs);
    }
    
    @Override
    public Timer registerTimer(String name) {
        return topoContext.registerTimer(name);
    }

    @Override
    public Histogram registerHistogram(String name) {
        return topoContext.registerHistogram(name);
    }

    @Override
    public Meter registerMeter(String name) {
        return topoContext.registerMeter(name);
    }

    @Override
    public Counter registerCounter(String name) {
        return topoContext.registerCounter(name);
    }

    @Override
    public <T> Gauge<T> registerGauge(String name, Gauge<T> gauge) {
        return topoContext.registerGauge(name, gauge);
    }

    @Override
    public void registerMetricSet(String prefix, MetricSet set) {
        topoContext.registerMetricSet(prefix, set);
    }
}

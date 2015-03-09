/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.metricstore;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class containing metric values and all identifying fields to be stored in a MetricStore.
 */
public class Metric implements Comparable<Metric> {
    private static final Logger LOG = LoggerFactory.getLogger(Metric.class);

    // key fields
    private String name;
    private long timestamp;
    private String topologyId;
    private String componentId;
    private String executorId;
    private String hostname;
    private String streamId;
    private int port;
    private AggLevel aggLevel = AggLevel.AGG_LEVEL_NONE;

    // value fields
    private double value;
    private long count = 1;
    private double min = 0.0;
    private double max = 0.0;
    private double sum = 0.0;


    /**
     *  Metric constructor.
     */
    public Metric(String name, Long timestamp, String topologyId, double value, String componentId, String executorId,
                  String hostname, String streamId, int port, AggLevel aggLevel) throws MetricException {
        this.name = name;
        this.timestamp = timestamp;
        this.topologyId = topologyId;
        this.componentId = componentId;
        this.executorId = executorId;
        this.hostname = hostname;
        this.streamId = streamId;
        this.port = port;
        this.setValue(value);
        setAggLevel(aggLevel);
    }

    /**
     *  A Metric constructor with the same settings cloned from another.
     */
    public Metric(Metric o) {
        this.name = o.getMetricName();
        this.timestamp = o.getTimestamp();
        this.topologyId = o.getTopologyId();
        this.value = o.getValue();
        this.componentId = o.getComponentId();
        this.executorId = o.getExecutorId();
        this.hostname = o.getHostname();
        this.streamId = o.getStreamId();
        this.port = o.getPort();
        this.count = o.getCount();
        this.min = o.getMin();
        this.max = o.getMax();
        this.sum = o.getSum();
        this.aggLevel = o.getAggLevel();
    }

    /**
     *  Check if a Metric matches another object.
     */
    @Override
    public boolean equals(Object other) {

        if (!(other instanceof Metric)) {
            return false;
        }

        Metric o = (Metric) other;

        return this == other
               || (this.name.equals(o.getMetricName())
                   && this.timestamp == o.getTimestamp()
                   && this.topologyId.equals(o.getTopologyId())
                   && this.value == o.getValue()
                   && this.componentId.equals(o.getComponentId())
                   && this.executorId.equals(o.getExecutorId())
                   && this.hostname.equals(o.getHostname())
                   && this.streamId.equals(o.getStreamId())
                   && this.port == o.getPort()
                   && this.count == o.getCount()
                   && this.min == o.getMin()
                   && this.max == o.getMax()
                   && this.sum == o.getSum()
                   && this.aggLevel == o.getAggLevel());
    }

    public AggLevel getAggLevel() {
        return this.aggLevel;
    }

    /**
     *  Set the aggLevel.
     */
    public void setAggLevel(AggLevel aggLevel) throws MetricException {
        if (aggLevel == null) {
            throw new MetricException("AggLevel not set for metric");
        }
        this.aggLevel = aggLevel;
    }

    /**
     *  Adds an additional value to the metric.
     */
    public void addValue(double value) {
        this.count += 1;
        this.min = Math.min(this.min, value);
        this.max = Math.max(this.max, value);
        this.sum += value;
        this.value = this.sum / this.count;
    }

    public double getSum() {
        return this.sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public long getCount() {
        return this.count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public double getMin() {
        return this.min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMax() {
        return this.max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public String getTopologyId() {
        return this.topologyId;
    }

    public void setTopologyId(String topologyId) {
        this.topologyId = topologyId;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getValue() {
        return this.value;
    }

    /**
     *  Initialize the metric value.
     */
    public void setValue(double value) {
        this.count = 1L;
        this.min = value;
        this.max = value;
        this.sum = value;
        this.value = value;
    }

    public String getMetricName() {
        return this.name;
    }

    public String getComponentId() {
        return this.componentId;
    }

    public String getExecutorId() {
        return this.executorId;
    }

    public String getHostname() {
        return this.hostname;
    }

    public String getStreamId() {
        return this.streamId;
    }

    public Integer getPort() {
        return this.port;
    }

    @Override
    public int compareTo(Metric o) {
        long a = this.getTimestamp();
        long b = o.getTimestamp();
        return Long.compare(a, b);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        Date date = new Date(this.timestamp);
        DateFormat format = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SSS");
        format.setTimeZone(TimeZone.getTimeZone("UTC"));
        sb.append(format.format(date));
        sb.append("|");
        sb.append(this.topologyId);
        sb.append("|");
        sb.append(aggLevel);
        sb.append("|");
        sb.append(this.name);
        sb.append("|");
        sb.append(this.componentId);
        sb.append("|");
        sb.append(this.executorId);
        sb.append("|");
        sb.append(this.hostname);
        sb.append("|");
        sb.append(this.port);
        sb.append("|");
        sb.append(this.streamId);
        return String.format("%s -- count: %d -- value: %f -- min: %f -- max: %f -- sum: %f",
                             sb.toString(),
                             this.count,
                             this.value,
                             this.min,
                             this.max,
                             this.sum);
    }
}


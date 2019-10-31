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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.storm.opentsdb;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * This class represents a metric data point in OpenTSDB's format.
 */
public class OpenTsdbMetricDatapoint implements Serializable {

    // metric name
    private final String metric;

    // map of tag value pairs
    private final Map<String, String> tags;

    // timestamp either in milliseconds or seconds at which this metric is occurred.
    private final long timestamp;

    // value of the metric
    private final Number value;

    // required for jackson serialization
    private OpenTsdbMetricDatapoint() {
        this(null, null, 0L, null);
    }

    public OpenTsdbMetricDatapoint(String metric, Map<String, String> tags, long timestamp, Number value) {
        this.metric = metric;
        this.tags = Collections.unmodifiableMap(tags);
        this.timestamp = timestamp;
        this.value = value;

        if (!(value instanceof Integer || value instanceof Long || value instanceof Float)) {
            throw new RuntimeException("Received tuple contains unsupported value: " + value + " field. It must be Integer/Long/Float.");
        }

    }

    /**
     * Retrieve the metric name of this datapoint.
     * @return metric name of this datapoint
     */
    public String getMetric() {
        return metric;
    }

    /**
     * Retrieve the map of tag/value pairs of this metric.
     * @return Map of tag/value pairs of this metric
     */
    public Map<String, String> getTags() {
        return tags;
    }

    /**
     * Retrieve the timestamp at which this metric occured.
     * @return timestamp either in milliseconds or seconds at which this metric occurred
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Retrieve the value of this metric datapoint.
     * @return value of this metric datapoint
     */
    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "OpenTsdbMetricDataPoint{"
                + "metric='" + metric + '\''
                + ", tags=" + tags
                + ", timestamp=" + timestamp
                + ", value=" + value
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OpenTsdbMetricDatapoint)) {
            return false;
        }

        OpenTsdbMetricDatapoint that = (OpenTsdbMetricDatapoint) o;

        if (timestamp != that.timestamp) {
            return false;
        }
        if (value != that.value) {
            return false;
        }
        if (!metric.equals(that.metric)) {
            return false;
        }
        return tags.equals(that.tags);

    }

    @Override
    public int hashCode() {
        int result = metric.hashCode();
        result = 31 * result + tags.hashCode();
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + value.hashCode();
        return result;
    }
}

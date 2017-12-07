/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.storm.metricstore;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * FilterOptions provides a method to select various filtering options for doing a scan of the metrics database.
 */
public class FilterOptions {
    private static final String componentId = "componentId";
    private static final String topologyId = "topologyId";
    private static final String startTime = "startTime";
    private static final String endTime = "endTime";
    private static final String metricName = "metricName";
    private static final String executorId = "executorId";
    private static final String hostId = "hostId";
    private static final String port = "port";
    private static final String streamId = "streamId";
    private Map<String, Object> options = new HashMap<>();
    private Set<AggLevel> aggLevels = null;

    public FilterOptions() {
        this.options.put(startTime, 0L);
    }

    public void setTopologyId(String topoId) {
        this.options.put(topologyId, topoId);
    }

    public String getTopologyId() {
        return (String)this.options.get(topologyId);
    }

    public void setComponentId(String component) {
        this.options.put(componentId, component);
    }

    public String getComponentId() {
        return (String)this.options.get(componentId);
    }

    public void setStartTime(Long time) {
        this.options.put(startTime, time);
    }

    public Long getStartTime() {
        return (Long)this.options.get(startTime);
    }

    public void setEndTime(Long time) {
        this.options.put(endTime, time);
    }

    /**
     *  Returns the end time if set, returns the current time otherwise.
     */
    public Long getEndTime() {
        Long time = (Long)this.options.get(endTime);
        if (time == null) {
            time = System.currentTimeMillis();
        }
        return time;
    }

    public void setMetricName(String name) {
        this.options.put(metricName, name);
    }

    public String getMetricName() {
        return (String)this.options.get(metricName);
    }

    public void setExecutorId(String id) {
        this.options.put(executorId, id);
    }

    public String getExecutorId() {
        return (String)this.options.get(executorId);
    }

    public void setHostId(String id) {
        this.options.put(hostId, id);
    }

    public String getHostId() {
        return (String)this.options.get(hostId);
    }

    public void setPort(Integer p) {
        this.options.put(port, p);
    }

    public Integer getPort() {
        return (Integer)this.options.get(port);
    }

    public void setStreamId(String id) {
        this.options.put(streamId, id);
    }

    public String getStreamId() {
        return (String)this.options.get(streamId);
    }

    /**
     *  Add an aggregation level to search for.
     */
    public void addAggLevel(AggLevel level) {
        if (this.aggLevels == null) {
            this.aggLevels = new HashSet<>(1);
        }
        this.aggLevels.add(level);
    }

    /**
     *  Set the aggregation levels to search for.
     */
    public void setAggLevels(Set<AggLevel> levels) throws MetricException {
        this.aggLevels = levels;
        if (this.aggLevels == null || this.aggLevels.isEmpty()) {
            throw new MetricException("Cannot search for empty AggLevel");
        }
    }

    /**
     *  Get the aggregation levels to search for.
     */
    public Set<AggLevel> getAggLevels() {
        if (this.aggLevels == null) {
            // assume filter choices have been made and since no selection was made, all levels are valid
            this.aggLevels = new HashSet<>(4);
            aggLevels.add(AggLevel.AGG_LEVEL_NONE);
            aggLevels.add(AggLevel.AGG_LEVEL_1_MIN);
            aggLevels.add(AggLevel.AGG_LEVEL_10_MIN);
            aggLevels.add(AggLevel.AGG_LEVEL_60_MIN);
        }
        return this.aggLevels;
    }
}

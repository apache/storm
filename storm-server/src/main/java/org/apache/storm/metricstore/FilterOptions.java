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

import java.util.HashSet;
import java.util.Set;

/**
 * FilterOptions provides a method to select various filtering options for doing a scan of the metrics database.
 */
public class FilterOptions {
    private Set<AggLevel> aggLevels = null;
    private long startTime = 0L;
    private long endTime = -1L;
    private String topologyId = null;
    private String componentId = null;
    private String metricName = null;
    private String executorId = null;
    private String hostId = null;
    private Integer port = null;
    private String streamId = null;

    public FilterOptions() {
    }

    public String getTopologyId() {
        return this.topologyId;
    }

    public void setTopologyId(String topoId) {
        this.topologyId = topoId;
    }

    public String getComponentId() {
        return this.componentId;
    }

    public void setComponentId(String component) {
        this.componentId = component;
    }

    public long getStartTime() {
        return this.startTime;
    }

    public void setStartTime(Long time) {
        this.startTime = time;
    }

    /**
     *  Returns the end time if set, returns the current time otherwise.
     */
    public long getEndTime() {
        if (this.endTime < 0L) {
            this.endTime = System.currentTimeMillis();
        }
        return this.endTime;
    }

    public void setEndTime(Long time) {
        this.endTime = time;
    }

    public String getMetricName() {
        return this.metricName;
    }

    public void setMetricName(String name) {
        this.metricName = name;
    }

    public String getExecutorId() {
        return this.executorId;
    }

    public void setExecutorId(String id) {
        this.executorId = id;
    }

    public String getHostId() {
        return this.hostId;
    }

    public void setHostId(String id) {
        this.hostId = id;
    }

    public Integer getPort() {
        return this.port;
    }

    public void setPort(Integer p) {
        this.port = p;
    }

    public String getStreamId() {
        return this.streamId;
    }

    public void setStreamId(String id) {
        this.streamId = id;
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

    /**
     *  Set the aggregation levels to search for.
     */
    public void setAggLevels(Set<AggLevel> levels) throws MetricException {
        this.aggLevels = levels;
        if (this.aggLevels == null || this.aggLevels.isEmpty()) {
            throw new MetricException("Cannot search for empty AggLevel");
        }
    }
}

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

package org.apache.storm.metrics2;

import java.util.HashMap;
import java.util.Map;

/**
 * Class to store task-related V2 metrics dimensions.
 */
public class TaskMetricDimensions {
    private int taskId;
    private String componentId;
    private String streamId;
    private Map<String, String> dimensions = new HashMap<>();

    public TaskMetricDimensions(int taskId, String componentId, String streamId, StormMetricRegistry metricRegistry) {
        this.taskId = taskId;
        dimensions.put("taskid", Integer.toString(this.taskId));

        this.componentId = componentId;
        if (this.componentId == null) {
            this.componentId = "";
        } else {
            dimensions.put("componentId", this.componentId);
        }

        this.streamId = streamId;
        if (this.streamId == null) {
            this.streamId = "";
        } else {
            dimensions.put("streamId", this.streamId);
        }

        dimensions.put("hostname", metricRegistry.getHostName());
        dimensions.put("topologyId", metricRegistry.getTopologyId());
        dimensions.put("port", metricRegistry.getPort().toString());
    }

    public Map<String, String> getDimensions() {
        return dimensions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TaskMetricDimensions otherD = (TaskMetricDimensions) o;
        return (taskId == otherD.taskId && componentId.equals(otherD.componentId)
                && streamId.equals(otherD.streamId));
    }

    @Override
    public int hashCode() {
        return taskId + componentId.hashCode() + streamId.hashCode();
    }
}
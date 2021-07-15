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

package org.apache.storm.metric.api;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;

public interface IMetricsConsumer {
    void prepare(Map<String, Object> topoConf, Object registrationArgument, TopologyContext context, IErrorReporter errorReporter);

    void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints);

    void cleanup();

    class TaskInfo {
        public String srcWorkerHost;
        public int srcWorkerPort;
        public String srcComponentId;
        public int srcTaskId;
        public long timestamp;
        public int updateIntervalSecs;

        public TaskInfo() {
        }

        public TaskInfo(String srcWorkerHost, int srcWorkerPort, String srcComponentId, int srcTaskId, long timestamp,
                        int updateIntervalSecs) {
            this.srcWorkerHost = srcWorkerHost;
            this.srcWorkerPort = srcWorkerPort;
            this.srcComponentId = srcComponentId;
            this.srcTaskId = srcTaskId;
            this.timestamp = timestamp;
            this.updateIntervalSecs = updateIntervalSecs;
        }

        @Override
        public String toString() {
            return "TASK_INFO: { host: " + srcWorkerHost
                    + ":" + srcWorkerPort
                    + " comp: " + srcComponentId
                    + "[" + srcTaskId + "]}";
        }
    }

    // We can't move this to outside without breaking backward compatibility.
    class DataPoint {
        public String name;
        public Object value;
        public Map<String, String> dimensions;

        public DataPoint() {
        }

        public DataPoint(String name, Object value) {
            this(name, value, Collections.emptyMap());
        }

        public DataPoint(String name, Object value, Map<String, String> dimensions) {
            this.name = name;
            this.value = value;
            this.dimensions = dimensions;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            sb.append(name);
            sb.append("=");
            sb.append(value);
            if (!dimensions.isEmpty()) {
                sb.append(", ");
                sb.append(dimensions.toString());
            }
            sb.append("]");
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof DataPoint)) {
                return false;
            }

            DataPoint dataPoint = (DataPoint) o;

            return Objects.equals(name, dataPoint.name)
                && Objects.deepEquals(value, dataPoint.value)
                && Objects.deepEquals(dimensions, dataPoint.dimensions);
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (value != null ? value.hashCode() : 0);
            result = 31 * result + (dimensions != null ? dimensions.hashCode() : 0);
            return result;
        }
    }
}

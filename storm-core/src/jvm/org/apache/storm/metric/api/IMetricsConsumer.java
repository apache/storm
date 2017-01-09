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
package org.apache.storm.metric.api;

import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

public interface IMetricsConsumer {
    public static class TaskInfo {
        public TaskInfo() {}
        public TaskInfo(String srcWorkerHost, int srcWorkerPort, String srcComponentId, int srcTaskId, long timestamp, int updateIntervalSecs) {
            this.srcWorkerHost = srcWorkerHost;
            this.srcWorkerPort = srcWorkerPort;
            this.srcComponentId = srcComponentId; 
            this.srcTaskId = srcTaskId; 
            this.timestamp = timestamp;
            this.updateIntervalSecs = updateIntervalSecs; 
        }
        public String srcWorkerHost;
        public int srcWorkerPort;
        public String srcComponentId; 
        public int srcTaskId; 
        public long timestamp;
        public int updateIntervalSecs;

        @Override
        public String toString() {
            return "TaskInfo{" +
                    "srcWorkerHost='" + srcWorkerHost + '\'' +
                    ", srcWorkerPort=" + srcWorkerPort +
                    ", srcComponentId='" + srcComponentId + '\'' +
                    ", srcTaskId=" + srcTaskId +
                    ", timestamp=" + timestamp +
                    ", updateIntervalSecs=" + updateIntervalSecs +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TaskInfo)) return false;

            TaskInfo taskInfo = (TaskInfo) o;

            if (srcWorkerPort != taskInfo.srcWorkerPort) return false;
            if (srcTaskId != taskInfo.srcTaskId) return false;
            if (timestamp != taskInfo.timestamp) return false;
            if (updateIntervalSecs != taskInfo.updateIntervalSecs) return false;
            if (srcWorkerHost != null ? !srcWorkerHost.equals(taskInfo.srcWorkerHost) : taskInfo.srcWorkerHost != null)
                return false;
            return srcComponentId != null ? srcComponentId.equals(taskInfo.srcComponentId) : taskInfo.srcComponentId == null;

        }

        @Override
        public int hashCode() {
            int result = srcWorkerHost != null ? srcWorkerHost.hashCode() : 0;
            result = 31 * result + srcWorkerPort;
            result = 31 * result + (srcComponentId != null ? srcComponentId.hashCode() : 0);
            result = 31 * result + srcTaskId;
            result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
            result = 31 * result + updateIntervalSecs;
            return result;
        }
    }

    // We can't move this to outside without breaking backward compatibility.
    public static class DataPoint {
        public DataPoint() {}
        public DataPoint(String name, Object value) {
            this.name = name;
            this.value = value;
        }
        @Override
        public String toString() {
            return "[" + name + " = " + value + "]";
        }
        public String name; 
        public Object value;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof DataPoint)) return false;

            DataPoint dataPoint = (DataPoint) o;

            return Objects.equals(name, dataPoint.name) && Objects.deepEquals(value, dataPoint.value);
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (value != null ? value.hashCode() : 0);
            return result;
        }
    }

    void prepare(Map stormConf, Object registrationArgument, TopologyContext context, IErrorReporter errorReporter);
    void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints);
    void cleanup();
}

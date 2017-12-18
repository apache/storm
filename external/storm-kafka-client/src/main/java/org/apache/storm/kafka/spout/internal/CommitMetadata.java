/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.storm.kafka.spout.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Object representing metadata committed to Kafka.
 */
public class CommitMetadata {
    private final String topologyId;
    private final int taskId;
    private final String threadName;

    /** Kafka metadata. */
    @JsonCreator
    public CommitMetadata(@JsonProperty("topologyId") String topologyId,
                          @JsonProperty("taskId") int taskId,
                          @JsonProperty("threadName") String threadName) {

        this.topologyId = topologyId;
        this.taskId = taskId;
        this.threadName = threadName;
    }

    public String getTopologyId() {
        return topologyId;
    }

    public int getTaskId() {
        return taskId;
    }

    public String getThreadName() {
        return threadName;
    }

    @Override
    public String toString() {
        return "CommitMetadata{"
            + "topologyId='" + topologyId + '\''
            + ", taskId=" + taskId
            + ", threadName='" + threadName + '\''
            + '}';
    }
}

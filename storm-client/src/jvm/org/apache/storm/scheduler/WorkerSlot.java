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

package org.apache.storm.scheduler;

import java.util.Arrays;
import java.util.List;

public class WorkerSlot {
    private final String nodeId;
    private final int port;

    public WorkerSlot(String nodeId, Number port) {
        if (port == null) {
            throw new NullPointerException("port cannot be null");
        }
        if (nodeId == null) {
            throw new NullPointerException("node id cannot be null");
        }
        this.nodeId = nodeId;
        this.port = port.intValue();
    }

    public String getNodeId() {
        return nodeId;
    }

    public int getPort() {
        return port;
    }

    public String getId() {
        return getNodeId() + ":" + getPort();
    }

    public List<Object> toList() {
        return Arrays.asList(nodeId, (long) port);
    }

    @Override
    public int hashCode() {
        return nodeId.hashCode() + 13 * ((Integer) port).hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof WorkerSlot)) {
            return false;
        }
        WorkerSlot other = (WorkerSlot) o;
        return this.port == other.port && this.nodeId.equals(other.nodeId);
    }

    @Override
    public String toString() {
        return getId();
    }
}

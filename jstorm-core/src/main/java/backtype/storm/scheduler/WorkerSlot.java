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
package backtype.storm.scheduler;

import java.io.Serializable;

public class WorkerSlot implements Comparable<WorkerSlot>, Serializable {
    
    private static final long serialVersionUID = -4451854497340313268L;
    String nodeId;
    int port;
    
    public WorkerSlot(String nodeId, Number port) {
        this.nodeId = nodeId;
        this.port = port.intValue();
    }
    
    public WorkerSlot() {
        
    }
    
    public String getNodeId() {
        return nodeId;
    }
    
    public int getPort() {
        return port;
    }
    
    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }
    
    public void setPort(int port) {
        this.port = port;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((nodeId == null) ? 0 : nodeId.hashCode());
        result = prime * result + port;
        return result;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        WorkerSlot other = (WorkerSlot) obj;
        if (nodeId == null) {
            if (other.nodeId != null)
                return false;
        } else if (!nodeId.equals(other.nodeId))
            return false;
        if (port != other.port)
            return false;
        return true;
    }
    
    @Override
    public String toString() {
        return this.nodeId + ":" + this.port;
    }
    
    @Override
    public int compareTo(WorkerSlot o) {
        String otherNode = o.getNodeId();
        if (nodeId == null) {
            if (otherNode != null) {
                return -1;
            } else {
                return port - o.getPort();
            }
        } else {
            int ret = nodeId.compareTo(otherNode);
            if (ret == 0) {
                return port - o.getPort();
            } else {
                return ret;
            }
        }
    }
}

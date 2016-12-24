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
package org.apache.storm.daemon.nimbus;

public final class TopologyResources {
    private final Double requestedMemOnHeap;
    private final Double requestedMemOffHeap;
    private final Double requestedCpu;
    private final Double assignedMemOnHeap;
    private final Double assignedMemOffHeap;
    private final Double assignedCpu;
    
    public TopologyResources(Double requestedMemOnHeap, Double requestedMemOffHeap,
            Double requestedCpu, Double assignedMemOnHeap, Double assignedMemOffHeap,
            Double assignedCpu) {
                this.requestedMemOnHeap = requestedMemOnHeap;
                this.requestedMemOffHeap = requestedMemOffHeap;
                this.requestedCpu = requestedCpu;
                this.assignedMemOnHeap = assignedMemOnHeap;
                this.assignedMemOffHeap = assignedMemOffHeap;
                this.assignedCpu = assignedCpu;
        
    }

    public Double getRequestedMemOnHeap() {
        return requestedMemOnHeap;
    }

    public Double getRequestedMemOffHeap() {
        return requestedMemOffHeap;
    }

    public Double getRequestedCpu() {
        return requestedCpu;
    }

    public Double getAssignedMemOnHeap() {
        return assignedMemOnHeap;
    }

    public Double getAssignedMemOffHeap() {
        return assignedMemOffHeap;
    }

    public Double getAssignedCpu() {
        return assignedCpu;
    }
}
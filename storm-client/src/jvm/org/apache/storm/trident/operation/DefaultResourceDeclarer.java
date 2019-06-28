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

package org.apache.storm.trident.operation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.storm.Config;
import org.apache.storm.generated.SharedMemory;
import org.apache.storm.topology.ResourceDeclarer;

/**
 * Default implementation of resources declarer.
 * @param <T> Must always be the type of the extending class. i.e. <code>public class SubResourceDeclarer extends
 *          DefaultResourceDeclarer&lt;SubResourceDeclarer&gt; {...}</code>
 */
public class DefaultResourceDeclarer<T extends DefaultResourceDeclarer> implements ResourceDeclarer<T>, ITridentResource {

    //@{link org.apache.storm.trident.planner.Node} and several other trident classes inherit from DefaultResourceDeclarer
    // These classes are serialized out as part of the bolts and spouts of a topology, often for each bolt/spout in the topology.
    // The following are marked as transient because they are never used after the topology is created so keeping them around just wastes
    // space in the serialized topology
    private final transient Map<String, Number> resources = new HashMap<>();
    private final transient Set<SharedMemory> sharedMemory = new HashSet<>();

    @Override
    public T setMemoryLoad(Number onHeap) {
        if (onHeap != null) {
            onHeap = onHeap.doubleValue();
            resources.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, onHeap);
        }
        return (T) this;
    }

    @Override
    public T setMemoryLoad(Number onHeap, Number offHeap) {
        setMemoryLoad(onHeap);

        if (offHeap != null) {
            offHeap = offHeap.doubleValue();
            resources.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, offHeap);
        }
        return (T) this;
    }

    @Override
    public T setCPULoad(Number amount) {
        if (amount != null) {
            amount = amount.doubleValue();
            resources.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, amount);
        }
        return (T) this;
    }

    @Override
    public Map<String, Number> getResources() {
        return new HashMap<String, Number>(resources);
    }

    @Override
    public Set<SharedMemory> getSharedMemory() {
        return sharedMemory;
    }

    @Override
    public T addSharedMemory(SharedMemory request) {
        sharedMemory.add(request);
        return (T) this;
    }
}

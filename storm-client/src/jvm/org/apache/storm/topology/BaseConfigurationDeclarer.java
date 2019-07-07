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

package org.apache.storm.topology;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.Config;

public abstract class BaseConfigurationDeclarer<T extends ComponentConfigurationDeclarer> implements ComponentConfigurationDeclarer<T> {
    @Override
    public T addConfiguration(String config, Object value) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(config, value);
        return addConfigurations(configMap);
    }

    @Override
    public T setDebug(boolean debug) {
        return addConfiguration(Config.TOPOLOGY_DEBUG, debug);
    }

    @Override
    public T setMaxTaskParallelism(Number val) {
        if (val != null) {
            val = val.intValue();
        }
        return addConfiguration(Config.TOPOLOGY_MAX_TASK_PARALLELISM, val);
    }

    @Override
    public T setMaxSpoutPending(Number val) {
        if (val != null) {
            val = val.intValue();
        }
        return addConfiguration(Config.TOPOLOGY_MAX_SPOUT_PENDING, val);
    }

    @Override
    public T setNumTasks(Number val) {
        if (val != null) {
            val = val.intValue();
        }
        return addConfiguration(Config.TOPOLOGY_TASKS, val);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T setMemoryLoad(Number onHeap) {
        if (onHeap != null) {
            onHeap = onHeap.doubleValue();
            addResource(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, onHeap);
            return addConfiguration(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, onHeap);
        }
        return (T) this;
    }

    @Override
    public T setMemoryLoad(Number onHeap, Number offHeap) {
        @SuppressWarnings("unchecked")
        T ret = (T) this;
        ret = setMemoryLoad(onHeap);

        if (offHeap != null) {
            offHeap = offHeap.doubleValue();
            addResource(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, offHeap);
            ret = addConfiguration(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, offHeap);
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T setCPULoad(Number amount) {
        if (amount != null) {
            addResource(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, amount);
            return addConfiguration(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, amount);
        }
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T addResource(String resourceName, Number resourceValue) {
        Map<String, Double> resourcesMap = (Map<String, Double>) getComponentConfiguration()
                .computeIfAbsent(Config.TOPOLOGY_COMPONENT_RESOURCES_MAP,
                    (x) -> new HashMap<String, Double>());
        resourcesMap.put(resourceName, resourceValue.doubleValue());

        return (T) this;
    }

    /**
     * Add generic resources for this component.
     */
    @Override
    public T addResources(Map<String, Double> resources) {
        if (resources != null) {
            Map<String, Double> currentResources = (Map<String, Double>) getComponentConfiguration().computeIfAbsent(
                Config.TOPOLOGY_COMPONENT_RESOURCES_MAP, (k) -> new HashMap<>());
            currentResources.putAll(resources);
        }
        return (T) this;
    }


}

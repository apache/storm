/*
 * Copyright 2018 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.scheduler.resource.normalization;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.storm.Config;
import org.apache.storm.Constants;

/**
 * Provides resource name normalization for resource maps.
 */
public class ResourceNameNormalizer {

    private final Map<String, String> resourceNameMapping;

    /**
     * Creates a new resource name normalizer.
     */
    public ResourceNameNormalizer() {
        Map<String, String> tmp = new HashMap<>();
        tmp.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, Constants.COMMON_CPU_RESOURCE_NAME);
        tmp.put(Config.SUPERVISOR_CPU_CAPACITY, Constants.COMMON_CPU_RESOURCE_NAME);
        tmp.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, Constants.COMMON_ONHEAP_MEMORY_RESOURCE_NAME);
        tmp.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, Constants.COMMON_OFFHEAP_MEMORY_RESOURCE_NAME);
        tmp.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, Constants.COMMON_TOTAL_MEMORY_RESOURCE_NAME);
        resourceNameMapping = Collections.unmodifiableMap(tmp);
    }

    /**
     * Normalizes a supervisor resource map or topology details map's keys to universal resource names.
     *
     * @param resourceMap resource map of either Supervisor or Topology
     * @return the resource map with common resource names
     */
    public Map<String, Double> normalizedResourceMap(Map<String, ? extends Number> resourceMap) {
        if (resourceMap == null) {
            return new HashMap<>();
        }
        return new HashMap<>(resourceMap.entrySet().stream()
                                        .collect(Collectors.toMap(
                                            //Map the key if needed
                                            (e) -> resourceNameMapping.getOrDefault(e.getKey(), e.getKey()),
                                            //Map the value
                                            (e) -> e.getValue().doubleValue())));
    }

    public Map<String, String> getResourceNameMapping() {
        return resourceNameMapping;
    }

}

/*
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

package org.apache.storm.scheduler.resource;

import static org.apache.storm.Constants.COMMON_CPU_RESOURCE_NAME;
import static org.apache.storm.Constants.COMMON_OFFHEAP_MEMORY_RESOURCE_NAME;
import static org.apache.storm.Constants.COMMON_ONHEAP_MEMORY_RESOURCE_NAME;
import static org.apache.storm.Constants.COMMON_TOTAL_MEMORY_RESOURCE_NAME;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.generated.WorkerResources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resources that have been normalized.
 */
public abstract class NormalizedResources {
    private static final Logger LOG = LoggerFactory.getLogger(NormalizedResources.class);
    public static final Map<String, String> RESOURCE_NAME_MAPPING;

    static {
        Map<String, String> tmp = new HashMap<>();
        tmp.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, COMMON_CPU_RESOURCE_NAME);
        tmp.put(Config.SUPERVISOR_CPU_CAPACITY, COMMON_CPU_RESOURCE_NAME);
        tmp.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, COMMON_ONHEAP_MEMORY_RESOURCE_NAME);
        tmp.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, COMMON_OFFHEAP_MEMORY_RESOURCE_NAME);
        tmp.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, COMMON_TOTAL_MEMORY_RESOURCE_NAME);
        RESOURCE_NAME_MAPPING = Collections.unmodifiableMap(tmp);
    }

    private static Map<String, Double> filterCommonResources(Map<String, Double> normalizedResources) {
        HashMap<String, Double> otherResources = new HashMap<>(normalizedResources);
        otherResources.keySet().removeIf(key -> 
            //We are going to filter out CPU and Memory, because they are captured elsewhere
            COMMON_CPU_RESOURCE_NAME.equals(key)
            || COMMON_TOTAL_MEMORY_RESOURCE_NAME.equals(key)
            || COMMON_OFFHEAP_MEMORY_RESOURCE_NAME.equals(key)
            || COMMON_ONHEAP_MEMORY_RESOURCE_NAME.equals(key));
        return otherResources;
    }

    private double cpu;
    private final Map<String, Double> otherResources;
    private final Map<String, Double> normalizedResources;

    /**
     * Copy constructor.
     */
    public NormalizedResources(NormalizedResources other) {
        cpu = other.cpu;
        otherResources = new HashMap<>(other.otherResources);
        normalizedResources = new HashMap<>(other.normalizedResources);
    }

    /**
     * Create a new normalized set of resources.  Note that memory is not
     * covered here because it is not consistent in requests vs offers because
     * of how on heap vs off heap is used.
     * @param resources the resources to be normalized.
     * @param defaults the default resources that will also be normalized and combined with the real resources.
     */
    public NormalizedResources(Map<String, ? extends Number> resources, Map<String, ? extends Number> defaults) {
        normalizedResources = normalizedResourceMap(defaults);
        normalizedResources.putAll(normalizedResourceMap(resources));
        cpu = normalizedResources.getOrDefault(Constants.COMMON_CPU_RESOURCE_NAME, 0.0);
        otherResources = filterCommonResources(normalizedResources);
    }

    protected final Map<String, Double> getNormalizedResources() {
        return this.normalizedResources;
    }

    /**
     * Normalizes a supervisor resource map or topology details map's keys to universal resource names.
     * @param resourceMap resource map of either Supervisor or Topology
     * @return the resource map with common resource names
     */
    public static Map<String, Double> normalizedResourceMap(Map<String, ? extends Number> resourceMap) {
        if (resourceMap == null) {
            return new HashMap<>();
        }
        return new HashMap<>(resourceMap.entrySet().stream()
            .collect(Collectors.toMap(
                //Map the key if needed
                (e) -> RESOURCE_NAME_MAPPING.getOrDefault(e.getKey(), e.getKey()),
                //Map the value
                (e) -> e.getValue().doubleValue())));
    }

    /**
     * Get the total amount of memory.
     * @return the total amount of memory requested or provided.
     */
    public abstract double getTotalMemoryMb();

    /**
     * Get the total amount of cpu.
     * @return the amount of cpu.
     */
    public double getTotalCpu() {
        return cpu;
    }

    private void add(Map<String, Double> resources) {
        for (Entry<String, Double> resource : resources.entrySet()) {
            this.otherResources.compute(resource.getKey(), (key, value) -> value != null 
                ? resource.getValue() + value
                : resource.getValue());
        }
    }

    public void add(NormalizedResources other) {
        this.cpu += other.cpu;
        add(other.otherResources);
    }

    /**
     * Add the resources from a worker to this.
     * @param value the worker resources that should be added to this.
     */
    public void add(WorkerResources value) {
        Map<String, Double> normalizedResources = value.get_resources();
        cpu += normalizedResources.getOrDefault(Constants.COMMON_CPU_RESOURCE_NAME, 0.0);
        add(filterCommonResources(normalizedResources));
    }

    /**
     * Remove the other resources from this.  This is the same as subtracting the resources in other from this.
     * @param other the resources we want removed.
     */
    public void remove(NormalizedResources other) {
        this.cpu -= other.cpu;
        assert cpu >= 0.0;
        for (Entry<String, Double> resource : other.otherResources.entrySet()) {
            this.otherResources.compute(resource.getKey(), (key, value) -> {
                double res;
                if (value == null) {
                    res = -resource.getValue();
                } else {
                    res = value - resource.getValue();
                }
                assert res >= 0.0;
                return res;
            });
        }
    }

    @Override
    public String toString() {
        return "CPU: " + cpu + " Other resources: " + otherResources;
    }

    /**
     * Return a Map of the normalized resource name to a double.  This should only
     * be used when returning thrift resource requests to the end user.
     */
    public Map<String,Double> toNormalizedMap() {
        HashMap<String, Double> ret = new HashMap<>();
        ret.put(Constants.COMMON_CPU_RESOURCE_NAME, cpu);
        //TODO: Are the other three filtered resources missing (e.g. memory)? 
        ret.putAll(otherResources);
        return ret;
    }

    /**
     * A simple sanity check to see if all of the resources in this would be large enough to hold the resources in other ignoring memory.
     * It does not check memory because with shared memory it is beyond the scope of this.
     * @param other the resources that we want to check if they would fit in this.
     * @return true if it might fit, else false if it could not possibly fit.
     */
    public boolean couldHoldIgnoringMemory(NormalizedResources other) {
        if (this.cpu < other.getTotalCpu()) {
            return false;
        }
        Set<String> allResourceNames = new HashSet<>();
        allResourceNames.addAll(this.otherResources.keySet());
        allResourceNames.addAll(other.otherResources.keySet());
        for (String resourceName : allResourceNames) {
            if (this.otherResources.getOrDefault(resourceName, 0.0) < other.otherResources.getOrDefault(resourceName, 0.0)) {
                return false;
            }
        }
        return true;
    }
    
    private void throwBecauseResourceIsMissingFromTotal(String resourceName) {
        throw new IllegalArgumentException("Total resources does not contain resource '"
            + resourceName
            + "'. All resources should be represented in the total. This is likely a bug in the Storm code");
    }
    
    /**
     * Calculate the average resource usage percentage with this being the total resources and
     * used being the amounts used.
     * @param used the amount of resources used.
     * @return the average percentage used 0.0 to 100.0. Clamps to 100.0 in case there are no available resources in the total.
     */
    public double calculateAveragePercentageUsedBy(NormalizedResources used) { 
        int skippedResourceTypes = 0;
        double total = 0.0;
        double totalMemory = getTotalMemoryMb();
        if (totalMemory != 0.0) {
            total += used.getTotalMemoryMb() / totalMemory;
        } else {
            skippedResourceTypes++;
        }
        double totalCpu = getTotalCpu();
        if (totalCpu != 0.0) {
            total += used.getTotalCpu() / getTotalCpu();
        } else {
            skippedResourceTypes++;
        }
        LOG.trace("Calculating avg percentage used by. Used CPU: {} Total CPU: {} Used Mem: {} Total Mem: {}"
            + " Other Used: {} Other Total: {}", totalCpu, used.getTotalCpu(), totalMemory, used.getTotalMemoryMb(),
            this.otherResources, used.otherResources);
        
        Set<String> allResourceNames = new HashSet<>();
        allResourceNames.addAll(this.otherResources.keySet());
        allResourceNames.addAll(used.otherResources.keySet());
        
        for (String resourceName : allResourceNames) {
            Double totalValue = this.otherResources.get(resourceName);
            Double usedValue = used.otherResources.get(resourceName);
            if (totalValue == null) {
                throwBecauseResourceIsMissingFromTotal(resourceName);
            } 
            if (totalValue == 0.0) {
                //Skip any resources where the total is 0, we should fall back to prioritizing by cpu and memory in that case
                skippedResourceTypes++;
                continue;
            }
            if (usedValue == null) {
                usedValue = 0.0;
            }
            total += usedValue / totalValue;
        }
        //To get the count we divide by we take two for cpu and memory, and one for each included resource.
        int divisor = 2 + allResourceNames.size() - skippedResourceTypes;
        if (divisor == 0) {
            /*This is an arbitrary choice to make the result consistent with calculateMin.
             Any value would be valid here, becase there are no resources in the total set of resources,
             so we're trying to average 0 values.
             */
            return 100.0;
        } else {
            return divisor;
        }
    }

    /**
     * Calculate the minimum resource usage percentage with this being the total resources and
     * used being the amounts used.
     * @param used the amount of resources used.
     * @return the minimum percentage used 0.0 to 100.0. Clamps to 100.0 in case there are no available resources in the total.
     */
    public double calculateMinPercentageUsedBy(NormalizedResources used) {
        double totalMemory = getTotalMemoryMb();
        double totalCpu = getTotalCpu();
        LOG.trace("Calculating min percentage used by. Used CPU: {} Total CPU: {} Used Mem: {} Total Mem: {}"
            + " Other Used: {} Other Total: {}", totalCpu, used.getTotalCpu(), totalMemory, used.getTotalMemoryMb(),
            this.otherResources, used.otherResources);
        
        Set<String> allResourceNames = new HashSet<>();
        allResourceNames.addAll(this.otherResources.keySet());
        allResourceNames.addAll(used.otherResources.keySet());
        
        double min = 100.0;
        if (totalMemory != 0.0) {
            min = Math.min(min, used.getTotalMemoryMb() / totalMemory);
        }
        if (totalCpu != 0.0) {
            min = Math.min(min, used.getTotalCpu() / totalCpu);
        }

        for (String resourceName : allResourceNames) {
            Double totalValue = this.otherResources.get(resourceName);
            Double usedValue = used.otherResources.get(resourceName);
            if (totalValue == null) {
                throwBecauseResourceIsMissingFromTotal(resourceName);
            } 
            if (totalValue == 0.0) {
                //Skip any resources where the total is 0, we should fall back to prioritizing by cpu and memory in that case
                continue;
            }
            if (usedValue == null) {
                usedValue = 0.0;
            }
            min = Math.min(min, usedValue / totalValue);
        }
        
        return min * 100.0;
    }
}
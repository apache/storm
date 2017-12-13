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

import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
        tmp.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, Constants.COMMON_CPU_RESOURCE_NAME);
        tmp.put(Config.SUPERVISOR_CPU_CAPACITY, Constants.COMMON_CPU_RESOURCE_NAME);
        tmp.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, Constants.COMMON_ONHEAP_MEMORY_RESOURCE_NAME);
        tmp.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, Constants.COMMON_OFFHEAP_MEMORY_RESOURCE_NAME);
        tmp.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, Constants.COMMON_TOTAL_MEMORY_RESOURCE_NAME);
        RESOURCE_NAME_MAPPING = Collections.unmodifiableMap(tmp);
    }

    private static double[] makeArray(Map<String, Double> normalizedResources) {
        //To avoid locking we will go through the map twice.  It should be small so it is probably not a big deal
        for (String key : normalizedResources.keySet()) {
            //We are going to skip over CPU and Memory, because they are captured elsewhere
            if (!Constants.COMMON_CPU_RESOURCE_NAME.equals(key)
                && !Constants.COMMON_TOTAL_MEMORY_RESOURCE_NAME.equals(key)
                && !Constants.COMMON_OFFHEAP_MEMORY_RESOURCE_NAME.equals(key)
                && !Constants.COMMON_ONHEAP_MEMORY_RESOURCE_NAME.equals(key)) {
                resourceNames.computeIfAbsent(key, (k) -> counter.getAndIncrement());
            }
        }
        //By default all of the values are 0
        double [] ret = new double[counter.get()];
        for (Map.Entry<String, Double> entry : normalizedResources.entrySet()) {
            Integer index = resourceNames.get(entry.getKey());
            if (index != null) {
                //index == null if it is memory or CPU
                ret[index] = entry.getValue();
            }
        }
        return ret;
    }

    private static final ConcurrentMap<String, Integer> resourceNames = new ConcurrentHashMap<>();
    private static final AtomicInteger counter = new AtomicInteger(0);
    private double cpu;
    private double[] otherResources;

    /**
     * This is for testing only.  It allows a test to reset the mapping of resource names in the array.
     * We reset the mapping because some algorithms sadly have different behavior if a resource exists
     * or not.
     */
    @VisibleForTesting
    public static void resetResourceNames() {
        resourceNames.clear();
        counter.set(0);
    }

    public NormalizedResources(NormalizedResources other) {
        cpu = other.cpu;
        otherResources = Arrays.copyOf(other.otherResources, other.otherResources.length);
    }

    /**
     * Create a new normalized set of resources.  Note that memory is not
     * covered here because it is not consistent in requests vs offers because
     * of how on heap vs off heap is used.
     * @param resources the resources to be normalized.
     * @param defaults the default resources that will also be normalized and combined with the real resources.
     */
    public NormalizedResources(Map<String, ? extends Number> resources, Map<String, ? extends Number> defaults) {
        Map<String, Double> normalizedResources = normalizedResourceMap(defaults);
        normalizedResources.putAll(normalizedResourceMap(resources));
        cpu = normalizedResources.getOrDefault(Constants.COMMON_CPU_RESOURCE_NAME, 0.0);
        otherResources = makeArray(normalizedResources);
        initializeMemory(normalizedResources);
    }

    /**
     * Initialize any memory usage from the normalized map.
     * @param normalizedResources the normalized resource map.
     */
    protected abstract void initializeMemory(Map<String, Double> normalizedResources);

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

    private void add(double[] resourceArray) {
        int otherLength = resourceArray.length;
        int length = otherResources.length;
        if (otherLength > length) {
            double [] newResources = new double[otherLength];
            System.arraycopy(newResources, 0, otherResources, 0, length);
            otherResources = newResources;
        }
        for (int i = 0; i < otherLength; i++) {
            otherResources[i] += resourceArray[i];
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
        add(makeArray(normalizedResources));
    }

    /**
     * Remove the resources from other.  This is the same as subtracting the resources in other from this.
     * @param other the resources we want removed.
     */
    public void remove(NormalizedResources other) {
        this.cpu -= other.cpu;
        assert cpu >= 0.0;
        int otherLength = other.otherResources.length;
        int length = otherResources.length;
        if (otherLength > length) {
            double [] newResources = new double[otherLength];
            System.arraycopy(newResources, 0, otherResources, 0, length);
            otherResources = newResources;
        }
        for (int i = 0; i < Math.min(length, otherLength); i++) {
            otherResources[i] -= other.otherResources[i];
            assert otherResources[i] >= 0.0;
        }
    }

    @Override
    public String toString() {
        return "CPU: " + cpu;
    }

    /**
     * Return a Map of the normalized resource name to a double.  This should only
     * be used when returning thrift resource requests to the end user.
     */
    public Map<String,Double> toNormalizedMap() {
        HashMap<String, Double> ret = new HashMap<>();
        ret.put(Constants.COMMON_CPU_RESOURCE_NAME, cpu);
        int length = otherResources.length;
        for (Map.Entry<String, Integer> entry: resourceNames.entrySet()) {
            int index = entry.getValue();
            if (index < length) {
                ret.put(entry.getKey(), otherResources[index]);
            }
        }
        return ret;
    }

    private double getResourceAt(int index) {
        if (index >= otherResources.length) {
            return 0.0;
        }
        return otherResources[index];
    }

    /**
     * A simple sanity check to see if all of the resources in this would be large enough to hold the resources in other ignoring memory.
     * It does not check memory because with shared memory it is beyond the scope of this.
     * @param other the resources that we want to check if they would fit in this.
     * @return true if it might fit, else false if it could not possibly fit.
     */
    public boolean couldHoldIgnoringSharedMemory(NormalizedResources other) {
        if (this.cpu < other.getTotalCpu()) {
            return false;
        }
        int length = Math.max(this.otherResources.length, other.otherResources.length);
        for (int i = 0; i < length; i++) {
            if (getResourceAt(i) < other.getResourceAt(i)) {
                return false;
            }
        }

        if (this.getTotalMemoryMb() < other.getTotalMemoryMb()) {
            return false;
        }
        return true;
    }

    /**
     * Calculate the average resource usage percentage with this being the total resources and
     * used being the amounts used.
     * @param used the amount of resources used.
     * @return the average percentage used 0.0 to 100.0.
     */
    public double calculateAveragePercentageUsedBy(NormalizedResources used) {
        double total = 0.0;
        double totalMemory = getTotalMemoryMb();
        if (totalMemory != 0.0) {
            total += used.getTotalMemoryMb() / totalMemory;
        }
        double totalCpu = getTotalCpu();
        if (totalCpu != 0.0) {
            total += used.getTotalCpu() / getTotalCpu();
        }
        //If total is 0 we add in a 0% used, so we can just skip over anything that is not in both.
        int length = Math.min(used.otherResources.length, otherResources.length);
        for (int i = 0; i < length; i++) {
            if (otherResources[i] != 0.0) {
                total += used.otherResources[i] / otherResources[i];
            }
        }
        //To get the count we divide by we need to take the maximum length because we are doing an average.
        return (total * 100.0) / (2 + Math.max(otherResources.length, used.otherResources.length));
    }

    /**
     * Calculate the minimum resource usage percentage with this being the total resources and
     * used being the amounts used.
     * @param used the amount of resources used.
     * @return the minimum percentage used 0.0 to 100.0.
     */
    public double calculateMinPercentageUsedBy(NormalizedResources used) {
        double totalMemory = getTotalMemoryMb();
        double totalCpu = getTotalCpu();
        if (used.otherResources.length != otherResources.length
            || totalMemory == 0.0
            || totalCpu == 0.0) {
            //If the lengths don't match one of the resources will be 0, which means we would calculate the percentage to be 0.0
            // and so the min would be 0.0 (assuming that we can never go negative on a resource being used.
            return 0.0;
        }
        double min = used.getTotalMemoryMb() / totalMemory;
        min = Math.min(min, used.getTotalCpu() / getTotalCpu());

        for (int i = 0; i < otherResources.length; i++) {
            if (otherResources[i] != 0.0) {
                min = Math.min(min, used.otherResources[i] / otherResources[i]);
            } else {
                return 0.0; //0 will be the minimum, because we count values not in here as 0
            }
        }
        return min * 100.0;
    }
}
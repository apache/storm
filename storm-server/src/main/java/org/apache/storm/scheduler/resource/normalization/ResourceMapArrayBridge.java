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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.storm.Constants;

/**
 * Provides translation between normalized resource maps and resource value arrays. Some operations use resource value arrays instead of the
 * full normalized resource map as an optimization. See {@link NormalizedResources}.
 */
public class ResourceMapArrayBridge {

    private final ConcurrentMap<String, Integer> resourceNamesToArrayIndex = new ConcurrentHashMap<>();
    private final AtomicInteger counter = new AtomicInteger(0);

    /**
     * Translates a normalized resource map to an array of resource values. Each resource name will be assigned an index in the array, which
     * is guaranteed to be consistent with subsequent invocations of this method. Note that CPU and memory resources are not translated by
     * this method, as they are expected to be captured elsewhere.
     *
     * @param normalizedResources The resources to translate to an array
     * @return The array of resource values
     */
    public double[] translateToResourceArray(Map<String, Double> normalizedResources) {
        //To avoid locking we will go through the map twice.  It should be small so it is probably not a big deal
        for (String key : normalizedResources.keySet()) {
            //We are going to skip over CPU and Memory, because they are captured elsewhere
            if (!Constants.COMMON_CPU_RESOURCE_NAME.equals(key)
                && !Constants.COMMON_TOTAL_MEMORY_RESOURCE_NAME.equals(key)
                && !Constants.COMMON_OFFHEAP_MEMORY_RESOURCE_NAME.equals(key)
                && !Constants.COMMON_ONHEAP_MEMORY_RESOURCE_NAME.equals(key)) {
                resourceNamesToArrayIndex.computeIfAbsent(key, (k) -> counter.getAndIncrement());
            }
        }
        //By default all of the values are 0
        double[] ret = new double[counter.get()];
        for (Map.Entry<String, Double> entry : normalizedResources.entrySet()) {
            Integer index = resourceNamesToArrayIndex.get(entry.getKey());
            if (index != null) {
                //index == null if it is memory or CPU
                ret[index] = entry.getValue();
            }
        }
        return ret;
    }

    /**
     * Create an array that has all values 0.
     * @return the empty array.
     */
    public double[] empty() {
        return new double[counter.get()];
    }

    /**
     * Translates an array of resource values to a normalized resource map.
     *
     * @param resources The resource array to translate
     * @return The normalized resource map
     */
    public Map<String, Double> translateFromResourceArray(double[] resources) {
        Map<String, Double> ret = new HashMap<>();
        int length = resources.length;
        for (Map.Entry<String, Integer> entry : resourceNamesToArrayIndex.entrySet()) {
            int index = entry.getValue();
            if (index < length) {
                ret.put(entry.getKey(), resources[index]);
            }
        }
        return ret;
    }

    public Map<String, Integer> getResourceNamesToArrayIndex() {
        return Collections.unmodifiableMap(resourceNamesToArrayIndex);
    }

}

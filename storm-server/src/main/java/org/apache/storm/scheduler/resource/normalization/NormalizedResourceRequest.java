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

package org.apache.storm.scheduler.resource.normalization;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.daemon.Acker;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.utils.ObjectReader;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A resource request with normalized resource names.
 */
public class NormalizedResourceRequest implements NormalizedResourcesWithMemory {

    private static final Logger LOG = LoggerFactory.getLogger(NormalizedResourceRequest.class);
    private final NormalizedResources normalizedResources;
    private double onHeap;
    private double offHeap;

    private NormalizedResourceRequest(Map<String, ? extends Number> resources,
        Map<String, Double> defaultResources) {
        if (resources == null && defaultResources == null) {
            onHeap = 0.0;
            offHeap = 0.0;
            normalizedResources = new NormalizedResources();
        } else {
            Map<String, Double> normalizedResourceMap = NormalizedResources.RESOURCE_NAME_NORMALIZER
                .normalizedResourceMap(defaultResources);
            normalizedResourceMap.putAll(NormalizedResources.RESOURCE_NAME_NORMALIZER.normalizedResourceMap(resources));
            onHeap = normalizedResourceMap.getOrDefault(Constants.COMMON_ONHEAP_MEMORY_RESOURCE_NAME, 0.0);
            offHeap = normalizedResourceMap.getOrDefault(Constants.COMMON_OFFHEAP_MEMORY_RESOURCE_NAME, 0.0);
            normalizedResources = new NormalizedResources(normalizedResourceMap);
        }
    }

    public NormalizedResourceRequest(ComponentCommon component, Map<String, Object> topoConf, String componentId) {
        this(parseResources(component.get_json_conf()), getDefaultResources(topoConf, componentId));
    }

    public NormalizedResourceRequest(Map<String, Object> topoConf, String componentId) {
        this((Map<String, ? extends Number>) null, getDefaultResources(topoConf, componentId));
    }

    public NormalizedResourceRequest() {
        this((Map<String, ? extends Number>) null, null);
    }

    private static void putIfMissing(Map<String, Double> dest, String destKey, Map<String, Object> src, String srcKey) {
        if (!dest.containsKey(destKey)) {
            Number value = (Number) src.get(srcKey);
            if (value != null) {
                dest.put(destKey, value.doubleValue());
            }
        }
    }

    private static Map<String, Double> getDefaultResources(Map<String, Object> topoConf, String componentId) {
        Map<String, Double> ret =
            NormalizedResources.RESOURCE_NAME_NORMALIZER.normalizedResourceMap((Map<String, Number>) topoConf.getOrDefault(
                Config.TOPOLOGY_COMPONENT_RESOURCES_MAP, new HashMap<>()));

        // Some components might have different resource configs.
        if (componentId != null) {
            if (componentId.equals(Acker.ACKER_COMPONENT_ID)) {
                if (topoConf.containsKey(Config.TOPOLOGY_ACKER_RESOURCES_ONHEAP_MEMORY_MB)) {
                    ret.put(Constants.COMMON_ONHEAP_MEMORY_RESOURCE_NAME,
                            ObjectReader.getDouble(topoConf.get(Config.TOPOLOGY_ACKER_RESOURCES_ONHEAP_MEMORY_MB)));
                }
                if (topoConf.containsKey(Config.TOPOLOGY_ACKER_RESOURCES_OFFHEAP_MEMORY_MB)) {
                    ret.put(Constants.COMMON_OFFHEAP_MEMORY_RESOURCE_NAME,
                            ObjectReader.getDouble(topoConf.get(Config.TOPOLOGY_ACKER_RESOURCES_OFFHEAP_MEMORY_MB)));
                }
                if (topoConf.containsKey(Config.TOPOLOGY_ACKER_CPU_PCORE_PERCENT)) {
                    ret.put(Constants.COMMON_CPU_RESOURCE_NAME,
                            ObjectReader.getDouble(topoConf.get(Config.TOPOLOGY_ACKER_CPU_PCORE_PERCENT)));
                }
            } else if (componentId.startsWith(Constants.METRICS_COMPONENT_ID_PREFIX)) {
                if (topoConf.containsKey(Config.TOPOLOGY_METRICS_CONSUMER_RESOURCES_ONHEAP_MEMORY_MB)) {
                    ret.put(Constants.COMMON_ONHEAP_MEMORY_RESOURCE_NAME,
                            ObjectReader.getDouble(topoConf.get(Config.TOPOLOGY_METRICS_CONSUMER_RESOURCES_ONHEAP_MEMORY_MB)));
                }
                if (topoConf.containsKey(Config.TOPOLOGY_METRICS_CONSUMER_RESOURCES_OFFHEAP_MEMORY_MB)) {
                    ret.put(Constants.COMMON_OFFHEAP_MEMORY_RESOURCE_NAME,
                            ObjectReader.getDouble(topoConf.get(Config.TOPOLOGY_METRICS_CONSUMER_RESOURCES_OFFHEAP_MEMORY_MB)));
                }
                if (topoConf.containsKey(Config.TOPOLOGY_METRICS_CONSUMER_CPU_PCORE_PERCENT)) {
                    ret.put(Constants.COMMON_CPU_RESOURCE_NAME,
                            ObjectReader.getDouble(topoConf.get(Config.TOPOLOGY_METRICS_CONSUMER_CPU_PCORE_PERCENT)));
                }
            }
        }

        putIfMissing(ret, Constants.COMMON_CPU_RESOURCE_NAME, topoConf, Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT);
        putIfMissing(ret, Constants.COMMON_OFFHEAP_MEMORY_RESOURCE_NAME, topoConf, Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB);
        putIfMissing(ret, Constants.COMMON_ONHEAP_MEMORY_RESOURCE_NAME, topoConf, Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB);
        return ret;
    }

    private static Map<String, Double> parseResources(String input) {
        Map<String, Double> topologyResources = new HashMap<>();
        JSONParser parser = new JSONParser();
        try {
            if (input != null) {
                Object obj = parser.parse(input);
                JSONObject jsonObject = (JSONObject) obj;

                // Legacy resource parsing
                if (jsonObject.containsKey(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB)) {
                    Double topoMemOnHeap = ObjectReader
                        .getDouble(jsonObject.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB), null);
                    topologyResources.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, topoMemOnHeap);
                }
                if (jsonObject.containsKey(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB)) {
                    Double topoMemOffHeap = ObjectReader
                        .getDouble(jsonObject.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB), null);
                    topologyResources.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, topoMemOffHeap);
                }
                if (jsonObject.containsKey(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT)) {
                    Double topoCpu = ObjectReader.getDouble(jsonObject.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT),
                                                            null);
                    topologyResources.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, topoCpu);
                }

                // If resource is also present in resources map will overwrite the above
                if (jsonObject.containsKey(Config.TOPOLOGY_COMPONENT_RESOURCES_MAP)) {
                    Map<String, Number> rawResourcesMap =
                        (Map<String, Number>) jsonObject.computeIfAbsent(
                            Config.TOPOLOGY_COMPONENT_RESOURCES_MAP, (k) -> new HashMap<>());

                    for (Map.Entry<String, Number> stringNumberEntry : rawResourcesMap.entrySet()) {
                        topologyResources.put(
                            stringNumberEntry.getKey(), stringNumberEntry.getValue().doubleValue());
                    }

                }
            }
        } catch (ParseException e) {
            LOG.error("Failed to parse component resources is:" + e.toString(), e);
            return null;
        }
        return topologyResources;
    }

    /**
     * Convert to a map that is used by configuration and the UI.
     * @return a map with the key as the resource name and the value the resource amount.
     */
    public Map<String, Double> toNormalizedMap() {
        Map<String, Double> ret = this.normalizedResources.toNormalizedMap();
        ret.put(Constants.COMMON_OFFHEAP_MEMORY_RESOURCE_NAME, offHeap);
        ret.put(Constants.COMMON_ONHEAP_MEMORY_RESOURCE_NAME, onHeap);
        return ret;
    }

    /*
     * return map with non generic resources removed
     */
    public static void removeNonGenericResources(Map<String, Double> map) {
        map.remove(Constants.COMMON_ONHEAP_MEMORY_RESOURCE_NAME);
        map.remove(Constants.COMMON_OFFHEAP_MEMORY_RESOURCE_NAME);
        map.remove(Constants.COMMON_TOTAL_MEMORY_RESOURCE_NAME);
        map.remove(Constants.COMMON_CPU_RESOURCE_NAME);
    }

    /*
     * return a map that is the sum of resources1 + resources2
     */
    public static Map<String, Double> addResourceMap(Map<String, Double> resources1, Map<String, Double> resources2) {
        Map<String, Double> sum = new HashMap<>(resources1);
        if (resources2 != null) {
            for (Map.Entry<String, Double> me : resources2.entrySet()) {
                Double cur = sum.getOrDefault(me.getKey(), 0.0) + me.getValue();
                sum.put(me.getKey(), cur);
            }
        }
        return sum;
    }

    /*
     * return a map that is the difference of resources1 - resources2
     */
    public static Map<String, Double> subtractResourceMap(Map<String, Double> resource1, Map<String, Double> resource2) {
        if (resource1 == null || resource2 == null) {
            return new HashMap<>();
        }
        Map<String, Double> difference = new HashMap<>(resource1);
        for (Map.Entry<String, Double> me : resource2.entrySet()) {
            Double sub = difference.getOrDefault(me.getKey(), 0.0) - me.getValue();
            difference.put(me.getKey(), sub);
        }
        return difference;
    }

    public double getOnHeapMemoryMb() {
        return onHeap;
    }

    public void addOnHeap(final double onHeap) {
        this.onHeap += onHeap;
    }

    public double getOffHeapMemoryMb() {
        return offHeap;
    }

    public void addOffHeap(final double offHeap) {
        this.offHeap += offHeap;
    }

    /**
     * Add the resources in other to this.
     *
     * @param other the other Request to add to this.
     */
    public void add(NormalizedResourceRequest other) {
        this.normalizedResources.add(other.normalizedResources);
        onHeap += other.onHeap;
        offHeap += other.offHeap;
    }

    /**
     * Add the resources from a worker to those in this.
     * @param value the resources on the worker.
     */
    public void add(WorkerResources value) {
        this.normalizedResources.add(value);
        //The resources are already normalized
        Map<String, Double> resources = value.get_resources();
        onHeap += resources.getOrDefault(Constants.COMMON_ONHEAP_MEMORY_RESOURCE_NAME, 0.0);
        offHeap += resources.getOrDefault(Constants.COMMON_OFFHEAP_MEMORY_RESOURCE_NAME, 0.0);
    }

    @Override
    public double getTotalMemoryMb() {
        return onHeap + offHeap;
    }

    @Override
    public String toString() {
        return "Normalized resources: " + toNormalizedMap();
    }

    public double getTotalCpu() {
        return this.normalizedResources.getTotalCpu();
    }

    @Override
    public NormalizedResources getNormalizedResources() {
        return this.normalizedResources;
    }

    @Override
    public void clear() {
        normalizedResources.clear();
        offHeap = 0.0;
        onHeap = 0.0;
    }

    @Override
    public boolean areAnyOverZero() {
        return onHeap > 0 || offHeap > 0 || normalizedResources.areAnyOverZero();
    }
}

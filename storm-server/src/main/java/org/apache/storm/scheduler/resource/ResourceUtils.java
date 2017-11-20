/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.scheduler.resource;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.storm.Config;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.utils.ObjectReader;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.storm.Constants.resourceNameMapping;

public class ResourceUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ResourceUtils.class);

    public static Map<String, Map<String, Double>> getBoltsResources(StormTopology topology,
                                                                     Map<String, Object> topologyConf) {
        Map<String, Map<String, Double>> boltResources = new HashMap<>();
        if (topology.get_bolts() != null) {
            for (Map.Entry<String, Bolt> bolt : topology.get_bolts().entrySet()) {
                Map<String, Double> topologyResources = parseResources(bolt.getValue().get_common().get_json_conf());
                checkInitialization(topologyResources, bolt.getValue().toString(), topologyConf);
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Turned {} into {}", bolt.getValue().get_common().get_json_conf(), topologyResources);
                }
                boltResources.put(bolt.getKey(), topologyResources);
            }
        }
        return boltResources;
    }

    public static Map<String, Map<String, Double>> getSpoutsResources(StormTopology topology,
                                                                      Map<String, Object> topologyConf) {
        Map<String, Map<String, Double>> spoutResources = new HashMap<>();
        if (topology.get_spouts() != null) {
            for (Map.Entry<String, SpoutSpec> spout : topology.get_spouts().entrySet()) {
                Map<String, Double> topologyResources = parseResources(spout.getValue().get_common().get_json_conf());
                checkInitialization(topologyResources, spout.getValue().toString(), topologyConf);
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Turned {} into {}", spout.getValue().get_common().get_json_conf(), topologyResources);
                }
                spoutResources.put(spout.getKey(), topologyResources);
            }
        }
        return spoutResources;
    }

    public static void updateStormTopologyResources(StormTopology topology, Map<String, Map<String, Double>> resourceUpdatesMap) {
        Map<String, Map<String, Double>> componentsUpdated = new HashMap<>();
        if (topology.get_spouts() != null) {
            for (Map.Entry<String, SpoutSpec> spout : topology.get_spouts().entrySet()) {
                SpoutSpec spoutSpec = spout.getValue();
                String spoutName = spout.getKey();

                if (resourceUpdatesMap.containsKey(spoutName)) {
                    ComponentCommon spoutCommon = spoutSpec.get_common();
                    Map<String, Double> resourcesUpdate = resourceUpdatesMap.get(spoutName);
                    String newJsonConf = getJsonWithUpdatedResources(spoutCommon.get_json_conf(), resourcesUpdate);
                    spoutCommon.set_json_conf(newJsonConf);
                    componentsUpdated.put(spoutName, resourcesUpdate);
                }
            }
        }

        if (topology.get_bolts() != null) {
            for (Map.Entry<String, Bolt> bolt : topology.get_bolts().entrySet()) {
                Bolt boltObj = bolt.getValue();
                String boltName = bolt.getKey();

                if(resourceUpdatesMap.containsKey(boltName)) {
                    ComponentCommon boltCommon = boltObj.get_common();
                    Map<String, Double> resourcesUpdate = resourceUpdatesMap.get(boltName);
                    String newJsonConf = getJsonWithUpdatedResources(boltCommon.get_json_conf(), resourceUpdatesMap.get(boltName));
                    boltCommon.set_json_conf(newJsonConf);
                    componentsUpdated.put(boltName, resourcesUpdate);
                }
            }
        }
        LOG.info("Component resources updated: {}", componentsUpdated);
        Map<String, Map<String, Double>> notUpdated = new HashMap<String, Map<String, Double>>();
        for (String component : resourceUpdatesMap.keySet()) {
            if (!componentsUpdated.containsKey(component)) {
                notUpdated.put(component, resourceUpdatesMap.get(component));
            }
        }
        LOG.info("Component resource updates ignored: {}", notUpdated);
    }

    public static String getJsonWithUpdatedResources(String jsonConf, Map<String, Double> resourceUpdates) {
        try {
            JSONParser parser = new JSONParser();
            Object obj = parser.parse(jsonConf);
            JSONObject jsonObject = (JSONObject) obj;

            if (resourceUpdates.containsKey(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB)) {
                Double topoMemOnHeap = resourceUpdates.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB);
                jsonObject.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, topoMemOnHeap);
            }
            if (resourceUpdates.containsKey(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB)) {
                Double topoMemOffHeap = resourceUpdates.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB);
                jsonObject.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, topoMemOffHeap);
            }
            if (resourceUpdates.containsKey(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT)) {
                Double topoCPU = resourceUpdates.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT);
                jsonObject.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, topoCPU);
            }
            return jsonObject.toJSONString();
        } catch (ParseException ex) {
            throw new RuntimeException("Failed to parse component resources with json: " +  jsonConf);
        }
    }

    public static void checkInitialization(Map<String, Double> topologyResources,
                                           String componentId, Map<String, Object> topologyConf) {
        StringBuilder msgBuilder = new StringBuilder();

        Set<String> resourceNameSet = new HashSet<>();

        resourceNameSet.add(
                Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT
        );
        resourceNameSet.add(
                Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB
        );
        resourceNameSet.add(
                Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB
        );

        Map<String, Double> topologyComponentResourcesMap =
                (Map<String, Double>) topologyConf.getOrDefault(
                        Config.TOPOLOGY_COMPONENT_RESOURCES_MAP, new HashMap<>());

        resourceNameSet.addAll(topologyResources.keySet());
        resourceNameSet.addAll(topologyComponentResourcesMap.keySet());

        for (String resourceName : resourceNameSet) {
            msgBuilder.append(checkInitResource(topologyResources, topologyConf, topologyComponentResourcesMap, resourceName));
        }

        Map<String, Double> normalizedTopologyResources = normalizedResourceMap(topologyResources);
        topologyResources.clear();
        topologyResources.putAll(normalizedTopologyResources);

        if (msgBuilder.length() > 0) {
            String resourceDefaults = msgBuilder.toString();
            LOG.debug(
                    "Unable to extract resource requirement for Component {} \n Resources : {}",
                    componentId, resourceDefaults);
        }
    }

    private static String checkInitResource(Map<String, Double> topologyResources, Map topologyConf,
                                            Map<String, Double> topologyComponentResourcesMap, String resourceName) {
        StringBuilder msgBuilder = new StringBuilder();
        String normalizedResourceName = resourceNameMapping.getOrDefault(resourceName, resourceName);
        if (!topologyResources.containsKey(normalizedResourceName)) {
            if (topologyConf.containsKey(resourceName)) {
                Double resourceValue = ObjectReader.getDouble(topologyConf.get(resourceName));
                if (resourceValue != null) {
                    topologyResources.put(normalizedResourceName, resourceValue);
                }
            }

            if (topologyComponentResourcesMap.containsKey(normalizedResourceName)) {
                Double resourceValue = ObjectReader.getDouble(topologyComponentResourcesMap.get(resourceName));
                if (resourceValue != null) {
                    topologyResources.put(normalizedResourceName, resourceValue);
                }
            }
        }

        return msgBuilder.toString();
    }

    public static Map<String, Double> parseResources(String input) {
        Map<String, Double> topologyResources = new HashMap<>();
        JSONParser parser = new JSONParser();
        LOG.debug("Input to parseResources {}", input);
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
        return normalizedResourceMap(topologyResources);
    }

    /**
     * Calculate the sum of a collection of doubles.
     * @param list collection of doubles
     * @return the sum of of collection of doubles
     */
    public static double sum(Collection<Double> list) {
        double sum = 0.0;
        for (Double elem : list) {
            sum += elem;
        }
        return sum;
    }

    /**
     * Calculate the average of a collection of doubles.
     * @param list a collection of doubles
     * @return the average of collection of doubles
     */
    public static double avg(Collection<Double> list) {
        return sum(list) / list.size();
    }

    /**
     * Normalizes a supervisor resource map or topology details map's keys to universal resource names.
     * @param resourceMap resource map of either Supervisor or Topology
     * @return the resource map with common resource names
     */
    public static Map<String, Double> normalizedResourceMap(Map<String, Double> resourceMap) {
        Map<String, Double> result = new HashMap();

        result.putAll(resourceMap);
        for (Map.Entry entry: resourceMap.entrySet()) {
            if (resourceNameMapping.containsKey(entry.getKey())) {
                result.put(resourceNameMapping.get(entry.getKey()), ObjectReader.getDouble(entry.getValue(), 0.0));
                result.remove(entry.getKey());
            }
        }
        return result;
    }

    public static Map<String, Double> addResources(Map<String, Double> resourceMap1, Map<String, Double> resourceMap2) {
        Map<String, Double> result = new HashMap();

        result.putAll(resourceMap1);

        for (Map.Entry<String, Double> entry: resourceMap2.entrySet()) {
            if (result.containsKey(entry.getKey())) {
                result.put(entry.getKey(), ObjectReader.getDouble(entry.getValue(),
                        0.0) + ObjectReader.getDouble(resourceMap1.get(entry.getKey()), 0.0));
            } else {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;

    }

    public static Double getMinValuePresentInResourceMap(Map<String, Double> resourceMap) {
        return Collections.min(resourceMap.values());
    }

    public static Map<String, Double> getPercentageOfTotalResourceMap(Map<String, Double> resourceMap, Map<String, Double> totalResourceMap) {
        Map<String, Double> result = new HashMap();

        for(Map.Entry<String, Double> entry: totalResourceMap.entrySet()) {
            if (resourceMap.containsKey(entry.getKey())) {
                result.put(entry.getKey(),  (ObjectReader.getDouble(resourceMap.get(entry.getKey()))/ entry.getValue()) * 100.0) ;
            } else {
                result.put(entry.getKey(), 0.0);
            }
        }
        return result;

    }
}

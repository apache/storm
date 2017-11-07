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
import java.util.HashMap;
import java.util.Map;

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

public class ResourceUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ResourceUtils.class);

    public static Map<String, Map<String, Double>> getBoltsResources(StormTopology topology,
                                                                     Map<String, Object> topologyConf) {
        Map<String, Map<String, Double>> boltResources = new HashMap<>();
        if (topology.get_bolts() != null) {
            for (Map.Entry<String, Bolt> bolt : topology.get_bolts().entrySet()) {
                Map<String, Double> topologyResources = parseResources(bolt.getValue().get_common().get_json_conf());
                checkInitialization(topologyResources, bolt.getKey(), topologyConf);
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
                checkInitialization(topologyResources, spout.getKey(), topologyConf);
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

    public static void checkInitialization(Map<String, Double> topologyResources, String com,
                                           Map<String, Object> topologyConf) {
        checkInitMem(topologyResources, com, topologyConf);
        checkInitCpu(topologyResources, com, topologyConf);
    }

    private static void checkInitMem(Map<String, Double> topologyResources, String com,
                                     Map<String, Object> topologyConf) {
        if (!topologyResources.containsKey(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB)) {
            Double onHeap = ObjectReader.getDouble(
                topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB), null);
            if (onHeap != null) {
                topologyResources.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, onHeap);
                LOG.debug(
                    "Unable to extract resource requirement for Component {}\n"
                        + " Resource : Memory Type : On Heap set to default {}",
                    com, onHeap);
            }
        }
        if (!topologyResources.containsKey(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB)) {
            Double offHeap = ObjectReader.getDouble(
                topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB), null);
            if (offHeap != null) {
                topologyResources.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, offHeap);
                LOG.debug(
                    "Unable to extract resource requirement for Component {}\n"
                        + " Resource : Memory Type : Off Heap set to default {}",
                    com, offHeap);
            }
        }
    }

    private static void checkInitCpu(Map<String, Double> topologyResources, String com,
                                     Map<String, Object> topologyConf) {
        if (!topologyResources.containsKey(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT)) {
            Double cpu = ObjectReader.getDouble(topologyConf.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT), null);
            if (cpu != null) {
                topologyResources.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, cpu);
                LOG.debug(
                    "Unable to extract resource requirement for Component {}\n"
                        + " Resource : CPU Pcore Percent set to default {}",
                    com, cpu);
            }
        }
    }

    public static Map<String, Double> parseResources(String input) {
        Map<String, Double> topologyResources = new HashMap<>();
        JSONParser parser = new JSONParser();
        LOG.debug("Input to parseResources {}", input);
        try {
            if (input != null) {
                Object obj = parser.parse(input);
                JSONObject jsonObject = (JSONObject) obj;
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
                LOG.debug("Topology Resources {}", topologyResources);
            }
        } catch (ParseException e) {
            LOG.error("Failed to parse component resources is:" + e.toString(), e);
            return null;
        }
        return topologyResources;
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
}

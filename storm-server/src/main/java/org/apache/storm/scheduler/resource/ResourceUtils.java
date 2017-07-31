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
                checkIntialization(topologyResources, bolt.getValue().toString(), topologyConf);
                LOG.warn("Turned {} into {}", bolt.getValue().get_common().get_json_conf(), topologyResources);
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
                checkIntialization(topologyResources, spout.getValue().toString(), topologyConf);
                spoutResources.put(spout.getKey(), topologyResources);
            }
        }
        return spoutResources;
    }

    public static void checkIntialization(Map<String, Double> topologyResources, String com,
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
                debugMessage("ONHEAP", com, topologyConf);
            }
        }
        if (!topologyResources.containsKey(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB)) {
            Double offHeap = ObjectReader.getDouble(
                topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB), null);
            if (offHeap != null) {
                topologyResources.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, offHeap);
                debugMessage("OFFHEAP", com, topologyConf);
            }
        }
    }

    private static void checkInitCpu(Map<String, Double> topologyResources, String com,
                                     Map<String, Object> topologyConf) {
        if (!topologyResources.containsKey(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT)) {
            Double cpu = ObjectReader.getDouble(topologyConf.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT), null);
            if (cpu != null) {
                topologyResources.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, cpu);
                debugMessage("CPU", com, topologyConf);
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

    private static void debugMessage(String memoryType, String com, Map<String, Object> topologyConf) {
        if (memoryType.equals("ONHEAP")) {
            LOG.debug(
                    "Unable to extract resource requirement for Component {}\n"
                        + " Resource : Memory Type : On Heap set to default {}",
                    com, topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB));
        } else if (memoryType.equals("OFFHEAP")) {
            LOG.debug(
                    "Unable to extract resource requirement for Component {}\n"
                        + " Resource : Memory Type : Off Heap set to default {}",
                    com, topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB));
        } else {
            LOG.debug(
                    "Unable to extract resource requirement for Component {}\n"
                        + " Resource : CPU Pcore Percent set to default {}",
                    com, topologyConf.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT));
        }
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

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.scheduler.resource;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourceRequest;
import org.apache.storm.scheduler.resource.normalization.NormalizedResources;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ResourceUtils.class);

    public static NormalizedResourceRequest getBoltResources(StormTopology topology, Map<String, Object> topologyConf,
                                                             String componentId) {
        if (topology.get_bolts() != null) {
            Bolt bolt = topology.get_bolts().get(componentId);
            return new NormalizedResourceRequest(bolt.get_common(), topologyConf, componentId);
        }
        return null;
    }

    public static Map<String, NormalizedResourceRequest> getBoltsResources(StormTopology topology, Map<String, Object> topologyConf) {
        Map<String, NormalizedResourceRequest> boltResources = new HashMap<>();
        if (topology.get_bolts() != null) {
            for (Map.Entry<String, Bolt> bolt : topology.get_bolts().entrySet()) {
                NormalizedResourceRequest topologyResources = new NormalizedResourceRequest(bolt.getValue().get_common(),
                        topologyConf, bolt.getKey());
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Turned {} into {}", bolt.getValue().get_common().get_json_conf(), topologyResources);
                }
                boltResources.put(bolt.getKey(), topologyResources);
            }
        }
        return boltResources;
    }

    public static NormalizedResourceRequest getSpoutResources(StormTopology topology,
        Map<String, Object> topologyConf, String componentId) {
        if (topology.get_spouts() != null) {
            SpoutSpec spout = topology.get_spouts().get(componentId);
            return new NormalizedResourceRequest(spout.get_common(), topologyConf, componentId);
        }
        return null;
    }

    public static Map<String, NormalizedResourceRequest> getSpoutsResources(StormTopology topology,
        Map<String, Object> topologyConf) {
        Map<String, NormalizedResourceRequest> spoutResources = new HashMap<>();
        if (topology.get_spouts() != null) {
            for (Map.Entry<String, SpoutSpec> spout : topology.get_spouts().entrySet()) {
                NormalizedResourceRequest topologyResources = new NormalizedResourceRequest(spout.getValue().get_common(),
                        topologyConf, spout.getKey());
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
                    Map<String, Double> resourcesUpdate = NormalizedResources
                        .RESOURCE_NAME_NORMALIZER.normalizedResourceMap(resourceUpdatesMap.get(spoutName));
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

                if (resourceUpdatesMap.containsKey(boltName)) {
                    ComponentCommon boltCommon = boltObj.get_common();
                    Map<String, Double> resourcesUpdate = NormalizedResources
                        .RESOURCE_NAME_NORMALIZER.normalizedResourceMap(resourceUpdatesMap.get(boltName));
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

    public static String getCorrespondingLegacyResourceName(String normalizedResourceName) {
        for (Map.Entry<String, String> entry : NormalizedResources.RESOURCE_NAME_NORMALIZER.getResourceNameMapping().entrySet()) {
            if (entry.getValue().equals(normalizedResourceName)) {
                return entry.getKey();
            }
        }
        return normalizedResourceName;
    }

    public static String getJsonWithUpdatedResources(String jsonConf, Map<String, Double> resourceUpdates) {
        try {
            JSONParser parser = new JSONParser();
            Object obj = parser.parse(jsonConf);
            JSONObject jsonObject = (JSONObject) obj;

            Map<String, Double> componentResourceMap =
                (Map<String, Double>) jsonObject.getOrDefault(
                    Config.TOPOLOGY_COMPONENT_RESOURCES_MAP, new HashMap<String, Double>()
                );

            for (Map.Entry<String, Double> resourceUpdateEntry : resourceUpdates.entrySet()) {
                if (NormalizedResources.RESOURCE_NAME_NORMALIZER.getResourceNameMapping().containsValue(resourceUpdateEntry.getKey())) {
                    // if there will be legacy values they will be in the outer conf
                    jsonObject.remove(getCorrespondingLegacyResourceName(resourceUpdateEntry.getKey()));
                    componentResourceMap.remove(getCorrespondingLegacyResourceName(resourceUpdateEntry.getKey()));
                }
                componentResourceMap.put(resourceUpdateEntry.getKey(), resourceUpdateEntry.getValue());
            }
            jsonObject.put(Config.TOPOLOGY_COMPONENT_RESOURCES_MAP, componentResourceMap);

            return jsonObject.toJSONString();
        } catch (ParseException ex) {
            throw new RuntimeException("Failed to parse component resources with json: " + jsonConf);
        }
    }

}

/**
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

package org.apache.storm.utils;

import org.apache.storm.Config;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.ComponentObject;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopologySpoutLag {
    // FIXME: This class can be moved to webapp once UI porting is done.

    private static final String SPOUT_ID = "spoutId";
    private static final String SPOUT_TYPE= "spoutType";
    private static final String SPOUT_LAG_RESULT = "spoutLagResult";
    private static final String ERROR_INFO = "errorInfo";
    private final static Logger logger = LoggerFactory.getLogger(TopologySpoutLag.class);

    public static Map<String, Map<String, Object>> lag (StormTopology stormTopology, Map<String, Object> topologyConf) {
        Map<String, Map<String, Object>> result = new HashMap<>();
        Map<String, SpoutSpec> spouts = stormTopology.get_spouts();
        String className = null;
        for (Map.Entry<String, SpoutSpec> spout: spouts.entrySet()) {
            try {
                SpoutSpec spoutSpec = spout.getValue();
                ComponentObject componentObject = spoutSpec.get_spout_object();
                // FIXME: yes it's a trick so we might be better to find alternative way...
                className = getClassNameFromComponentObject(componentObject);
                logger.debug("spout classname: {}", className);
                if (className.endsWith("storm.kafka.spout.KafkaSpout")) {
                    result.put(spout.getKey(), getLagResultForNewKafkaSpout(spout.getKey(), spoutSpec, topologyConf));
                } else if (className.endsWith("storm.kafka.KafkaSpout")) {
                    result.put(spout.getKey(), getLagResultForOldKafkaSpout(spout.getKey(), spoutSpec, topologyConf));
                }
            } catch (Exception e) {
                logger.warn("Exception thrown while getting lag for spout id: " + spout.getKey() + " and spout class: " + className);
                logger.warn("Exception message:" + e.getMessage(), e);
            }
        }
        return result;
    }

    private static String getClassNameFromComponentObject(ComponentObject componentObject) {
        try {
            Object object = Utils.getSetComponentObject(componentObject);
            return object.getClass().getCanonicalName();
        } catch (RuntimeException e) {

            if (e.getCause() instanceof ClassNotFoundException) {
                return e.getCause().getMessage().trim();
            }

            throw e;
        }
    }

    private static List<String> getCommandLineOptionsForNewKafkaSpout (Map<String, Object> jsonConf) {
        logger.debug("json configuration: {}", jsonConf);

        List<String> commands = new ArrayList<>();
        String configKeyPrefix = "config.";
        commands.add("-t");
        commands.add((String)jsonConf.get(configKeyPrefix + "topics"));
        commands.add("-g");
        commands.add((String)jsonConf.get(configKeyPrefix + "groupid"));
        commands.add("-b");
        commands.add((String)jsonConf.get(configKeyPrefix + "bootstrap.servers"));
        String securityProtocol = (String)jsonConf.get(configKeyPrefix + "security.protocol");
        if (securityProtocol != null && !securityProtocol.isEmpty()) {
            commands.add("-s");
            commands.add(securityProtocol);
        }
        return commands;
    }

    private static List<String> getCommandLineOptionsForOldKafkaSpout (Map<String, Object> jsonConf, Map<String, Object> topologyConf) {
        logger.debug("json configuration: {}", jsonConf);

        List<String> commands = new ArrayList<>();
        String configKeyPrefix = "config.";
        commands.add("-o");
        commands.add("-t");
        commands.add((String)jsonConf.get(configKeyPrefix + "topics"));
        commands.add("-n");
        commands.add((String)jsonConf.get(configKeyPrefix + "zkRoot"));
        String zkServers = (String)jsonConf.get(configKeyPrefix + "zkServers");
        if (zkServers == null || zkServers.isEmpty()) {
            StringBuilder zkServersBuilder = new StringBuilder();
            Integer zkPort = ((Number) topologyConf.get(Config.STORM_ZOOKEEPER_PORT)).intValue();
            for (String zkServer: (List<String>) topologyConf.get(Config.STORM_ZOOKEEPER_SERVERS)) {
                zkServersBuilder.append(zkServer + ":" + zkPort + ",");
            }
            zkServers = zkServersBuilder.toString();
        }
        commands.add("-z");
        commands.add(zkServers);
        if (jsonConf.get(configKeyPrefix + "leaders") != null) {
            commands.add("-p");
            commands.add((String)jsonConf.get(configKeyPrefix + "partitions"));
            commands.add("-l");
            commands.add((String)jsonConf.get(configKeyPrefix + "leaders"));
        } else {
            commands.add("-r");
            commands.add((String)jsonConf.get(configKeyPrefix + "zkNodeBrokers"));
            Boolean isWildCard = (Boolean) topologyConf.get("kafka.topic.wildcard.match");
            if (isWildCard != null && isWildCard.booleanValue()) {
                commands.add("-w");
            }
        }
        return commands;
    }

    private static Map<String, Object> getLagResultForKafka (String spoutId, SpoutSpec spoutSpec, Map<String, Object> topologyConf, boolean old) throws IOException {
        ComponentCommon componentCommon = spoutSpec.get_common();
        String json = componentCommon.get_json_conf();
        Map<String, Object> result = null;
        String errorMsg = "Offset lags for kafka not supported for older versions. Please update kafka spout to latest version.";
        if (json != null && !json.isEmpty()) {
            List<String> commands = new ArrayList<>();
            String stormHomeDir = System.getenv("STORM_BASE_DIR");
            if (stormHomeDir != null && !stormHomeDir.endsWith("/")) {
                stormHomeDir += File.separator;
            }
            commands.add(stormHomeDir != null ? stormHomeDir + "bin" + File.separator + "storm-kafka-monitor" : "storm-kafka-monitor");
            Map<String, Object> jsonMap = null;
            try {
                jsonMap = (Map<String, Object>) JSONValue.parseWithException(json);
            } catch (ParseException e) {
                throw new IOException(e);
            }
            commands.addAll(old ? getCommandLineOptionsForOldKafkaSpout(jsonMap, topologyConf) : getCommandLineOptionsForNewKafkaSpout(jsonMap));

            logger.debug("Command to run: {}", commands);

            // if commands contains one or more null value, spout is compiled with lower version of storm-kafka / storm-kafka-client
            if (!commands.contains(null)) {
                String resultFromMonitor = ShellUtils.execCommand(commands.toArray(new String[0]));

                try {
                    result = (Map<String, Object>) JSONValue.parseWithException(resultFromMonitor);
                } catch (ParseException e) {
                    logger.debug("JSON parsing failed, assuming message as error message: {}", resultFromMonitor);
                    // json parsing fail -> error received
                    errorMsg = resultFromMonitor;
                }
            }
        }

        Map<String, Object> kafkaSpoutLagInfo = new HashMap<>();
        kafkaSpoutLagInfo.put(SPOUT_ID, spoutId);
        kafkaSpoutLagInfo.put(SPOUT_TYPE, "KAFKA");

        if (result != null) {
            kafkaSpoutLagInfo.put(SPOUT_LAG_RESULT, result);
        } else {
            kafkaSpoutLagInfo.put(ERROR_INFO, errorMsg);
        }

        return kafkaSpoutLagInfo;
    }

    private static Map<String, Object> getLagResultForNewKafkaSpout (String spoutId, SpoutSpec spoutSpec, Map<String, Object> topologyConf) throws IOException {
        return getLagResultForKafka(spoutId, spoutSpec, topologyConf, false);
    }

    private static Map<String, Object> getLagResultForOldKafkaSpout (String spoutId, SpoutSpec spoutSpec, Map<String, Object> topologyConf) throws IOException {
        return getLagResultForKafka(spoutId, spoutSpec, topologyConf, true);
    }
}

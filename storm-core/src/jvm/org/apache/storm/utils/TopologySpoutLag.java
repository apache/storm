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

package org.apache.storm.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopologySpoutLag {
    private static final String SPOUT_ID = "spoutId";
    private static final String SPOUT_TYPE = "spoutType";
    private static final String SPOUT_LAG_RESULT = "spoutLagResult";
    private static final String ERROR_INFO = "errorInfo";
    private static final String CONFIG_KEY_PREFIX = "config.";
    private static final String TOPICS_CONFIG = CONFIG_KEY_PREFIX + "topics";
    private static final String GROUPID_CONFIG = CONFIG_KEY_PREFIX + "groupid";
    private static final String BOOTSTRAP_CONFIG = CONFIG_KEY_PREFIX + "bootstrap.servers";
    private static final String LEADERS_CONFIG = CONFIG_KEY_PREFIX + "leaders";
    private static final String ZKROOT_CONFIG = CONFIG_KEY_PREFIX + "zkRoot";
    private final static Logger logger = LoggerFactory.getLogger(TopologySpoutLag.class);

    public static Map<String, Map<String, Object>> lag(StormTopology stormTopology, Map topologyConf) {
        Map<String, Map<String, Object>> result = new HashMap<>();
        Map<String, SpoutSpec> spouts = stormTopology.get_spouts();
        for (Map.Entry<String, SpoutSpec> spout: spouts.entrySet()) {
            try {
                SpoutSpec spoutSpec = spout.getValue();
                addLagResultForKafkaSpout(result, spout.getKey(), spoutSpec, topologyConf);
            } catch (Exception e) {
                logger.warn("Exception thrown while getting lag for spout id: " + spout.getKey());
                logger.warn("Exception message:" + e.getMessage(), e);
            }
        }
        return result;
    }

    private static List<String> getCommandLineOptionsForNewKafkaSpout (Map<String, Object> jsonConf) {
        logger.debug("json configuration: {}", jsonConf);

        List<String> commands = new ArrayList<>();
        commands.add("-t");
        commands.add((String) jsonConf.get(TOPICS_CONFIG));
        commands.add("-g");
        commands.add((String) jsonConf.get(GROUPID_CONFIG));
        commands.add("-b");
        commands.add((String) jsonConf.get(BOOTSTRAP_CONFIG));
        String securityProtocol = (String) jsonConf.get(CONFIG_KEY_PREFIX + "security.protocol");
        if (securityProtocol != null && !securityProtocol.isEmpty()) {
            commands.add("-s");
            commands.add(securityProtocol);
        }
        return commands;
    }

    private static List<String> getCommandLineOptionsForOldKafkaSpout (Map<String, Object> jsonConf, Map topologyConf) {
        logger.debug("json configuration: {}", jsonConf);

        List<String> commands = new ArrayList<>();
        commands.add("-o");
        commands.add("-t");
        commands.add((String)jsonConf.get(TOPICS_CONFIG));
        commands.add("-n");
        commands.add((String)jsonConf.get(ZKROOT_CONFIG));
        String zkServers = (String)jsonConf.get(CONFIG_KEY_PREFIX + "zkServers");
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
        if (jsonConf.get(LEADERS_CONFIG) != null) {
            commands.add("-p");
            commands.add((String)jsonConf.get(CONFIG_KEY_PREFIX + "partitions"));
            commands.add("-l");
            commands.add((String)jsonConf.get(LEADERS_CONFIG));
        } else {
            commands.add("-r");
            commands.add((String)jsonConf.get(CONFIG_KEY_PREFIX + "zkNodeBrokers"));
            Boolean isWildCard = (Boolean) topologyConf.get("kafka.topic.wildcard.match");
            if (isWildCard != null && isWildCard.booleanValue()) {
                commands.add("-w");
            }
        }
        return commands;
    }

    private static void addLagResultForKafkaSpout(Map<String, Map<String, Object>> finalResult, String spoutId, SpoutSpec spoutSpec,
                                                  Map topologyConf) throws IOException {
        ComponentCommon componentCommon = spoutSpec.get_common();
        String json = componentCommon.get_json_conf();
        if (json != null && !json.isEmpty()) {
            Map<String, Object> jsonMap = null;
            try {
                jsonMap = (Map<String, Object>) JSONValue.parseWithException(json);
            } catch (ParseException e) {
                throw new IOException(e);
            }

            if (jsonMap.containsKey(TOPICS_CONFIG)
                && jsonMap.containsKey(GROUPID_CONFIG)
                && jsonMap.containsKey(BOOTSTRAP_CONFIG)) {
                finalResult.put(spoutId, getLagResultForNewKafkaSpout(spoutId, spoutSpec, topologyConf));
            } else if (jsonMap.containsKey(TOPICS_CONFIG)
                && jsonMap.containsKey(ZKROOT_CONFIG)) {
                //Probably the old spout
                finalResult.put(spoutId, getLagResultForOldKafkaSpout(spoutId, spoutSpec, topologyConf));
            }
        }
    }

    private static Map<String, Object> getLagResultForKafka (String spoutId, SpoutSpec spoutSpec, Map topologyConf, boolean old) throws IOException {
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

    private static Map<String, Object> getLagResultForNewKafkaSpout (String spoutId, SpoutSpec spoutSpec, Map topologyConf) throws IOException {
        return getLagResultForKafka(spoutId, spoutSpec, topologyConf, false);
    }

    private static Map<String, Object> getLagResultForOldKafkaSpout (String spoutId, SpoutSpec spoutSpec, Map topologyConf) throws IOException {
        return getLagResultForKafka(spoutId, spoutSpec, topologyConf, true);
    }
}

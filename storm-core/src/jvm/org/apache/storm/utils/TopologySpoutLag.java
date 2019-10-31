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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.utils;

import com.google.common.base.Strings;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopologySpoutLag {
    // FIXME: This class can be moved to webapp once UI porting is done.

    private static final String SPOUT_ID = "spoutId";
    private static final String SPOUT_TYPE = "spoutType";
    private static final String SPOUT_LAG_RESULT = "spoutLagResult";
    private static final String ERROR_INFO = "errorInfo";
    private static final String CONFIG_KEY_PREFIX = "config.";
    private static final String TOPICS_CONFIG = CONFIG_KEY_PREFIX + "topics";
    private static final String GROUPID_CONFIG = CONFIG_KEY_PREFIX + "groupid";
    private static final String BOOTSTRAP_CONFIG = CONFIG_KEY_PREFIX + "bootstrap.servers";
    private static final String SECURITY_PROTOCOL_CONFIG = CONFIG_KEY_PREFIX + "security.protocol";
    private static final Set<String> ALL_CONFIGS = new HashSet<>(Arrays.asList(TOPICS_CONFIG, GROUPID_CONFIG,
            BOOTSTRAP_CONFIG, SECURITY_PROTOCOL_CONFIG));
    private static final Logger LOGGER = LoggerFactory.getLogger(TopologySpoutLag.class);

    public static Map<String, Map<String, Object>> lag(StormTopology stormTopology, Map<String, Object> topologyConf) {
        Map<String, Map<String, Object>> result = new HashMap<>();
        Map<String, SpoutSpec> spouts = stormTopology.get_spouts();
        for (Map.Entry<String, SpoutSpec> spout: spouts.entrySet()) {
            try {
                SpoutSpec spoutSpec = spout.getValue();
                addLagResultForKafkaSpout(result, spout.getKey(), spoutSpec);
            } catch (Exception e) {
                LOGGER.warn("Exception thrown while getting lag for spout id: " + spout.getKey());
                LOGGER.warn("Exception message:" + e.getMessage(), e);
            }
        }
        return result;
    }

    private static List<String> getCommandLineOptionsForNewKafkaSpout(Map<String, Object> jsonConf) {
        LOGGER.debug("json configuration: {}", jsonConf);

        List<String> commands = new ArrayList<>();
        commands.add("-t");
        commands.add((String) jsonConf.get(TOPICS_CONFIG));
        commands.add("-g");
        commands.add((String) jsonConf.get(GROUPID_CONFIG));
        commands.add("-b");
        commands.add((String) jsonConf.get(BOOTSTRAP_CONFIG));
        String securityProtocol = (String) jsonConf.get(SECURITY_PROTOCOL_CONFIG);
        if (!Strings.isNullOrEmpty(securityProtocol)) {
            commands.add("-s");
            commands.add(securityProtocol);
        }
        return commands;
    }

    private static File createExtraPropertiesFile(Map<String, Object> jsonConf) {
        File file = null;
        Map<String, String> extraProperties = new HashMap<>();
        for (Map.Entry<String, Object> conf: jsonConf.entrySet()) {
            if (conf.getKey().startsWith(CONFIG_KEY_PREFIX) && !ALL_CONFIGS.contains(conf.getKey())) {
                extraProperties.put(conf.getKey().substring(CONFIG_KEY_PREFIX.length()), conf.getValue().toString());
            }
        }
        if (!extraProperties.isEmpty()) {
            try {
                file = File.createTempFile("kafka-consumer-extra", "props");
                file.deleteOnExit();
                Properties properties = new Properties();
                properties.putAll(extraProperties);
                try (FileOutputStream fos = new FileOutputStream(file)) {
                    properties.store(fos, "Kafka consumer extra properties");
                }
            } catch (IOException ex) {
                // ignore
            }
        }
        return file;
    }

    private static void addLagResultForKafkaSpout(Map<String, Map<String, Object>> finalResult, String spoutId, SpoutSpec spoutSpec)
        throws IOException {
        ComponentCommon componentCommon = spoutSpec.get_common();
        String json = componentCommon.get_json_conf();
        if (!Strings.isNullOrEmpty(json)) {
            Map<String, Object> jsonMap = null;
            try {
                jsonMap = (Map<String, Object>) JSONValue.parseWithException(json);
            } catch (ParseException e) {
                throw new IOException(e);
            }

            if (jsonMap.containsKey(TOPICS_CONFIG)
                && jsonMap.containsKey(GROUPID_CONFIG)
                && jsonMap.containsKey(BOOTSTRAP_CONFIG)) {
                finalResult.put(spoutId, getLagResultForNewKafkaSpout(spoutId, spoutSpec));
            }
        }
    }

    private static Map<String, Object> getLagResultForKafka(String spoutId, SpoutSpec spoutSpec) throws IOException {
        ComponentCommon componentCommon = spoutSpec.get_common();
        String json = componentCommon.get_json_conf();
        Map<String, Object> result = null;
        String errorMsg = new StringBuilder("Make sure Kafka spout version is latest and ")
            .append(TOPICS_CONFIG)
            .append(", ")
            .append(GROUPID_CONFIG)
            .append(" & ")
            .append(BOOTSTRAP_CONFIG)
            .append(" are not null for newer versions of Kafka spout.")
            .toString();
        if (!Strings.isNullOrEmpty(json)) {
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
            commands.addAll(getCommandLineOptionsForNewKafkaSpout(jsonMap));

            File extraPropertiesFile = createExtraPropertiesFile(jsonMap);
            if (extraPropertiesFile != null) {
                commands.add("-c");
                commands.add(extraPropertiesFile.getAbsolutePath());
            }
            LOGGER.debug("Command to run: {}", commands);

            // if commands contains one or more null value, spout is compiled with lower version of storm-kafka-client
            if (!commands.contains(null)) {
                try {
                    String resultFromMonitor = new ShellCommandRunnerImpl().execCommand(commands.toArray(new String[0]));

                    try {
                        result = (Map<String, Object>) JSONValue.parseWithException(resultFromMonitor);
                    } catch (ParseException e) {
                        LOGGER.debug("JSON parsing failed, assuming message as error message: {}", resultFromMonitor);
                        // json parsing fail -> error received
                        errorMsg = resultFromMonitor;
                    }
                } finally {
                    if (extraPropertiesFile != null) {
                        extraPropertiesFile.delete();
                    }
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

    private static Map<String, Object> getLagResultForNewKafkaSpout(String spoutId, SpoutSpec spoutSpec) throws IOException {
        return getLagResultForKafka(spoutId, spoutSpec);
    }
}

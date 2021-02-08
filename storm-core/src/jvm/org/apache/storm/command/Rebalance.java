/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.command;

import static java.lang.String.format;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.RebalanceOptions;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Rebalance {

    private static final Logger LOG = LoggerFactory.getLogger(Rebalance.class);

    public static void main(String[] args) throws Exception {
        Map<String, Object> cl = CLI.opt("w", "wait", null, CLI.AS_INT)
                                    .opt("n", "num-workers", null, CLI.AS_INT)
                                    .opt("e", "executor", null, new ExecutorParser(), CLI.INTO_MAP)
                                    .opt("r", "resources", null, new ResourcesParser(), CLI.INTO_MAP)
                                    .opt("t", "topology-conf", null, new ConfParser(), CLI.INTO_MAP)
                                    .arg("topologyName", CLI.FIRST_WINS)
                                    .parse(args);
        final String name = (String) cl.get("topologyName");
        Utils.validateTopologyName(name);
        final RebalanceOptions rebalanceOptions = new RebalanceOptions();
        Integer wait = (Integer) cl.get("w");
        if (null != wait) {
            rebalanceOptions.set_wait_secs(wait);
        }
        Integer numWorkers = (Integer) cl.get("n");
        if (null != numWorkers) {
            rebalanceOptions.set_num_workers(numWorkers);
        }
        Map<String, Integer> numExecutors = (Map<String, Integer>) cl.get("e");
        if (null != numExecutors) {
            rebalanceOptions.set_num_executors(numExecutors);
        }
        Map<String, Map<String, Double>> resourceOverrides = (Map<String, Map<String, Double>>) cl.get("r");
        if (null != resourceOverrides) {
            rebalanceOptions.set_topology_resources_overrides(resourceOverrides);
        }

        Map<String, Object> confOverrides = (Map<String, Object>) cl.get("t");
        Map<String, Object> jvmOpts = Utils.readCommandLineOpts(); // values in -Dstorm.options (originally -c in storm.py)
        if (jvmOpts != null && !jvmOpts.isEmpty()) {
            if (confOverrides == null) {
                confOverrides = jvmOpts;
            } else {
                confOverrides.putAll(jvmOpts); // override with values obtained from -Dstorm.options
            }
            LOG.info("Rebalancing topology with overrides {}", JSONObject.toJSONString(confOverrides));
        }

        if (null != confOverrides) {
            rebalanceOptions.set_topology_conf_overrides(JSONValue.toJSONString(confOverrides));
        }

        NimbusClient.withConfiguredClient(new NimbusClient.WithNimbus() {
            @Override
            public void run(Nimbus.Iface nimbus) throws Exception {
                nimbus.rebalance(name, rebalanceOptions);
                LOG.info("Topology {} is rebalancing", name);
            }
        });
    }

    static final class ConfParser implements CLI.Parse {
        @Override
        public Object parse(String value) {
            if (value == null) {
                throw new RuntimeException("No arguments found for topology config override!");
            }
            try {
                return Utils.parseJson(value);
            } catch (Exception e) {
                throw new RuntimeException("Error trying to parse topology config override", e);
            }
        }
    }

    static final class ResourcesParser implements CLI.Parse {
        @Override
        public Object parse(String value) {
            if (value == null) {
                throw new RuntimeException("No arguments found for topology resources override!");
            }
            try {
                //This is a bit ugly The JSON we are expecting should be in the form
                // {"component": {"resource": value, ...}, ...}
                // But because value is coming from JSON it is going to be a Number, and we want it to be a Double.
                // So the goal is to go through each entry and update it accordingly
                Map<String, Map<String, Double>> ret = new HashMap<>();
                for (Map.Entry<String, Object> compEntry : Utils.parseJson(value).entrySet()) {
                    String comp = compEntry.getKey();
                    Map<String, Number> numResources = (Map<String, Number>) compEntry.getValue();
                    Map<String, Double> doubleResource = new HashMap<>();
                    for (Map.Entry<String, Number> entry : numResources.entrySet()) {
                        doubleResource.put(entry.getKey(), entry.getValue().doubleValue());
                    }
                    ret.put(comp, doubleResource);
                }
                return ret;
            } catch (Exception e) {
                throw new RuntimeException("Error trying to parse resource override", e);
            }
        }
    }

    static final class ExecutorParser implements CLI.Parse {
        @Override
        public Object parse(String value) {
            try {
                int splitIndex = value.lastIndexOf('=');
                String componentName = value.substring(0, splitIndex);
                Integer parallelism = Integer.parseInt(value.substring(splitIndex + 1));
                Map<String, Integer> result = new HashMap<String, Integer>();
                result.put(componentName, parallelism);
                return result;
            } catch (Throwable ex) {
                throw new IllegalArgumentException(
                    format("Failed to parse '%s' correctly. Expected in <component>=<parallelism> format", value), ex);
            }
        }
    }

}

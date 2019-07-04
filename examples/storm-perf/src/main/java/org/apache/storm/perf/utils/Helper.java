/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package org.apache.storm.perf.utils;

import java.util.Map;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Utils;


public class Helper {

    public static void kill(Nimbus.Iface client, String topoName) throws Exception {
        KillOptions opts = new KillOptions();
        opts.set_wait_secs(0);
        client.killTopologyWithOpts(topoName, opts);
    }

    public static int getInt(Map map, Object key, int def) {
        return ObjectReader.getInt(Utils.get(map, key, def));
    }

    public static String getStr(Map map, Object key) {
        return (String) map.get(key);
    }

    public static void collectMetricsAndKill(String topologyName, Integer pollInterval, int duration) throws Exception {
        Map<String, Object> clusterConf = Utils.readStormConfig();
        Nimbus.Iface client = NimbusClient.getConfiguredClient(clusterConf).getClient();
        try (BasicMetricsCollector metricsCollector = new BasicMetricsCollector(topologyName, clusterConf)) {

            if (duration > 0) {
                int times = duration / pollInterval;
                metricsCollector.collect(client);
                for (int i = 0; i < times; i++) {
                    Thread.sleep(pollInterval * 1000);
                    metricsCollector.collect(client);
                }
            } else {
                while (true) { //until Ctrl-C
                    metricsCollector.collect(client);
                    Thread.sleep(pollInterval * 1000);
                }
            }
        } finally {
            kill(client, topologyName);
        }
    }

    /**
     * Kill topo on Ctrl-C.
     */
    public static void setupShutdownHook(final String topoName) {
        Map<String, Object> clusterConf = Utils.readStormConfig();
        final Nimbus.Iface client = NimbusClient.getConfiguredClient(clusterConf).getClient();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    System.out.println("Killing...");
                    Helper.kill(client, topoName);
                    System.out.println("Killed Topology");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public static void runOnClusterAndPrintMetrics(int durationSec, String topoName, Map<String, Object> topoConf, StormTopology topology)
        throws Exception {
        // submit topology
        StormSubmitter.submitTopologyWithProgressBar(topoName, topoConf, topology);
        setupShutdownHook(topoName); // handle Ctrl-C

        // poll metrics every minute, then kill topology after specified duration
        Integer pollIntervalSec = 60;
        collectMetricsAndKill(topoName, pollIntervalSec, durationSec);
    }
}

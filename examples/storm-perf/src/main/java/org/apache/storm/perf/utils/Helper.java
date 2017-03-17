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

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.SpoutStats;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.perf.KafkaHdfsTopo;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import java.util.Map;


public class Helper {

  public static void kill(Nimbus.Client client, String topoName) throws Exception {
    KillOptions opts = new KillOptions();
    opts.set_wait_secs(0);
    client.killTopologyWithOpts(topoName, opts);
  }

  public static void killAndShutdownCluster(LocalCluster cluster, String topoName) throws Exception {
    KillOptions opts = new KillOptions();
    opts.set_wait_secs(0);
    cluster.killTopologyWithOpts(topoName, opts);
    cluster.shutdown();
  }


    public static LocalCluster runOnLocalCluster(String topoName, StormTopology topology) {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topoName, new Config(), topology);
        return cluster;
    }

    public static int getInt(Map map, Object key, int def) {
        return Utils.getInt(Utils.get(map, key, def));
    }

    public static String getStr(Map map, Object key) {
        return (String) map.get(key);
    }

    public static void collectMetricsAndKill(String topologyName, Integer pollInterval, Integer duration) throws Exception {
        Map clusterConf = Utils.readStormConfig();
        Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();
        BasicMetricsCollector metricsCollector = new BasicMetricsCollector(client, topologyName, clusterConf);

        int times = duration / pollInterval;
        metricsCollector.collect(client);
        for (int i = 0; i < times; i++) {
            Thread.sleep(pollInterval * 1000);
            metricsCollector.collect(client);
        }
        metricsCollector.close();
        kill(client, topologyName);
    }

    public static void collectLocalMetricsAndKill(LocalCluster localCluster, String topologyName, Integer pollInterval, Integer duration, Map clusterConf) throws Exception {
        BasicMetricsCollector metricsCollector = new BasicMetricsCollector(localCluster, topologyName, clusterConf);

        int times = duration / pollInterval;
        metricsCollector.collect(localCluster);
        for (int i = 0; i < times; i++) {
            Thread.sleep(pollInterval * 1000);
            metricsCollector.collect(localCluster);
        }
        metricsCollector.close();
        killAndShutdownCluster(localCluster, topologyName);
    }

    /** Kill topo and Shutdown local cluster on Ctrl-C */
  public static void setupShutdownHook(final LocalCluster cluster, final String topoName) {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        cluster.killTopology(topoName);
        System.out.println("Killed Topology");
        cluster.shutdown();
      }
    });
  }

  /** Kill topo on Ctrl-C */
  public static void setupShutdownHook(final String topoName) {
    Map clusterConf = Utils.readStormConfig();
    final Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        try {
          Helper.kill(client, topoName);
          System.out.println("Killed Topology");
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
  }

    public static void runOnClusterAndPrintMetrics(Integer durationSec, String topoName, Map topoConf, StormTopology topology) throws Exception {
      // submit topology
      StormSubmitter.submitTopologyWithProgressBar(topoName, topoConf, topology);
      setupShutdownHook(topoName); // handle Ctrl-C

      // poll metrics every minute, then kill topology after specified duration
      Integer pollIntervalSec = 60;
      collectMetricsAndKill(topoName, pollIntervalSec, durationSec);
    }
}

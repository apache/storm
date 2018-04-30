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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.storm.nimbus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.storm.Config;
import org.apache.storm.ILocalCluster;
import org.apache.storm.ISubmitterHook;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.testing.TestGlobalCount;
import org.apache.storm.testing.TestWordCounter;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests local cluster with nimbus and a plugin for {@link Config#STORM_TOPOLOGY_SUBMISSION_NOTIFIER_PLUGIN}.
 */
public class LocalNimbusTest {

    public static StormTopology createTestTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("words", new TestWordSpout(), generateParallelismHint());
        builder.setBolt("count", new TestWordCounter(), generateParallelismHint()).shuffleGrouping("words");
        builder.setBolt("globalCount", new TestGlobalCount(), generateParallelismHint()).shuffleGrouping("count");

        return builder.createTopology();
    }

    private static int generateParallelismHint() {
        return new Random().nextInt(9) + 1;
    }

    @Test
    public void testSubmitTopologyToLocalNimbus() throws Exception {
        int port = Utils.getAvailablePort();
        try (ILocalCluster localCluster = new LocalCluster.Builder()
            .withNimbusDaemon(true)
            .withDaemonConf(Config.NIMBUS_THRIFT_PORT, port)
            .build()) {
            Config topoConf = new Config();
            topoConf.putAll(Utils.readDefaultConfig());
            topoConf.setDebug(true);
            topoConf.put("storm.cluster.mode", "local"); // default is aways "distributed" but here local cluster is being used.
            topoConf.put(Config.STORM_TOPOLOGY_SUBMISSION_NOTIFIER_PLUGIN, InmemoryTopologySubmitterHook.class.getName());
            topoConf.put(Config.NIMBUS_THRIFT_PORT, port);

            List<TopologyDetails> topologyNames = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                final String topologyName = "word-count-" + UUID.randomUUID().toString();
                final StormTopology stormTopology = createTestTopology();
                topologyNames.add(new TopologyDetails(topologyName, stormTopology));
                localCluster.submitTopology(topologyName, topoConf, stormTopology);
            }

            Assert.assertEquals(InmemoryTopologySubmitterHook.submittedTopologies, topologyNames);
        }
    }

    public static class InmemoryTopologySubmitterHook implements ISubmitterHook {
        public static final List<TopologyDetails> submittedTopologies = new ArrayList<>();

        @Override
        public void notify(TopologyInfo topologyInfo, Map<String, Object> topoConf, StormTopology topology) throws IllegalAccessException {
            submittedTopologies.add(new TopologyDetails(topologyInfo.get_name(), topology));
        }
    }

    private static class TopologyDetails {
        private final String topologyName;
        private final StormTopology stormTopology;

        public TopologyDetails(String topologyName, StormTopology stormTopology) {
            this.topologyName = topologyName;
            this.stormTopology = stormTopology;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TopologyDetails)) return false;

            TopologyDetails that = (TopologyDetails) o;

            if (topologyName != null ? !topologyName.equals(that.topologyName) : that.topologyName != null) {
                return false;
            }
            return !(stormTopology != null ? !stormTopology.equals(that.stormTopology) : that.stormTopology != null);

        }

        @Override
        public int hashCode() {
            int result = topologyName != null ? topologyName.hashCode() : 0;
            result = 31 * result + (stormTopology != null ? stormTopology.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "TopologyDetails{" +
                   "topologyName='" + topologyName + '\'' +
                   ", stormTopology=" + stormTopology +
                   '}';
        }
    }
}

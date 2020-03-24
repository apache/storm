/*
 * Copyright 2018 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.messaging;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.Testing;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.testing.CompleteTopologyParam;
import org.apache.storm.testing.FixedTuple;
import org.apache.storm.testing.IntegrationTest;
import org.apache.storm.testing.MockedSources;
import org.apache.storm.testing.TestGlobalCount;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Values;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@IntegrationTest
public class NettyIntegrationTest {

    @Test
    public void testIntegration() throws Exception {
        Map<String, Object> daemonConf = new HashMap<>();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, true);
        daemonConf.put(Config.STORM_MESSAGING_TRANSPORT, "org.apache.storm.messaging.netty.Context");
        daemonConf.put(Config.STORM_MESSAGING_NETTY_AUTHENTICATION, false);
        daemonConf.put(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE, 1024000);
        daemonConf.put(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS, 1000);
        daemonConf.put(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS, 5000);
        daemonConf.put(Config.STORM_MESSAGING_NETTY_CLIENT_WORKER_THREADS, 1);
        daemonConf.put(Config.STORM_MESSAGING_NETTY_SERVER_WORKER_THREADS, 1);

        try (LocalCluster cluster = new LocalCluster.Builder().withSimulatedTime().withSupervisors(4)
                .withSupervisorSlotPortMin(6710).withDaemonConf(daemonConf).build()) {

            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("1", new TestWordSpout(true), 4);
            builder.setBolt("2", new TestGlobalCount(), 6).shuffleGrouping("1");
            StormTopology topology = builder.createTopology();

            // important for test that tuples = multiple of 4 and 6
            List<FixedTuple> testTuples = Stream.of("a", "b",
                                                    "a", "b",
                                                    "a", "b",
                                                    "a", "b",
                                                    "a", "b",
                                                    "a", "b",
                                                    "a", "b",
                                                    "a", "b",
                                                    "a", "b",
                                                    "a", "b",
                                                    "a", "b",
                                                    "a", "b")
                    .map(value -> new FixedTuple(new Values(value)))
                    .collect(Collectors.toList());

            MockedSources mockedSources = new MockedSources(Collections.singletonMap("1", testTuples));

            CompleteTopologyParam completeTopologyParams = new CompleteTopologyParam();
            completeTopologyParams.setStormConf(Collections.singletonMap(Config.TOPOLOGY_WORKERS, 3));
            completeTopologyParams.setMockedSources(mockedSources);

            Map<String, List<FixedTuple>> results = Testing.completeTopology(cluster, topology, completeTopologyParams);

            assertThat(Testing.readTuples(results, "2").size(), is(6 * 4));
        }
    }
}

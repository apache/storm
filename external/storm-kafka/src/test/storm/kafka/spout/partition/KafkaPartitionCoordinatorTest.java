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
package storm.kafka.spout.partition;

import backtype.storm.Config;
import com.google.common.collect.Lists;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import storm.kafka.*;
import storm.kafka.spout.Broker;
import storm.kafka.spout.KafkaConfig;
import storm.kafka.spout.helper.ConsumerConnectionCache;
import storm.kafka.spout.helper.KafkaUtils;
import storm.kafka.spout.helper.ZkState;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.when;

@PowerMockIgnore({"javax.management.*", "javax.xml.parsers.*", "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest(KafkaUtils.class)
public class KafkaPartitionCoordinatorTest {

    @Mock
    private ConsumerConnectionCache consumerConnectionCache;

    private KafkaTestBroker broker;
    private TestingServer server;
    private Map stormConf = new HashMap();
    private KafkaConfig spoutConfig;
    private ZkState state;
    private SimpleConsumer simpleConsumer;
    private final long REFRESH_MILLIS = 1000;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        server = new TestingServer();
        String connectionString = server.getConnectString();
        spoutConfig = new KafkaConfig(Lists.newArrayList(Broker.fromString(connectionString)), "topic");
        Map conf = buildZookeeperConfig(server);
        state = new ZkState(conf);
        broker = new KafkaTestBroker();
        simpleConsumer = new SimpleConsumer("localhost", broker.getPort(), 60000, 1024, "testClient");
        when(consumerConnectionCache.register(any(Broker.class), anyInt())).thenReturn(simpleConsumer);
    }

    private Map buildZookeeperConfig(TestingServer server) {
        Map conf = new HashMap();
        conf.put(Config.TRANSACTIONAL_ZOOKEEPER_PORT, server.getPort());
        conf.put(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, Arrays.asList("localhost"));
        conf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 20000);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, 3);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, 30);
        return conf;
    }

    @After
    public void shutdown() throws Exception {
        simpleConsumer.close();
        broker.shutdown();
        server.close();
    }

    @Test
    public void testOnePartitionPerTask() throws Exception {
        int totalTasks = 64;
        int partitionsPerTask = 1;
        PowerMockito.spy(KafkaUtils.class);
        GlobalPartitionInformation partitionInformation = TestUtils.buildPartitionInfo(totalTasks);
        when(KafkaUtils.getTopicPartitionInfo(spoutConfig.seedBrokers, spoutConfig.topic)).thenReturn(partitionInformation);
        List<KafkaPartitionCoordinator> coordinatorList = buildCoordinators(totalTasks / partitionsPerTask);
        for (KafkaPartitionCoordinator coordinator : coordinatorList) {
            List<PartitionManager> myManagedPartitions = coordinator.getMyManagedPartitions();
            assertEquals(partitionsPerTask, myManagedPartitions.size());
            assertEquals(coordinator.taskIndex, myManagedPartitions.get(0).getPartition().partition);
        }
    }


    @Test
    public void testPartitionsChange() throws Exception {
        final int totalTasks = 64;
        int partitionsPerTask = 2;
        //PowerMockito.mockStatic(KafkaUtils.class);
        PowerMockito.spy(KafkaUtils.class);

        when(KafkaUtils.getTopicPartitionInfo(spoutConfig.seedBrokers, spoutConfig.topic)).thenReturn(TestUtils.buildPartitionInfo(totalTasks, 9092));
        List<KafkaPartitionCoordinator> coordinatorList = buildCoordinators(totalTasks / partitionsPerTask);

        List<List<PartitionManager>> partitionManagersBeforeRefresh = getPartitionManagers(coordinatorList);
        waitForRefresh();

        when(KafkaUtils.getTopicPartitionInfo(spoutConfig.seedBrokers, spoutConfig.topic)).thenReturn(TestUtils.buildPartitionInfo(totalTasks, 9093));
        List<List<PartitionManager>> partitionManagersAfterRefresh = getPartitionManagers(coordinatorList);
        assertEquals(partitionManagersAfterRefresh.size(), partitionManagersAfterRefresh.size());
        Iterator<List<PartitionManager>> iterator = partitionManagersAfterRefresh.iterator();
        for (List<PartitionManager> partitionManagersBefore : partitionManagersBeforeRefresh) {
            List<PartitionManager> partitionManagersAfter = iterator.next();
            assertPartitionsAreDifferent(partitionManagersBefore, partitionManagersAfter, partitionsPerTask);
        }
    }

    private void assertPartitionsAreDifferent(List<PartitionManager> partitionManagersBefore, List<PartitionManager> partitionManagersAfter, int partitionsPerTask) {
        assertEquals(partitionsPerTask, partitionManagersBefore.size());
        assertEquals(partitionManagersBefore.size(), partitionManagersAfter.size());
        for (int i = 0; i < partitionsPerTask; i++) {
            assertNotEquals(partitionManagersBefore.get(i).getPartition(), partitionManagersAfter.get(i).getPartition());
        }

    }

    private List<List<PartitionManager>> getPartitionManagers(List<KafkaPartitionCoordinator> coordinatorList) {
        List<List<PartitionManager>> partitions = new ArrayList();
        for (PartitionCoordinator coordinator : coordinatorList) {
            partitions.add(coordinator.getMyManagedPartitions());
        }
        return partitions;
    }

    private void waitForRefresh() throws InterruptedException {
        Thread.sleep(REFRESH_MILLIS + 1);
    }

    private List<KafkaPartitionCoordinator> buildCoordinators(int totalTasks) {
        List<KafkaPartitionCoordinator> coordinatorList = new ArrayList<KafkaPartitionCoordinator>();
        for (int i = 0; i < totalTasks; i++) {
            KafkaPartitionCoordinator coordinator = new KafkaPartitionCoordinator(consumerConnectionCache, stormConf, spoutConfig, state, i, totalTasks, "test-id");
            coordinator.REFRESH_INTERVAL_MILLS = REFRESH_MILLIS;
            coordinatorList.add(coordinator);
        }
        return coordinatorList;
    }


}

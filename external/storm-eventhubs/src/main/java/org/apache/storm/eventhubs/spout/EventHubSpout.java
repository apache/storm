/*******************************************************************************
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
 *******************************************************************************/
package org.apache.storm.eventhubs.spout;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.eventhubs.core.EventHubConfig;
import org.apache.storm.eventhubs.core.EventHubMessage;
import org.apache.storm.eventhubs.core.EventHubReceiverImpl;
import org.apache.storm.eventhubs.core.FieldConstants;
import org.apache.storm.eventhubs.core.IEventHubReceiver;
import org.apache.storm.eventhubs.core.IEventHubReceiverFactory;
import org.apache.storm.eventhubs.core.IPartitionCoordinator;
import org.apache.storm.eventhubs.core.IPartitionManager;
import org.apache.storm.eventhubs.core.IPartitionManagerFactory;
import org.apache.storm.eventhubs.core.MessageId;
import org.apache.storm.eventhubs.core.PartitionManager;
import org.apache.storm.eventhubs.core.StaticPartitionCoordinator;
import org.apache.storm.eventhubs.state.IStateStore;
import org.apache.storm.eventhubs.state.ZookeeperStateStore;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

/**
 * Emit tuples (messages) from an Azure EventHub
 */
public class EventHubSpout extends BaseRichSpout {

    private static final long serialVersionUID = -8460916098313963614L;

    private static final Logger LOGGER = LoggerFactory.getLogger(EventHubSpout.class);

    private final EventHubSpoutConfig eventHubConfig;
    private final int checkpointIntervalInSeconds;
    private final IPartitionManagerFactory pmFactory;
    private final IEventHubReceiverFactory recvFactory;

    // namespacefqdn/eventhubname
    private final String eventHubPath;

    private transient IStateStore stateStore;
    private transient IPartitionCoordinator partitionCoordinator;
    private transient SpoutOutputCollector collector;

    private long lastCheckpointTime;
    private int currentPartitionIndex = -1;

    public EventHubSpout(
            final String username,
            final String password,
            final String namespace,
            final String entityPath,
            final int partitionCount) {
        this(new EventHubSpoutConfig(username, password, namespace, entityPath, partitionCount));
    }

    public EventHubSpout(
            final String username,
            final String password,
            final String namespace,
            final String entityPath,
            final int partitionCount,
            final int batchSize) {
        this(new EventHubSpoutConfig(username, password, namespace, entityPath, partitionCount, batchSize));
    }

    public EventHubSpout(final EventHubSpoutConfig spoutConfig) {
        this(spoutConfig, null, null, null);
    }

    public EventHubSpout(
            final EventHubSpoutConfig spoutConfig,
            final IStateStore store,
            final IPartitionManagerFactory pmFactory,
            final IEventHubReceiverFactory recvFactory) {
        this.eventHubConfig = spoutConfig;
        this.checkpointIntervalInSeconds = spoutConfig.getCheckpointIntervalInSeconds();
        this.lastCheckpointTime = System.currentTimeMillis();
        this.stateStore = store;
        this.pmFactory = pmFactory == null ? new PartitionManagerFactory() : pmFactory;
        this.recvFactory = recvFactory == null ? new EventHubReceiverFactory() : recvFactory;
        this.eventHubPath = String.join("/",
                spoutConfig.getTargetAddress(), spoutConfig.getEntityPath());
    }

    /**
     * This is a extracted method that is easy to test
     *
     * @param config
     * @param totalTasks
     * @param taskIndex
     * @param collector
     * @throws Exception
     */
    public void preparePartitions(
            final Map config,
            final int totalTasks,
            final int taskIndex,
            final SpoutOutputCollector collector) throws Exception {
        this.collector = collector;
        if (this.stateStore == null) {
            final String zkEndpointAddress = !StringUtils.isBlank(this.eventHubConfig.getZkConnectionString())
                    ? this.eventHubConfig.getZkConnectionString()
                    : this.getZooKeeperEndpointAddress(config);

            this.stateStore = new ZookeeperStateStore(zkEndpointAddress,
                    Integer.parseInt(config.get(Config.STORM_ZOOKEEPER_RETRY_TIMES).toString()),
                    Integer.parseInt(config.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL).toString()));
        }
        this.stateStore.open();

        LOGGER.info("EventHub: " + this.eventHubPath+ ", TaskIndex: " + taskIndex
                + ", TotalTasks: " + totalTasks + ", Total Partitions:"
                + this.eventHubConfig.getPartitionCount());
        this.partitionCoordinator = new StaticPartitionCoordinator(
                this.eventHubConfig, taskIndex, totalTasks, this.stateStore,
                this.pmFactory, this.recvFactory);

        for (final IPartitionManager partitionManager : partitionCoordinator.getMyPartitionManagers()) {
            partitionManager.open();
        }
    }

    @Override
    public void open(
            final Map config,
            final TopologyContext context,
            final SpoutOutputCollector collector) {
        LOGGER.debug(String.format("EventHubSpout(%s) start: open()", this.eventHubPath));
        final String topologyName = (String) config.get(Config.TOPOLOGY_NAME);
        this.eventHubConfig.setTopologyName(topologyName);

        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        int taskIndex = context.getThisTaskIndex();
        if (totalTasks > this.eventHubConfig.getPartitionCount()) {
            throw new RuntimeException("Total tasks of EventHubSpout " + totalTasks
                    + " is greater than partition count: " + this.eventHubConfig.getPartitionCount());
        }

        LOGGER.info(
                String.format("EventHub: %s, TopologyName: %s, TotalTasks: %d, TaskIndex: %d",
                        this.eventHubPath, topologyName, totalTasks, taskIndex));

        try {
            preparePartitions(config, totalTasks, taskIndex, collector);
        } catch (Exception e) {
            collector.reportError(e);
            throw new RuntimeException(e);
        }

        // register metrics
        context.registerMetric("EventHubReceiver", new IMetric() {
            @Override
            public Object getValueAndReset() {
                Map<String, Object> concatMetricsDataMaps = new HashMap<String, Object>();
                for (IPartitionManager partitionManager : partitionCoordinator.getMyPartitionManagers()) {
                    concatMetricsDataMaps.putAll(partitionManager.getMetricsData());
                }
                return concatMetricsDataMaps;
            }
        }, Integer.parseInt(config.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS).toString()));
        LOGGER.debug(String.format("EventHubSpout(%s) end: open()", this.eventHubPath));
    }

    @Override
    public void nextTuple() {
        final List<IPartitionManager> partitionManagers = partitionCoordinator.getMyPartitionManagers();
        EventHubMessage ehm = null;

        for (int i = 0; i < partitionManagers.size(); i++) {
            currentPartitionIndex = (currentPartitionIndex + 1) % partitionManagers.size();
            final IPartitionManager partitionManager = partitionManagers.get(currentPartitionIndex);
            if (partitionManager == null) {
                throw new RuntimeException(
                        "PartitionManager doesn't exist for partition: "
                        + String.join("/",this.eventHubPath, String.valueOf(currentPartitionIndex)));
            }

            ehm = partitionManager.receive();
            if (ehm != null) {
                break;
            }
        }

        if (ehm != null) {
            final MessageId messageId = ehm.getMessageId();
            final List<Object> tuple = eventHubConfig.getEventDataScheme().deserialize(ehm);
            collector.emit(tuple, messageId);
        }

        checkpointIfNeeded();
    }

    @Override
    public void ack(final Object msgId) {
        final MessageId messageId = (MessageId) msgId;
        final IPartitionManager partitionManager = partitionCoordinator.getPartitionManager(messageId.getPartitionId());
        final String offset = messageId.getOffset();
        partitionManager.ack(offset);
    }

    @Override
    public void fail(final Object msgId) {
        final MessageId messageId = (MessageId) msgId;
        final IPartitionManager partitionManager = partitionCoordinator.getPartitionManager(messageId.getPartitionId());
        final String offset = messageId.getOffset();
        partitionManager.fail(offset);
    }

    @Override
    public void deactivate() {
        // let's checkpoint so that we can get the last checkpoint when restarting.
        checkpoint();
    }

    @Override
    public void close() {
        for (IPartitionManager partitionManager : partitionCoordinator.getMyPartitionManagers()) {
            partitionManager.close();
        }
        stateStore.close();
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        List<String> fields = new LinkedList<String>();
        fields.add(FieldConstants.MESSAGE_FIELD);

        if (Strings.isNullOrEmpty(eventHubConfig.getOutputStreamId())) {
            declarer.declare(new Fields(fields));
        } else {
            declarer.declareStream(eventHubConfig.getOutputStreamId(), new Fields(fields));
        }
    }

    private void checkpointIfNeeded() {
        final long nextCheckpointTime = this.lastCheckpointTime + (checkpointIntervalInSeconds * 1000);
        if (nextCheckpointTime < System.currentTimeMillis()) {

            this.checkpoint();
            this.lastCheckpointTime = System.currentTimeMillis();
        }
    }

    private void checkpoint() {
        for (IPartitionManager partitionManager : partitionCoordinator.getMyPartitionManagers()) {
            partitionManager.checkpoint();
        }
    }

    private String getZooKeeperEndpointAddress(final Map config) {
        @SuppressWarnings("unchecked")
        final List<String> zkServers = (List<String>) config.get(Config.STORM_ZOOKEEPER_SERVERS);
        final Integer zkPort = ((Number) config.get(Config.STORM_ZOOKEEPER_PORT)).intValue();
        return String.join(",",
                zkServers.stream().map(x -> x + ":" + zkPort).collect(Collectors.toList()));
    }

    private static final class PartitionManagerFactory implements IPartitionManagerFactory {
        private static final long serialVersionUID = -3134660797825594845L;

        @Override
        public IPartitionManager create(EventHubConfig ehConfig,
                                        String partitionId,
                                        IStateStore stateStore,
                                        IEventHubReceiver receiver) {
            return new PartitionManager(ehConfig, partitionId, stateStore, receiver);
        }
    }

    private static final class EventHubReceiverFactory implements IEventHubReceiverFactory {
        private static final long serialVersionUID = 7215384402396274196L;

        @Override
        public IEventHubReceiver create(EventHubConfig spoutConfig, String partitionId) {
            return new EventHubReceiverImpl(spoutConfig, partitionId);
        }
    }
}

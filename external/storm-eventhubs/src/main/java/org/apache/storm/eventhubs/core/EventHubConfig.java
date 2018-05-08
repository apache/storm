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
package org.apache.storm.eventhubs.core;

import java.io.Serializable;

import org.apache.storm.eventhubs.format.IEventDataScheme;
import org.apache.storm.eventhubs.format.StringEventDataScheme;

import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;

/**
 * Captures connection details for EventHub
 */
public class EventHubConfig implements Serializable {
    private static final long serialVersionUID = -2913928074769667240L;
    protected String userName;
    protected String password;
    protected String namespace;
    protected String entityPath;
    protected int partitionCount;
    protected String zkConnectionString = null;
    protected int checkpointIntervalInSeconds = 10;
    protected int maxPendingMsgsPerPartition = FieldConstants.DEFAULT_MAX_PENDING_PER_PARTITION;
    protected int receiveEventsMaxCount = FieldConstants.DEFAULT_RECEIVE_MAX_CAP;
    protected int prefetchCount = FieldConstants.DEFAULT_PREFETCH_COUNT;
    protected long enqueueTimeFilter = 0;
    protected String connectionString;
    protected String topologyName;
    protected IEventDataScheme eventDataScheme = new StringEventDataScheme();
    protected String consumerGroupName = EventHubClient.DEFAULT_CONSUMER_GROUP_NAME;

    public EventHubConfig(String namespace, String entityPath, String userName, String password, int partitionCount) {
        this.namespace = namespace;
        this.entityPath = entityPath;
        this.userName = userName;
        this.password = password;
        this.partitionCount = partitionCount;
        this.connectionString = new ConnectionStringBuilder()
                .setNamespaceName(namespace)
                .setEventHubName(entityPath)
                .setSasKeyName(userName)
                .setSasKey(password)
                .toString();
    }

    /**
     * Returns username used in credentials provided to EventHub
     *
     * @return username
     */
    public String getUserName() {
        return userName;
    }

    /**
     * Returns password used in credentials provided to EventHub
     *
     * @return password
     */
    public String getPassword() {
        return password;
    }

    /**
     * Returns servicebus namespace used when connecting to EventHub
     *
     * @return servicebus namespace
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * Returns name of the EventHub
     *
     * @return EventHub name
     */
    public String getEntityPath() {
        return entityPath;
    }

    /**
     * Returns specified partition count on the EventHub
     *
     * @return partition count
     */
    public int getPartitionCount() {
        return partitionCount;
    }

    /**
     * Sets the zookeeper connection string. (Example:
     * zk1-clusterfqdn:2181,zk2-clusterfqdn:2181)
     *
     * @param zkConnectionString Zookeeper connection string
     */
    public void setZkConnectionString(String zkConnectionString) {
        this.zkConnectionString = zkConnectionString;
    }

    /**
     * Returns the configured zookeeper connection string.
     */
    public String getZkConnectionString() {
        return zkConnectionString;
    }

    /**
     * Returns the specified frequency interval at which checkpoint information is
     * persisted.
     *
     * @return checkpoint interval
     */
    public int getCheckpointIntervalInSeconds() {
        return checkpointIntervalInSeconds;
    }

    /**
     * Sets the frequency with which checkpoint information is persisted to
     * zookeeper
     *
     * @param checkpointIntervalInSeconds
     */
    public void setCheckpointIntervalInSeconds(int checkpointIntervalInSeconds) {
        this.checkpointIntervalInSeconds = checkpointIntervalInSeconds;
    }

    /**
     * Returns the configured the size of the pending queue for each partition.
     * While the pending queue is at full capacity no new receive calls will be made
     * to EventHub. The default value for it is
     * {@link FieldConstants#DEFAULT_MAX_PENDING_PER_PARTITION}
     *
     * @return
     */
    public int getMaxPendingMsgsPerPartition() {
        return maxPendingMsgsPerPartition;
    }

    /**
     * configured the size of the pending queue for each partition. While the
     * pending queue is at full capacity no new receive calls will be made to
     * EventHub. The default value for it is
     * {@link FieldConstants#DEFAULT_MAX_PENDING_PER_PARTITION}
     *
     * @param maxPendingMsgsPerPartition
     */
    public void setMaxPendingMsgsPerPartition(int maxPendingMsgsPerPartition) {
        this.maxPendingMsgsPerPartition = maxPendingMsgsPerPartition;
    }

    /**
     * Returns the configured upper limit on number of events that can be received
     * from EventHub per call. Default is
     * {@link FieldConstants#DEFAULT_RECEIVE_MAX_CAP}
     *
     * @return
     */
    public int getReceiveEventsMaxCount() {
        return receiveEventsMaxCount;
    }

    /**
     * Configures the upper limit on number of events that can be received from
     * EventHub per call. Default is {@link FieldConstants#DEFAULT_RECEIVE_MAX_CAP}
     * <p>
     * Setting this to a value greater than one will reduce the number of calls that
     * are made to EventHub. The received events are buffered in an internal cache
     * and fed to the spout during the nextTuple call.
     * </p>
     *
     * @param receiveEventsMaxCount
     * @return
     */
    public void setReceiveEventsMaxCount(int receiveEventsMaxCount) {
        this.receiveEventsMaxCount = receiveEventsMaxCount;
    }

    /**
     * Returns the configured value for the TimeBased filter for when to start
     * receiving events from.
     *
     * @return
     */
    public long getEnqueueTimeFilter() {
        return enqueueTimeFilter;
    }

    /**
     * Configures value for the TimeBased filter for when to start receiving events
     * from.
     *
     * @param enqueueTimeFilter
     */
    public void setEnqueueTimeFilter(long enqueueTimeFilter) {
        this.enqueueTimeFilter = enqueueTimeFilter;
    }

    /**
     * Returns the connection string used when talking to EventHub
     *
     * @return
     */
    public String getConnectionString() {
        return connectionString;
    }

    /**
     * Configures the connection string to be used when talking to EventHub
     *
     * @param connectionString
     */
    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    /**
     * Name of the toppology
     *
     * @return
     */
    public String getTopologyName() {
        return topologyName;
    }

    /**
     * Name of the topology
     *
     * @param topologyName
     */
    public void setTopologyName(String topologyName) {
        this.topologyName = topologyName;
    }

    /**
     * Returns the configured Serialization/Deserialization scheme in use.
     * <p>
     * Please refer to {@link IEventDataScheme} for implementation choices.
     * </p>
     *
     * @return
     */
    public IEventDataScheme getEventDataScheme() {
        return eventDataScheme;
    }

    /**
     * Configures Serialization/Deserialization scheme in use.
     * <p>
     * Please refer to {@link IEventDataScheme} for implementation choices.
     * </p>
     *
     * @param scheme
     */
    public void setEventDataScheme(IEventDataScheme scheme) {
        this.eventDataScheme = scheme;
    }

    /**
     * Consumer group name to use when receiving events from EventHub
     *
     * @return consumer group name
     */
    public String getConsumerGroupName() {
        return consumerGroupName;
    }

    /**
     * sets the consumer group name to use with EventHub
     *
     * @param consumerGroupName consumer group name
     */
    public void setConsumerGroupName(String consumerGroupName) {
        this.consumerGroupName = consumerGroupName;
    }

    /**
     * Gets the configured prefetch count value
     *
     * @return prefetchCount value set.
     */
    public int getPrefetchCount() {
        return prefetchCount;
    }

    /**
     * Sets the configurable prefetch value on the EventHubClient implementation.
     *
     * @param prefetchCount value for prefetch count.
     * @see PartitionReceiver#setPrefetchCount(int)
     */
    public void setPrefetchCount(int prefetchCount) {
        this.prefetchCount = prefetchCount;
    }
}

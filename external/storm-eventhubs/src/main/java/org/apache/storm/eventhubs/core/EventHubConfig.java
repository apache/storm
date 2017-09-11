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

import org.apache.storm.eventhubs.format.EventHubMessageDataScheme;
import org.apache.storm.eventhubs.format.IEventDataScheme;

import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.servicebus.ConnectionStringBuilder;

/**
 * Captures connection details for EventHub
 *
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
	protected int receiverCredits = 1024;
	protected int maxPendingMsgsPerPartition = 1024;
	protected int receiveEventsMaxCount = FieldConstants.DEFAULT_RECEIVE_MAX_CAP;
	protected int prefetchCount = FieldConstants.DEFAULT_PREFETCH_COUNT;	
	protected long enqueueTimeFilter = 0;
	protected String connectionString;
	protected String topologyName;
	protected IEventDataScheme eventDataScheme = new EventHubMessageDataScheme();
	protected String consumerGroupName = EventHubClient.DEFAULT_CONSUMER_GROUP_NAME;

	public EventHubConfig(String namespace, String entityPath, String userName, String password, int partitionCount) {
		this.namespace = namespace;
		this.entityPath = entityPath;
		this.userName = userName;
		this.password = password;
		this.partitionCount = partitionCount;
		this.connectionString = new ConnectionStringBuilder(namespace, entityPath, userName, password).toString();
	}

	public String getUserName() {
		return userName;
	}

	public String getPassword() {
		return password;
	}

	public String getNamespace() {
		return namespace;
	}

	public String getEntityPath() {
		return entityPath;
	}

	public int getPartitionCount() {
		return partitionCount;
	}

	public void setZkConnectionString(String zkConnectionString) {
		this.zkConnectionString = zkConnectionString;
	}

	public String getZkConnectionString() {
		return zkConnectionString;
	}

	public int getCheckpointIntervalInSeconds() {
		return checkpointIntervalInSeconds;
	}

	public void setCheckpointIntervalInSeconds(int checkpointIntervalInSeconds) {
		this.checkpointIntervalInSeconds = checkpointIntervalInSeconds;
	}

	public int getReceiverCredits() {
		return receiverCredits;
	}

	public void setReceiverCredits(int receiverCredits) {
		this.receiverCredits = receiverCredits;
	}

	public int getMaxPendingMsgsPerPartition() {
		return maxPendingMsgsPerPartition;
	}

	public void setMaxPendingMsgsPerPartition(int maxPendingMsgsPerPartition) {
		this.maxPendingMsgsPerPartition = maxPendingMsgsPerPartition;
	}

	public int getReceiveEventsMaxCount() {
		return receiveEventsMaxCount;
	}

	public void setReceiveEventsMaxCount(int receiveEventsMaxCount) {
		this.receiveEventsMaxCount = receiveEventsMaxCount;
	}

	public long getEnqueueTimeFilter() {
		return enqueueTimeFilter;
	}

	public void setEnqueueTimeFilter(long enqueueTimeFilter) {
		this.enqueueTimeFilter = enqueueTimeFilter;
	}

	public String getConnectionString() {
		return connectionString;
	}

	public void setConnectionString(String connectionString) {
		this.connectionString = connectionString;
	}

	public String getTopologyName() {
		return topologyName;
	}

	public void setTopologyName(String topologyName) {
		this.topologyName = topologyName;
	}

	public IEventDataScheme getEventDataScheme() {
		return eventDataScheme;
	}

	public void setEventDataScheme(IEventDataScheme scheme) {
		this.eventDataScheme = scheme;
	}

	public String getConsumerGroupName() {
		return consumerGroupName;
	}

	public void setConsumerGroupName(String consumerGroupName) {
		this.consumerGroupName = consumerGroupName;
	}

	public int getPrefetchCount() {
		return prefetchCount;
	}

	public void setPrefetchCount(int prefetchCount) {
		this.prefetchCount = prefetchCount;
	}
}

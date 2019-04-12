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

import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.servicebus.ConnectionStringBuilder;

import java.io.Serializable;

import org.apache.storm.eventhubs.format.IEventDataScheme;
import org.apache.storm.eventhubs.format.StringEventDataScheme;

public class EventHubConfig implements Serializable {
    private static final long serialVersionUID = -2913928074769667240L;

    protected final String userName;
    protected final String password;
    protected final String namespace;
    protected final String entityPath;
    protected final int partitionCount;

    protected String zkConnectionString = null; // if null then use zookeeper used by Storm
    protected int checkpointIntervalInSeconds = 10;
    protected int receiverCredits = 1024;
    protected int maxPendingMsgsPerPartition = 1024;
    protected int receiveEventsMaxCount = 1;
    
    protected int prefetchCount = 999;
    protected long enqueueTimeFilter = 0L; // timestamp in millisecond, 0 means disabling filter
    protected String connectionString;
    protected String topologyName;
    protected IEventDataScheme scheme = new StringEventDataScheme();
    protected String consumerGroupName = EventHubClient.DEFAULT_CONSUMER_GROUP_NAME;

    public EventHubConfig(String namespace, String entityPath, String username,
            String password, int partitionCount) {
        this.namespace = namespace;
        this.entityPath = entityPath;
        this.userName = username;
        this.password = password;
        this.partitionCount = partitionCount;
        this.connectionString = new ConnectionStringBuilder(namespace, entityPath, 
            username, password).toString();
    }
    
    public String getUserName() {
    	return this.userName;
    }
    
    public String getPassword() {
    	return this.password;
    }

    public String getNamespace() {
        return this.namespace;
    }

    public String getEntityPath() {
        return this.entityPath;
    }

    public int getPartitionCount() {
        return this.partitionCount;
    }

    public void setZkConnectionString(String value) {
    	this.zkConnectionString = value;
    }

    public String getZkConnectionString() {
        return this.zkConnectionString;
    }

    public int getCheckpointIntervalInSeconds() {
        return this.checkpointIntervalInSeconds;
    }

    public void setCheckpointIntervalInSeconds(int value) {
    	this.checkpointIntervalInSeconds = value;
    }

    @Deprecated
    public int getReceiverCredits() {
        return this.receiverCredits;
    }

    @Deprecated
    public void setReceiverCredits(int value) {
    	this.receiverCredits = value;
    }

    public int getMaxPendingMsgsPerPartition() {
        return this.maxPendingMsgsPerPartition;
    }

    public void setMaxPendingMsgsPerPartition(int maxPendingMsgsPerPartition) {
    	this.maxPendingMsgsPerPartition = maxPendingMsgsPerPartition;
    }
    
    public int getReceiveEventsMaxCount() {
    	return this.receiveEventsMaxCount;
    }
    
    public void setReceiveEventsMaxCount(int receiveEventsMaxCount) {
    	this.receiveEventsMaxCount = receiveEventsMaxCount;
    }

    public long getEnqueueTimeFilter() {
        return this.enqueueTimeFilter;
    }

    public void setEnqueueTimeFilter(long enqueueTimeFilter) {
    	this.enqueueTimeFilter = enqueueTimeFilter;
    }

    public String getConnectionString() {
      return this.connectionString;
    }
    
    public void setConnectionString(String connectionString) {
      this.connectionString = connectionString;
    }    
    
    public String getTopologyName() {
        return this.topologyName;
    }

    public void setTopologyName(String topologyName) {
    	this.topologyName = topologyName;
    }

    public IEventDataScheme getEventDataScheme() {
        return this.scheme;
    }

    public void setEventDataScheme(IEventDataScheme scheme) {
        this.scheme = scheme;
    }

    public String getConsumerGroupName() {
        return this.consumerGroupName;
    }

    public void setConsumerGroupName(String consumerGroupName) {
    	this.consumerGroupName = consumerGroupName;
    }

    public int getPrefetchCount() {
    	return this.prefetchCount;
    }
    
    public void setPrefetchCount(int prefetchCount) {
    	this.prefetchCount = prefetchCount;
    }
}

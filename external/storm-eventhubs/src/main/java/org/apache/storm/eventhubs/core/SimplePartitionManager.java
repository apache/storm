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

import com.google.common.collect.Iterables;

import com.microsoft.azure.eventhubs.EventData;

import org.apache.storm.eventhubs.state.IStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;

/**
 * A simple partition manager that does not re-send failed messages
 */
public class SimplePartitionManager implements IPartitionManager {
	private static final Logger logger = LoggerFactory.getLogger(SimplePartitionManager.class);
	protected static final String statePathPrefix = "/eventhubspout";

	protected final IEventHubReceiver receiver;
	protected String lastOffset = FieldConstants.DefaultStartingOffset;
	protected String committedOffset = FieldConstants.DefaultStartingOffset;
  
	protected final EventHubConfig config;
	protected final String partitionId;
	protected final IStateStore stateStore;
	protected final String statePath;
  
	public SimplePartitionManager(EventHubConfig ehConfig, String partitionId, IStateStore stateStore,
			IEventHubReceiver receiver) {
		this.receiver = receiver;
		this.config = ehConfig;
		this.partitionId = partitionId;
		this.statePath = this.getPartitionStatePath();
		this.stateStore = stateStore;
	}
  
	@Override
	public void open() throws Exception {
		// Read from state store, if not found, use startingOffset
		String offset = this.stateStore.readData(this.statePath);
		logger.debug("read offset from state store: " + offset);
		
		IEventFilter filter;
		if ((offset == null) && (this.config.getEnqueueTimeFilter() != 0L)) {
			filter = new TimestampFilter(Instant.ofEpochMilli(this.config.getEnqueueTimeFilter()));
		} else {
			filter = new OffsetFilter((offset == null) ? FieldConstants.DefaultStartingOffset : offset);
		}

		this.receiver.open(filter);
	}
  
	@Override
	public void close() {
		this.receiver.close();
		this.checkpoint();
	}
  
	@Override
	public void checkpoint() {
		String completedOffset = getCompletedOffset();
		if (this.committedOffset.equals(completedOffset)) {
			logger.debug("No checkpointing needed. Completed Offset: " + completedOffset);
			return;
		}
		logger.debug("saving Offset: " + completedOffset + ", to path: " + this.statePath);
		this.stateStore.saveData(this.statePath, completedOffset);
		this.committedOffset = completedOffset;
  	}
  
	protected String getCompletedOffset() {
		return lastOffset;
	}

	@Override
	public EventHubMessage receive() {
		EventHubMessage msg = null;
		
		Iterable<EventData> receivedEvent = this.receiver.receive(1);
		EventData lastEvent = (EventData)Iterables.getLast(receivedEvent);
		if (lastEvent != null) {
			msg = new EventHubMessage(lastEvent, this.partitionId);
			this.lastOffset = msg.getOffset();
		}
		return msg;
	}

	@Override
	public void ack(String offset) {
		// do nothing
	}

	@Override
	public void fail(String offset) {
		logger.warn("fail on " + offset);
		// do nothing
	}
  
	private String getPartitionStatePath() {
		// Partition state path = 
		// "/{prefix}/{topologyName}/{namespace}/{entityPath}/partitions/{partitionId}/state";
	    String partitionStatePath = String.join("/", new String[] { statePathPrefix, this.config.getTopologyName(),
	    		this.config.getNamespace(), this.config.getEntityPath(), "partitions", this.partitionId });
	    logger.debug("partition state path: " + partitionStatePath);
	    return partitionStatePath;
	}
  
	@Override
	public Map<String, Object> getMetricsData() {
		return receiver.getMetricsData();
	}
	
	public String getPartitionId() {
		return this.partitionId;
	}
}

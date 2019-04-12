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

import org.apache.storm.eventhubs.state.IStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.eventhubs.EventData;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;

public class PartitionManager extends SimplePartitionManager {
	private static final Logger logger = LoggerFactory.getLogger(PartitionManager.class);

	// All sent events are stored in pending
	private final Map<String, EventHubMessage> pending;
	// All failed events are put in toResend, which is sorted by event's offset
	private final TreeSet<EventHubMessage> toResend;
	private final TreeSet<EventHubMessage> waitingToEmit;

	public PartitionManager(EventHubConfig spoutConfig, String partitionId, IStateStore stateStore,
			IEventHubReceiver receiver) {
		super(spoutConfig, partitionId, stateStore, receiver);
    
		this.pending = new LinkedHashMap<String, EventHubMessage>();
		this.toResend = new TreeSet<EventHubMessage>();
		this.waitingToEmit = new TreeSet<EventHubMessage>();
	}
	
	private void fill()
	{
	    Iterable<EventData> receivedEvents = this.receiver.receive(this.config.getReceiveEventsMaxCount());
	    if ((receivedEvents == null) || (receivedEvents.spliterator().getExactSizeIfKnown() == 0L))
	    {
	    	logger.debug("No messages received from EventHub.");
	    	return;
	    }
	    String startOffset = null;
	    String endOffset = null;
	    for (EventData ed : receivedEvents)
	    {
	    	EventHubMessage ehm = new EventHubMessage(ed, this.partitionId);
	    	startOffset = startOffset == null ? ehm.getOffset() : startOffset;
	    	endOffset = ehm.getOffset();
	    	this.waitingToEmit.add(ehm);
	    }
	    logger.debug("Received Messages Start Offset: " + startOffset + ", End Offset: " + endOffset);
	}
	
	@Override
	public EventHubMessage receive() {
		logger.debug("Retrieving messages for partition: " + this.partitionId);
		
		int countToRetrieve = this.pending.size() - this.config.getMaxPendingMsgsPerPartition();
		if (countToRetrieve >= 0) {
			logger.debug("Pending queue has more than " + this.config.getMaxPendingMsgsPerPartition() +
					" messages. No new events will be retrieved from EventHub.");
			return null;
		}

    	EventHubMessage ehm = null;
    	if (!this.toResend.isEmpty()) {
    	    ehm = toResend.pollFirst();
    	} else {
    	    if (this.waitingToEmit.isEmpty()) {
    	        fill();
    	    }
    	    ehm = this.waitingToEmit.pollFirst();
    	}

    	if (ehm == null) {
    		logger.debug("No messages pending or waiting for reprocessing.");
    		return null;
    	}
    	
    	this.lastOffset = ehm.getOffset();
    	this.pending.put(this.lastOffset, ehm);

    	return ehm;
	}

	@Override
	public void ack(String offset) {
		this.pending.remove(offset);
	}

	@Override
	public void fail(String offset) {
		logger.warn("fail on " + offset);
		this.toResend.add(this.pending.remove(offset));
	}
  
	@Override
	protected String getCompletedOffset() {
		String offset = null;
    
		if (this.pending.size() > 0) {
			// find the smallest offset in pending list
			offset = pending.keySet().iterator().next();
		}
		if (this.toResend.size() > 0) {
			// Find the smallest offset in toResend list
			String offset2 = this.toResend.first().getOffset();
			if ((offset == null) || (offset2.compareTo(offset) < 0)) {
				offset = offset2;
			}
		}
		if (offset == null) {
			offset = this.lastOffset;
		}
		return offset;
	}
}

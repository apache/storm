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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.storm.eventhubs.state.IStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.minlog.Log;
import com.microsoft.azure.eventhubs.EventData;

public class PartitionManager extends SimplePartitionManager {
	private static final Logger logger = LoggerFactory.getLogger(PartitionManager.class);

	// all sent events are stored in pending
	private final Map<String, EventHubMessage> pending;

	// all failed events are put in toResend, which is sorted by event's offset
	private final TreeSet<EventHubMessage> toResend;

	private final TreeSet<EventHubMessage> waitingToEmit;

	public PartitionManager(EventHubConfig ehConfig, String partitionId, IStateStore stateStore,
			IEventHubReceiver receiver) {
		super(ehConfig, partitionId, stateStore, receiver);

		this.pending = new LinkedHashMap<String, EventHubMessage>();
		this.toResend = new TreeSet<EventHubMessage>();
		this.waitingToEmit = new TreeSet<EventHubMessage>();
	}

	private void fill() {
		Iterable<EventData> receivedEvents = receiver.receive(config.getReceiveEventsMaxCount());
		if (receivedEvents == null || receivedEvents.spliterator().getExactSizeIfKnown() == 0) {
			logger.debug("No messages received from EventHub.");
			return;
		}

		String startOffset = null;
		String endOffset = null;
		for (EventData ed : receivedEvents) {
			EventHubMessage ehm = new EventHubMessage(ed, partitionId);
			startOffset = (startOffset == null) ? ehm.getOffset() : startOffset;
			endOffset = ehm.getOffset();
			waitingToEmit.add(ehm);
		}

		logger.debug("Received Messages Start Offset: " + startOffset + ", End Offset: " + endOffset);
	}

	@Override
	public EventHubMessage receive() {
		logger.debug("Retrieving messages for partition: " + partitionId);
		int countToRetrieve = pending.size() - config.getMaxPendingMsgsPerPartition();

		if (countToRetrieve >= 0) {
			Log.debug("Pending queue has more than " + config.getMaxPendingMsgsPerPartition()
					+ " messages. No new events will be retrieved from EventHub.");
			return null;
		}

		EventHubMessage ehm = null;
		if (!toResend.isEmpty()) {
			ehm = toResend.pollFirst();
		} else {
			if (waitingToEmit.isEmpty()) {
				fill();
			}
			ehm = waitingToEmit.pollFirst();
		}

		if (ehm == null) {
			logger.debug("No messages pending or waiting for reprocessing.");
			return null;
		}

		lastOffset = ehm.getOffset();
		pending.put(lastOffset, ehm);
		return ehm;
	}

	@Override
	public void ack(String offset) {
		pending.remove(offset);
	}

	@Override
	public void fail(String offset) {
		logger.warn("fail on " + offset);
		toResend.add(pending.remove(offset));
	}

	@Override
	protected String getCompletedOffset() {
		String offset = null;

		if (pending.size() > 0) {
			// find the smallest offset in pending list
			offset = pending.keySet().iterator().next();
		}
		if (toResend.size() > 0) {
			// find the smallest offset in toResend list
			String offset2 = toResend.first().getOffset();
			if (offset == null || offset2.compareTo(offset) < 0) {
				offset = offset2;
			}
		}
		if (offset == null) {
			offset = lastOffset;
		}
		return offset;
	}
}

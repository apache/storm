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

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.storm.eventhubs.core.IEventFilter;
import org.apache.storm.eventhubs.core.IEventHubReceiver;
import org.apache.storm.eventhubs.core.OffsetFilter;
import org.apache.storm.eventhubs.core.TimestampFilter;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.servicebus.ServiceBusException;
import com.microsoft.azure.servicebus.amqp.AmqpConstants;

/**
 * A mock receiver that emits fake data with offset starting from given offset
 * and increase by 1 each time.
 */
public class EventHubReceiverMock implements IEventHubReceiver {
	private enum BodyType {
		Data, AmqpSequence, AmqpValue
	};

	private static boolean isPaused = false;
	private final String partitionId;
	private long currentOffset;
	private boolean isOpen;
	private BodyType bodyType;

	public EventHubReceiverMock(String pid) {
		this(pid, BodyType.Data);
	}

	public EventHubReceiverMock(String pid, BodyType bodyType) {
		partitionId = pid;
		isPaused = false;
		this.bodyType = bodyType;
	}

	/**
	 * Use this method to pause/resume all the receivers. If paused all receiver
	 * will return null.
	 * 
	 * @param val
	 */
	public static void setPause(boolean val) {
		isPaused = val;
	}

	public static EventData getEventData(String body, String offset, Long sequence, Date enqueueTime) {
		HashMap<String, Object> m = new HashMap<String, Object>();
		m.put(AmqpConstants.OFFSET_ANNOTATION_NAME, offset);
		m.put(AmqpConstants.SEQUENCE_NUMBER_ANNOTATION_NAME, sequence);
		m.put(AmqpConstants.ENQUEUED_TIME_UTC_ANNOTATION_NAME, enqueueTime);
		EventData ed = new EventDataMock(body.getBytes(), m);
		return ed;
	}

	@Override
	public void open(IEventFilter filter) throws IOException, ServiceBusException {
		currentOffset = (filter instanceof OffsetFilter) ? Long.parseLong(((OffsetFilter) filter).getOffset())
				: ((TimestampFilter) filter).getTime().toEpochMilli();
		isOpen = true;
	}

	@Override
	public void close() {
		isOpen = false;
	}

	@Override
	public boolean isOpen() {
		return isOpen;
	}

	@Override
	public Iterable<EventData> receive() {
		return receive(1);
	}

	@Override
	public Iterable<EventData> receive(int batchSize) {
		if (isPaused) {
			return new LinkedList<EventData>();
		}

		List<EventData> retList = new LinkedList<EventData>();
		for (int i = 0; i < batchSize; ++i) {
			currentOffset++;
			EventData edata = getEventData("message" + currentOffset, String.valueOf(currentOffset),
					(long) currentOffset, new Date());
			retList.add(edata);
		}

		return retList;
	}

	@Override
	public Map getMetricsData() {
		return null;
	}
}

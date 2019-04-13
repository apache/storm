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

import java.time.Instant;
import java.util.LinkedList;
import java.util.Map;

import org.apache.storm.eventhubs.core.IEventFilter;
import org.apache.storm.eventhubs.core.IEventHubReceiver;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubException;

/**
 * A mock receiver that emits fake data with offset starting from given offset
 * and increase by 1 each time.
 */
public class EventHubReceiverMock implements IEventHubReceiver {
	private static boolean isPaused = false;
	private long currentOffset;
	private boolean isOpen;

	public EventHubReceiverMock(String pid) {
		// partition id (pid) isn't used
		isPaused = false;
	}
  
	/**
	 * Use this method to pause/resume all the receivers.
	 * If paused all receiver will return null.
	 * @param val
	 */
	public static void setPause(boolean val) {
		isPaused = val;
	}

	@Override
	public void open(IEventFilter filter) throws EventHubException {
		currentOffset = Long.parseLong(filter.getEventPosition().getOffset());
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
	public Iterable<EventData> receive(int count) {
		if (isPaused) {
			return null;
		}

		currentOffset++;
		EventData ed = EventData.create(("message" + currentOffset).getBytes());
		EventData.SystemProperties sysprops = new EventData.SystemProperties(currentOffset, Instant.now(), String.valueOf(currentOffset), null);
		ed.setSystemProperties(sysprops);
		LinkedList<EventData> events = new LinkedList<EventData>();
		events.add(ed);
		return events;
	}
  
	@Override
	public Map<String, Object> getMetricsData() {
		return null;
	}
}

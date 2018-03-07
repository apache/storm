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

import java.io.IOException;
import java.util.Map;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubException;

/**
 * EventHub based Receiver contracts
 *
 */
public interface IEventHubReceiver {

	/**
	 * Open / Establish connection to Eventhub given filters. The partition to
	 * receive events from will be specified in an implementation specific way.
	 * 
	 * @param filter
	 *            offset or timestamp based filter
	 * @throws EventHubException
	 * @see {@link IEventFilter} {@link OffsetFilter} {@link TimestampFilter}
	 */
	void open(IEventFilter filter) throws IOException, EventHubException ;

	/**
	 * Cleanup and close connection to Eventhub
	 */
	void close();

	/**
	 * Check if connection to eventhub is active
	 * 
	 * @return
	 */
	boolean isOpen();

	/**
	 * Receive 'one' event from EventHub for processing from a target partition
	 * 
	 * @return
	 */
	Iterable<EventData> receive();

	/**
	 * Receive up to specified number of events from EventHub for processing from a
	 * target partition.
	 * 
	 * @param maxEventCount
	 *            maximum number of events to read
	 * @return events read from eventhub
	 */
	Iterable<EventData> receive(int maxEventCount);

	/**
	 * Metrics capturing call rate, and mean execution time.
	 * 
	 * @return Map of key,value pairs for metrics
	 */
	Map<String, Object> getMetricsData();
}

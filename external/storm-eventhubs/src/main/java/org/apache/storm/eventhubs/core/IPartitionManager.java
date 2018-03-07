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

import java.util.Map;

import org.apache.storm.metric.api.IMetric;

import com.microsoft.azure.eventhubs.EventData;

/**
 * Read/Write events from an EventHub partition.
 *
 */
public interface IPartitionManager {

	/**
	 * Establishes connection to EventHub
	 * 
	 * @throws Exception
	 */
	void open() throws Exception;

	/**
	 * Closes EventHub connection
	 */
	void close();

	/**
	 * returns a list of {@link EventData} from an EventHub partition
	 * 
	 * @return {@link EventData}
	 */
	EventHubMessage receive();

	/**
	 * Persists the last successfully processed EventHub offset information to a
	 * state store.
	 */
	void checkpoint();

	/**
	 * ACK's successful processing of {@link EventData} identified by given offset.
	 * The specifics of successfully processed message handling are up to the
	 * implementation.
	 * 
	 * @param offset
	 *            index of processed message.
	 */
	void ack(String offset);

	/**
	 * Handles failed processing of {@link EventData} identified by given offset.
	 * The specifics of failed message handling are up to the implementation.
	 * 
	 * @param offset
	 *            index of processed message.
	 */
	void fail(String offset);

	/**
	 * Returns pairs of {<String, Object>} metrics collected. See {@link IMetric}
	 * implementations for better understanding.
	 * 
	 * @return Map
	 */
	Map<String, Object> getMetricsData();

	/**
	 * Returns the partitionId that this manager is responsible for.
	 * 
	 * @return partition id.
	 */
	String getPartitionId();
}

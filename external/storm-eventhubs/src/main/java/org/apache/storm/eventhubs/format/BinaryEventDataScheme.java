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
package org.apache.storm.eventhubs.format;

import java.util.LinkedList;
import java.util.List;

import org.apache.storm.eventhubs.core.EventHubMessage;
import org.apache.storm.eventhubs.core.FieldConstants;
import org.apache.storm.tuple.Fields;

import com.microsoft.azure.eventhubs.EventData;

/**
 * An Event Data Scheme which deserializes message payload into the raw bytes.
 *
 * The resulting tuple may contain upto three fields. The first field will
 * always be present, and will contain the byte array for the eventhub message.
 * Depending on the configuration parameters if Application and System
 * properties are required, the tuple will contain two maps, one for the AMQP
 * application properties, and the second for the System properties associated
 * to the tuple.
 * 
 * @see EventData.SystemProperties
 */
public class BinaryEventDataScheme implements IEventDataScheme {

	private static final long serialVersionUID = 5548996695376773616L;

	@Override
	public List<Object> deserialize(EventHubMessage eventHubMessage) {
		List<Object> contents = new LinkedList<Object>();
		contents.add(eventHubMessage.getContent());
		contents.add(eventHubMessage.getApplicationProperties());
		contents.add(eventHubMessage.getSystemProperties());
		return contents;
	}

	@Override
	public Fields getOutputFields() {
		return new Fields(FieldConstants.MESSAGE_FIELD, FieldConstants.META_DATA_FIELD,
				FieldConstants.SYSTEM_PROPERTIES_FIELD);
	}
}

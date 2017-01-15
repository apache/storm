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

import org.apache.storm.tuple.Fields;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.amqp_1_0.client.Message;
import org.apache.qpid.amqp_1_0.type.Section;
import org.apache.qpid.amqp_1_0.type.messaging.AmqpValue;
import org.apache.qpid.amqp_1_0.type.messaging.ApplicationProperties;
import org.apache.qpid.amqp_1_0.type.messaging.Data;

/**
 * An Event Data Scheme which deserializes message payload into the Strings. No
 * encoding is assumed. The receiver will need to handle parsing of the string
 * data in appropriate encoding.
 *
 * The resulting tuple would contain two items: the the message string, and a
 * map of properties that include metadata, which can be used to determine who
 * processes the message, and how it is processed.
 * 
 * For passing the raw bytes of a messsage to Bolts, refer to
 * {@link BinaryEventDataScheme}.
 */
public class EventDataScheme implements IEventDataScheme {

	private static final long serialVersionUID = 1L;

	@Override
	public List<Object> deserialize(Message message) {
		final List<Object> fieldContents = new ArrayList<Object>();

		Map metaDataMap = new HashMap();
		String messageData = "";

		for (Section section : message.getPayload()) {
			if (section instanceof Data) {
				Data data = (Data) section;
				messageData = new String(data.getValue().getArray());
			} else if (section instanceof AmqpValue) {
				AmqpValue amqpValue = (AmqpValue) section;
				messageData = amqpValue.getValue().toString();
			} else if (section instanceof ApplicationProperties) {
				final ApplicationProperties applicationProperties = (ApplicationProperties) section;
				metaDataMap = applicationProperties.getValue();
			}
		}

		fieldContents.add(messageData);
		fieldContents.add(metaDataMap);
		return fieldContents;
	}

	@Override
	public Fields getOutputFields() {
		return new Fields(FieldConstants.Message, FieldConstants.META_DATA);
	}
}

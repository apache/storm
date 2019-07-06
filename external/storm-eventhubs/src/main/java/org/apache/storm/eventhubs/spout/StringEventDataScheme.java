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

import com.microsoft.azure.eventhubs.EventData;
import java.util.ArrayList;
import java.util.List;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An Event Data Scheme which deserializes message payload into the Strings. No encoding is assumed. The receiver will need to handle
 * parsing of the string data in appropriate encoding.
 *
 * <p>Note: Unlike other schemes provided, this scheme does not include any metadata.
 *
 * <p>For metadata please refer to {@link BinaryEventDataScheme}, {@link EventDataScheme}
 */
public class StringEventDataScheme implements IEventDataScheme {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(StringEventDataScheme.class);

    @Override
    public List<Object> deserialize(EventData eventData) {
        final List<Object> fieldContents = new ArrayList<Object>();
        String messageData = "";
        if (eventData.getBytes() != null) {
            messageData = new String(eventData.getBytes());
        } else if (eventData.getObject() != null) {
            /*Will only serialize AMQPValue type*/
            try {
                if (!(eventData.getObject() instanceof List)) {
                    messageData = eventData.getObject().toString();
                } else {
                    throw new RuntimeException("Cannot serialize the given AMQP type.");
                }
            } catch (RuntimeException e) {
                logger.error("Failed to serialize EventData payload class"
                             + eventData.getObject().getClass());
                logger.error("Exception encountered while serializing EventData payload is"
                             + e.toString());
                throw e;
            }
        }
        fieldContents.add(messageData);
        return fieldContents;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(FieldConstants.Message);
    }
}

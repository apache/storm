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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An Event Data Scheme which deserializes message payload into the raw bytes.
 *
 * <p>The resulting tuple would contain three items, the first being the message bytes, and the second a map of properties that include
 * metadata, which can be used to determine who processes the message, and how it is processed.The third is the system properties which
 * exposes information like enqueue-time, offset and sequence number
 */
public class BinaryEventDataScheme implements IEventDataScheme {

    private static final Logger logger = LoggerFactory.getLogger(BinaryEventDataScheme.class);

    @Override
    public List<Object> deserialize(EventData eventData) {
        final List<Object> fieldContents = new ArrayList<Object>();
        byte[] messageData = null;
        if (eventData.getBytes() != null) {
            messageData = eventData.getBytes();
        } else if (eventData.getObject() != null) {
            try {
                messageData = SerializeDeserializeUtil.serialize(eventData.getObject());
            } catch (IOException e) {
                logger.error("Failed to serialize EventData payload class"
                             + eventData.getObject().getClass());
                logger.error("Exception encountered while serializing EventData payload is"
                             + e.toString());
                throw new RuntimeException(e);
            }
        }
        Map<String, Object> metaDataMap = eventData.getProperties();
        Map<String, Object> systemMetaDataMap = eventData.getSystemProperties();
        fieldContents.add(messageData);
        fieldContents.add(metaDataMap);
        fieldContents.add(systemMetaDataMap);
        return fieldContents;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(FieldConstants.Message, FieldConstants.META_DATA,
                          FieldConstants.SYSTEM_META_DATA);
    }
}

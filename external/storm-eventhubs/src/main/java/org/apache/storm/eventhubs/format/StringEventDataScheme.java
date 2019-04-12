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

import org.apache.storm.eventhubs.core.EventHubMessage;
import org.apache.storm.eventhubs.core.FieldConstants;
import org.apache.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.List;

/**
 * An Event Data Scheme which deserializes message payload into the Strings.
 * No encoding is assumed. The receiver will need to handle parsing of the
 * string data in appropriate encoding.
 *
 * Note: Unlike other schemes provided, this scheme does not include any
 * metadata.
 *
 * For metadata please refer to {@link BinaryEventDataScheme}, {@link EventDataScheme}
 */
public class StringEventDataScheme implements IEventDataScheme {
    private static final long serialVersionUID = 1L;

    @Override
    public List<Object> deserialize(EventHubMessage eventHubMessage) {
        final List<Object> fieldContents = new ArrayList<Object>();
        String messageData = new String(eventHubMessage.getContent());
        fieldContents.add(messageData);
        return fieldContents;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(FieldConstants.MESSAGE_FIELD);
    }
}

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
 * This scheme constructs an {@link EventHubMessage} object from the received
 * EventHub events, and exposes the constructed EventHubMessage object as a single
 * tuple.
 *
 * @see EventHubMessage
 */
public class EventHubMessageDataScheme implements IEventDataScheme {

    private static final long serialVersionUID = 5548996695376773616L;

    @Override
    public Fields getOutputFields() {
        List<String> fields = new LinkedList<String>();
        fields.add(FieldConstants.MESSAGE_FIELD);

        return new Fields(fields);
    }

    @Override
    public List<Object> deserialize(EventHubMessage eventHubMessage) {
        List<Object> contents = new LinkedList<Object>();
        contents.add(eventHubMessage);
        return contents;
    }
}

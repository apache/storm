/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.mqtt.mappers;

import org.apache.storm.mqtt.MqttMessage;
import org.apache.storm.mqtt.MqttMessageMapper;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * Given a String topic and byte[] message, emits a tuple with fields
 * "topic" and "message", both of which are Strings.
 */
public class StringMessageMapper implements MqttMessageMapper {
    @Override
    public Values toValues(MqttMessage message) {
        return new Values(message.getTopic(), new String(message.getMessage()));
    }

    @Override
    public Fields outputFields() {
        return new Fields("topic", "message");
    }
}

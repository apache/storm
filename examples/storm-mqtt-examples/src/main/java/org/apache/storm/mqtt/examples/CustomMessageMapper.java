/**
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
 */

package org.apache.storm.mqtt.examples;

import org.apache.storm.mqtt.MqttMessage;
import org.apache.storm.mqtt.MqttMessageMapper;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Given a topic name: "users/{user}/{location}/{deviceId}"
 * and a payload of "{temperature}/{humidity}"
 * emits a tuple containing
 * {@code user(String), deviceId(String), location(String), temperature(float),
 * humidity(float)}.
 */
public final class CustomMessageMapper implements MqttMessageMapper {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(
            CustomMessageMapper.class);
    private static final int TOPIC_INDEX_1 = 2;
    private static final int TOPIC_INDEX_2 = 4;
    private static final int TOPIC_INDEX_3 = 3;

    /**
     * Converts MQTT message to an instance of {@code Values}.
     * @param message the message to convert
     * @return the converted values
     */
    @Override
    public Values toValues(final MqttMessage message) {
        String topic = message.getTopic();
        String[] topicElements = topic.split("/");
        String[] payloadElements = new String(message.getMessage()).split("/");

        return new Values(topicElements[TOPIC_INDEX_1],
                topicElements[TOPIC_INDEX_2],
                topicElements[TOPIC_INDEX_3],
                Float.parseFloat(payloadElements[0]),
                Float.parseFloat(payloadElements[1]));
    }

    /**
     * Gets the output fields.
     * @return the output fields
     */
    @Override
    public Fields outputFields() {
        return new Fields("user",
                "deviceId",
                "location",
                "temperature",
                "humidity");
    }

    /**
     * Constructor.
     */
    public CustomMessageMapper() {
    }
}

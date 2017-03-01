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
package org.apache.storm.kafka.bolt.selector;

import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uses field with a given index to select the topic name from a tuple .
 */
public class FieldIndexTopicSelector implements KafkaTopicSelector {
    private static final long serialVersionUID = -3830575380208166367L;

    private static final Logger LOG = LoggerFactory.getLogger(FieldIndexTopicSelector.class);

    private final int fieldIndex;
    private final String defaultTopicName;

    public FieldIndexTopicSelector(int fieldIndex, String defaultTopicName) {
        this.fieldIndex = fieldIndex;
        if (fieldIndex < 0) {
            throw new IllegalArgumentException("fieldIndex cannot be negative");
        }
        this.defaultTopicName = defaultTopicName;
    }

    @Override
    public String getTopic(Tuple tuple) {
        if (fieldIndex < tuple.size()) {
            return tuple.getString(fieldIndex);
        } else {
            LOG.warn("Field index {} is out of bounds. Using default topic {}", fieldIndex, defaultTopicName);
            return defaultTopicName;
        }
    }
}

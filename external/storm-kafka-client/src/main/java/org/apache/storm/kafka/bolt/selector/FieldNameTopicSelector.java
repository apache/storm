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
 * Uses field name to select topic name from tuple .
 */
public class FieldNameTopicSelector implements KafkaTopicSelector {
    private static final long serialVersionUID = -3903708904533396833L;
    private static final Logger LOG = LoggerFactory.getLogger(FieldNameTopicSelector.class);

    private final String fieldName;
    private final String defaultTopicName;


    public FieldNameTopicSelector(String fieldName, String defaultTopicName) {
        this.fieldName = fieldName;
        this.defaultTopicName = defaultTopicName;
    }

    @Override
    public String getTopic(Tuple tuple) {
        if (tuple.contains(fieldName)) {
            return tuple.getStringByField(fieldName);
        } else {
            LOG.warn("Field {} Not Found. Returning default topic {}", fieldName, defaultTopicName);
            return defaultTopicName;
        }
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.storm.kafka.spout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.tuple.Fields;

/**
 * Based off of a given Kafka topic a ConsumerRecord came from it will be translated to a Storm tuple
 * and emitted to a given stream.
 * @param <K> the key of the incoming Records
 * @param <V> the value of the incoming Records
 */
public class ByTopicRecordTranslator<K, V> implements RecordTranslator<K, V> {
    private static final long serialVersionUID = -121699733778988688L;
    private final RecordTranslator<K,V> defaultTranslator;
    private final Map<String, RecordTranslator<K,V>> topicToTranslator = new HashMap<>();
    private final Map<String, Fields> streamToFields = new HashMap<>();
    
    /**
     * Create a simple record translator that will use func to extract the fields of the tuple,
     * named by fields, and emit them to stream. This will handle all topics not explicitly set
     * elsewhere.
     * @param func extracts and turns them into a list of objects to be emitted
     * @param fields the names of the fields extracted
     * @param stream the stream to emit these fields on.
     */
    public ByTopicRecordTranslator(Func<ConsumerRecord<K, V>, List<Object>> func, Fields fields, String stream) {
        this(new SimpleRecordTranslator<>(func, fields, stream));
    }
    
    /**
     * Create a simple record translator that will use func to extract the fields of the tuple,
     * named by fields, and emit them to the default stream. This will handle all topics not explicitly set
     * elsewhere.
     * @param func extracts and turns them into a list of objects to be emitted
     * @param fields the names of the fields extracted
     */
    public ByTopicRecordTranslator(Func<ConsumerRecord<K, V>, List<Object>> func, Fields fields) {
        this(new SimpleRecordTranslator<>(func, fields));
    }
    
    /**
     * @param defaultTranslator a translator that will be used for all topics not explicitly set
     *     elsewhere.
     */
    public ByTopicRecordTranslator(RecordTranslator<K,V> defaultTranslator) {
        this.defaultTranslator = defaultTranslator;
        //This shouldn't throw on a Check, because nothing is configured yet
        cacheNCheckFields(defaultTranslator);
    }
    
    /**
     * Configure a translator for a given topic with tuples to be emitted to the default stream.
     * @param topic the topic this should be used for
     * @param func extracts and turns them into a list of objects to be emitted
     * @param fields the names of the fields extracted
     * @return this to be able to chain configuration
     * @throws IllegalStateException if the topic is already registered to another translator
     * @throws IllegalArgumentException if the Fields for the stream this emits to do not match
     *     any already configured Fields for the same stream
     */
    public ByTopicRecordTranslator<K, V> forTopic(String topic, Func<ConsumerRecord<K, V>, List<Object>> func, Fields fields) {
        return forTopic(topic, new SimpleRecordTranslator<>(func, fields));
    }
    
    /**
     * Configure a translator for a given topic.
     * @param topic the topic this should be used for
     * @param func extracts and turns them into a list of objects to be emitted
     * @param fields the names of the fields extracted
     * @param stream the stream to emit the tuples to.
     * @return this to be able to chain configuration
     * @throws IllegalStateException if the topic is already registered to another translator
     * @throws IllegalArgumentException if the Fields for the stream this emits to do not match
     *     any already configured Fields for the same stream
     */
    public ByTopicRecordTranslator<K, V> forTopic(String topic, Func<ConsumerRecord<K, V>,
        List<Object>> func, Fields fields, String stream) {
        return forTopic(topic, new SimpleRecordTranslator<>(func, fields, stream));
    }
    
    /**
     * Configure a translator for a given kafka topic.
     * @param topic the topic this translator should handle
     * @param translator the translator itself
     * @return this to be able to chain configuration
     * @throws IllegalStateException if the topic is already registered to another translator
     * @throws IllegalArgumentException if the Fields for the stream this emits to do not match
     *     any already configured Fields for the same stream
     */
    public ByTopicRecordTranslator<K, V> forTopic(String topic, RecordTranslator<K,V> translator) {
        if (topicToTranslator.containsKey(topic)) {
            throw new IllegalStateException("Topic " + topic + " is already registered");
        }
        cacheNCheckFields(translator);
        topicToTranslator.put(topic, translator);
        return this;
    }
    
    private void cacheNCheckFields(RecordTranslator<K, V> translator) {
        for (String stream : translator.streams()) {
            Fields fromTrans = translator.getFieldsFor(stream);
            Fields cached = streamToFields.get(stream);
            if (cached != null && !fromTrans.equals(cached)) {
                throw new IllegalArgumentException("Stream " + stream + " currently has Fields of " 
                    + cached + " which is not the same as those being added in " + fromTrans);
            }
            
            if (cached == null) {
                streamToFields.put(stream, fromTrans);
            }
        }
    }

    @Override
    public List<Object> apply(ConsumerRecord<K, V> record) {
        RecordTranslator<K, V> trans = topicToTranslator.getOrDefault(record.topic(), defaultTranslator);
        return trans.apply(record);
    }

    @Override
    public Fields getFieldsFor(String stream) {
        return streamToFields.get(stream);
    }
    
    @Override
    public List<String> streams() {
        return new ArrayList<>(streamToFields.keySet());
    }
}

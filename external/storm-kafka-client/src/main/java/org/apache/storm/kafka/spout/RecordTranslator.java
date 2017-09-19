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

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.kafka.spout.KafkaSpoutConfig.Builder;
import org.apache.storm.tuple.Fields;

/**
 * Translate a {@link org.apache.kafka.clients.consumer.ConsumerRecord} to a tuple.
 */
public interface RecordTranslator<K, V> extends Serializable, Func<ConsumerRecord<K, V>, List<Object>> {
    public static final List<String> DEFAULT_STREAM = Collections.singletonList("default");
    
    /**
     * Translate the ConsumerRecord into a list of objects that can be emitted.
     * @param record the record to translate
     * @return the objects in the tuple.  Return a {@link KafkaTuple}
     *     if you want to route the tuple to a non-default stream.
     *     Return null to discard an invalid {@link ConsumerRecord} if {@link Builder#setEmitNullTuples(boolean)} is set to true
     */
    List<Object> apply(ConsumerRecord<K,V> record);
    
    /**
     * Get the fields associated with a stream.  The streams passed in are
     * returned by the {@link RecordTranslator#streams() } method.
     * @param stream the stream the fields are for
     * @return the fields for that stream.
     */
    Fields getFieldsFor(String stream);
    
    /**
     * Get the list of streams this translator will handle.
     * @return the list of streams that this will handle.
     */
    default List<String> streams() {
        return DEFAULT_STREAM;
    }
}

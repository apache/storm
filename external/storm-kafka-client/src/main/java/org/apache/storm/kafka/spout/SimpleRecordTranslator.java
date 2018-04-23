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

import java.util.Arrays;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.tuple.Fields;

public class SimpleRecordTranslator<K, V> implements RecordTranslator<K, V> {
    private static final long serialVersionUID = 4678369144122009596L;
    private final Fields fields;
    private final Func<ConsumerRecord<K, V>, List<Object>> func;
    private final String stream;

    public SimpleRecordTranslator(Func<ConsumerRecord<K, V>, List<Object>> func, Fields fields) {
        this(func, fields, "default");
    }
    
    /**
     * Creates a SimpleRecordTranslator.
     * @param func The mapping function responsible for translating a Kafka record to a Tuple
     * @param fields The fields tuples constructed by this translator will contain
     * @param stream The stream tuples constructed by this translator will target
     */
    public SimpleRecordTranslator(Func<ConsumerRecord<K, V>, List<Object>> func, Fields fields, String stream) {
        this.func = func;
        this.fields = fields;
        this.stream = stream;
    }
    
    @Override
    public List<Object> apply(ConsumerRecord<K, V> record) {
        List<Object> vals = func.apply(record);
        if (vals == null) {
            return null;
        }
        KafkaTuple ret = new KafkaTuple();
        ret.addAll(vals);
        return ret.routedTo(stream);
    }

    @Override
    public Fields getFieldsFor(String stream) {
        return fields;
    }
    
    @Override
    public List<String> streams() {
        return Arrays.asList(stream);
    }
}

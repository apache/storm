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

import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class DefaultRecordTranslator<K, V> implements RecordTranslator<K, V> {
    private static final long serialVersionUID = -5782462870112305750L;
    public static final Fields FIELDS = new Fields("topic", "partition", "offset", "key", "value");
    
    @Override
    public List<Object> apply(ConsumerRecord<K, V> record) {
        return new Values(record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value());
    }

    @Override
    public Fields getFieldsFor(String stream) {
        return FIELDS;
    }
}

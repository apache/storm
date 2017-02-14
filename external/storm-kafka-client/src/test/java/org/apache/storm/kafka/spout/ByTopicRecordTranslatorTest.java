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

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.junit.Test;

public class ByTopicRecordTranslatorTest {
    public static Func<ConsumerRecord<String, String>, List<Object>> JUST_KEY_FUNC = new Func<ConsumerRecord<String, String>, List<Object>>() {
        @Override
        public List<Object> apply(ConsumerRecord<String, String> record) {
            return new Values(record.key());
        }
    };
    
    public static Func<ConsumerRecord<String, String>, List<Object>> JUST_VALUE_FUNC = new Func<ConsumerRecord<String, String>, List<Object>>() {
        @Override
        public List<Object> apply(ConsumerRecord<String, String> record) {
            return new Values(record.value());
        }
    };
    
    public static Func<ConsumerRecord<String, String>, List<Object>> KEY_VALUE_FUNC = new Func<ConsumerRecord<String, String>, List<Object>>() {
        @Override
        public List<Object> apply(ConsumerRecord<String, String> record) {
            return new Values(record.key(), record.value());
        }
    };
    
    @Test
    public void testBasic() {
        ByTopicRecordTranslator<String, String> trans = 
                new ByTopicRecordTranslator<>(JUST_KEY_FUNC, new Fields("key"));
        trans.forTopic("TOPIC 1", JUST_VALUE_FUNC, new Fields("value"), "value-stream");
        trans.forTopic("TOPIC 2", KEY_VALUE_FUNC, new Fields("key", "value"), "key-value-stream");
        HashSet<String> expectedStreams = new HashSet<>();
        expectedStreams.add("default");
        expectedStreams.add("value-stream");
        expectedStreams.add("key-value-stream");
        assertEquals(expectedStreams, new HashSet<>(trans.streams()));

        ConsumerRecord<String, String> cr1 = new ConsumerRecord<>("TOPIC OTHER", 100, 100, "THE KEY", "THE VALUE");
        assertEquals(new Fields("key"), trans.getFieldsFor("default"));
        assertEquals(Arrays.asList("THE KEY"), trans.apply(cr1));
        
        ConsumerRecord<String, String> cr2 = new ConsumerRecord<>("TOPIC 1", 100, 100, "THE KEY", "THE VALUE");
        assertEquals(new Fields("value"), trans.getFieldsFor("value-stream"));
        assertEquals(Arrays.asList("THE VALUE"), trans.apply(cr2));
        
        ConsumerRecord<String, String> cr3 = new ConsumerRecord<>("TOPIC 2", 100, 100, "THE KEY", "THE VALUE");
        assertEquals(new Fields("key", "value"), trans.getFieldsFor("key-value-stream"));
        assertEquals(Arrays.asList("THE KEY", "THE VALUE"), trans.apply(cr3));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testFieldCollision() {
        ByTopicRecordTranslator<String, String> trans = 
                new ByTopicRecordTranslator<>(JUST_KEY_FUNC, new Fields("key"));
        trans.forTopic("foo", JUST_VALUE_FUNC, new Fields("value"));
    }
    
    @Test(expected = IllegalStateException.class)
    public void testTopicCollision() {
        ByTopicRecordTranslator<String, String> trans = 
                new ByTopicRecordTranslator<>(JUST_KEY_FUNC, new Fields("key"));
        trans.forTopic("foo", JUST_VALUE_FUNC, new Fields("value"), "foo1");
        trans.forTopic("foo", KEY_VALUE_FUNC, new Fields("key", "value"), "foo2");
    }

}

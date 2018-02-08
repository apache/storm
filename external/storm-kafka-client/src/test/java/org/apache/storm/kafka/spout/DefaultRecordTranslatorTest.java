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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.tuple.Fields;
import org.junit.Test;

public class DefaultRecordTranslatorTest {
    @Test
    public void testBasic() {
        DefaultRecordTranslator<String, String> trans = new DefaultRecordTranslator<>();
        assertEquals(Arrays.asList("default"), trans.streams());
        assertEquals(new Fields("topic", "partition", "offset", "key", "value"), trans.getFieldsFor("default"));
        ConsumerRecord<String, String> cr = new ConsumerRecord<>("TOPIC", 100, 100, "THE KEY", "THE VALUE");
        assertEquals(Arrays.asList("TOPIC", 100, 100l, "THE KEY", "THE VALUE"), trans.apply(cr));
    }
}

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

import static org.junit.Assert.*;

import java.util.HashMap;

import org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy;
import org.junit.Test;

public class KafkaSpoutConfigTest {

    @Test
    public void testBasic() {
        KafkaSpoutConfig<String, String> conf = KafkaSpoutConfig.builder("localhost:1234", "topic").build();
        assertEquals(FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST, conf.getFirstPollOffsetStrategy());
        assertNull(conf.getConsumerGroupId());
        assertTrue(conf.getTranslator() instanceof DefaultRecordTranslator);
        HashMap<String, Object> expected = new HashMap<>();
        expected.put("bootstrap.servers", "localhost:1234");
        expected.put("enable.auto.commit", "false");
        assertEquals(expected, conf.getKafkaProps());
    }
}

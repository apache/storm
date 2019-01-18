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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class KafkaSpoutConfigTest {
    
    @Test
    public void testBasic() {
        KafkaSpoutConfig<String, String> conf = KafkaSpoutConfig.builder("localhost:1234", "topic").build();
        assertEquals(conf.getFirstPollOffsetStrategy(), FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST);
        assertNull(conf.getConsumerGroupId());
        assertTrue(conf.getTranslator() instanceof DefaultRecordTranslator);
        HashMap<String, Object> expected = new HashMap<>();
        expected.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234");
        expected.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        expected.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        expected.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        expected.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        assertEquals(conf.getKafkaProps(), expected);
        assertEquals(conf.getMetricsTimeBucketSizeInSecs(), KafkaSpoutConfig.DEFAULT_METRICS_TIME_BUCKET_SIZE_SECONDS);
    }

    @Test
    public void testSetEmitNullTuplesToTrue() {
        final KafkaSpoutConfig<String, String> conf = KafkaSpoutConfig.builder("localhost:1234", "topic")
                .setEmitNullTuples(true)
                .build();

        assertTrue(conf.isEmitNullTuples(), "Failed to set emit null tuples to true");
    }
    
    @Test
    public void testShouldNotChangeAutoOffsetResetPolicyWhenNotUsingAtLeastOnce() {
        KafkaSpoutConfig<String, String> conf = KafkaSpoutConfig.builder("localhost:1234", "topic")
            .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_MOST_ONCE)
            .build();
        
        assertThat("When at-least-once is not specified, the spout should use the Kafka default auto offset reset policy",
            conf.getKafkaProps().get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), nullValue());
    }
    
    @Test
    public void testWillRespectExplicitAutoOffsetResetPolicy() {
        KafkaSpoutConfig<String, String> conf = KafkaSpoutConfig.builder("localhost:1234", "topic")
            .setProp(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none")
            .build();
        
        assertThat("Should allow users to pick a different auto offset reset policy than the one recommended for the at-least-once processing guarantee",
            conf.getKafkaProps().get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), is("none"));
    }

    @Test
    public void testMetricsTimeBucketSizeInSecs() {
        KafkaSpoutConfig<String, String> conf = KafkaSpoutConfig.builder("localhost:1234", "topic")
             .setMetricsTimeBucketSizeInSecs(100)
            .build();

        assertEquals(conf.getMetricsTimeBucketSizeInSecs(), 100);
    }
    
    @Test
    public void testThrowsIfEnableAutoCommitIsSet() {
        Assertions.assertThrows(IllegalStateException.class, () -> KafkaSpoutConfig.builder("localhost:1234", "topic")
            .setProp(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true)
            .build());
    }
}

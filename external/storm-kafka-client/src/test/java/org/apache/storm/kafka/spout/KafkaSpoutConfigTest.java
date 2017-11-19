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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
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
        expected.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234");
        expected.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        expected.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        expected.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        expected.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        assertEquals(expected, conf.getKafkaProps());
    }

    @Test
    public void testSetEmitNullTuplesToTrue() {
        final KafkaSpoutConfig<String, String> conf = KafkaSpoutConfig.builder("localhost:1234", "topic")
                .setEmitNullTuples(true)
                .build();

        assertTrue("Failed to set emit null tuples to true", conf.isEmitNullTuples());
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
            (String)conf.getKafkaProps().get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), is("none"));
    }
    
    @Test
    public void testCanConfigureWithExplicitTrueBooleanAutoCommitMode() {
        /*
         * Since adding setProcessingGuarantee to KafkaSpoutConfig we don't want users to set "enable.auto.commit" in the consumer config,
         * because setting the processing guarantee will do it automatically. For backward compatibility we need to be able to handle the 
         * property being set anyway for a few releases, and try to set a processing guarantee that corresponds to the property.
         */
        
        KafkaSpoutConfig<String, String> conf = KafkaSpoutConfig.builder("localhost:1234", "topic")
            .setProp(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true)
            .build();
        
        assertThat("When setting enable auto commit to true explicitly the spout should use the 'none' processing guarantee",
            conf.getProcessingGuarantee(), is(KafkaSpoutConfig.ProcessingGuarantee.NONE));
    }
    
    @Test
    public void testCanConfigureWithExplicitFalseBooleanAutoCommitMode() {
        KafkaSpoutConfig<String, String> conf = KafkaSpoutConfig.builder("localhost:1234", "topic")
            .setProp(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
            .build();
        
        assertThat("When setting enable auto commit to false explicitly the spout should use the 'at-least-once' processing guarantee",
            conf.getProcessingGuarantee(), is(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE));
    }
    
    @Test
    public void testCanConfigureWithExplicitTrueStringAutoCommitMode() {
        KafkaSpoutConfig<String, String> conf = KafkaSpoutConfig.builder("localhost:1234", "topic")
            .setProp(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
            .build();
        
        assertThat("When setting enable auto commit to true explicitly the spout should use the 'none' processing guarantee",
            conf.getProcessingGuarantee(), is(KafkaSpoutConfig.ProcessingGuarantee.NONE));
    }
    
    @Test
    public void testCanConfigureWithExplicitFalseStringAutoCommitMode() {
        KafkaSpoutConfig<String, String> conf = KafkaSpoutConfig.builder("localhost:1234", "topic")
            .setProp(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
            .build();
        
        assertThat("When setting enable auto commit explicitly to false the spout should use the 'at-least-once' processing guarantee",
            conf.getProcessingGuarantee(), is(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE));
    }
}

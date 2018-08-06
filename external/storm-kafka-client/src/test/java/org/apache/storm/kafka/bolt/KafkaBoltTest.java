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
package org.apache.storm.kafka.bolt;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.storm.Testing;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.MkTupleParam;
import org.apache.storm.tuple.Tuple;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaBoltTest {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaBoltTest.class);

    private <K, V> KafkaBolt<K, V> makeBolt(Producer<K, V> producer) {
        KafkaBolt<K, V> bolt = new KafkaBolt<K, V>() {
            @Override
            protected Producer<K, V> mkProducer(Properties props) {
                return producer;
            }
        };
        bolt.withTopicSelector("MY_TOPIC");

        return bolt;
    }

    private Tuple createTestTuple(String... values) {
        MkTupleParam param = new MkTupleParam();
        param.setFields("key", "message");
        return Testing.testTuple(Arrays.asList(values), param);
    }

    @Test
    public void testSimple() {
        MockProducer<String, String> producer = new MockProducer<>(Cluster.empty(), false, null, null, null);
        KafkaBolt<String, String> bolt = makeBolt(producer);

        OutputCollector collector = mock(OutputCollector.class);
        TopologyContext context = mock(TopologyContext.class);
        Map<String, Object> conf = new HashMap<>();
        bolt.prepare(conf, context, collector);

        String key = "KEY";
        String value = "VALUE";
        Tuple testTuple = createTestTuple(key, value);
        bolt.execute(testTuple);

        assertThat(producer.history().size(), is(1));
        ProducerRecord<String, String> arg = producer.history().get(0);

        LOG.info("GOT {} ->", arg);
        LOG.info("{}, {}, {}", arg.topic(), arg.key(), arg.value());
        assertThat(arg.topic(), is("MY_TOPIC"));
        assertThat(arg.key(), is(key));
        assertThat(arg.value(), is(value));

        // Complete the send
        producer.completeNext();
        verify(collector).ack(testTuple);
    }

    @Test
    public void testSimpleWithError() {
        MockProducer<String, String> producer = new MockProducer<>(Cluster.empty(), false, null, null, null);
        KafkaBolt<String, String> bolt = makeBolt(producer);

        OutputCollector collector = mock(OutputCollector.class);
        TopologyContext context = mock(TopologyContext.class);
        Map<String, Object> conf = new HashMap<>();
        bolt.prepare(conf, context, collector);

        String key = "KEY";
        String value = "VALUE";
        Tuple testTuple = createTestTuple(key, value);
        bolt.execute(testTuple);

        assertThat(producer.history().size(), is(1));
        ProducerRecord<String, String> arg = producer.history().get(0);

        LOG.info("GOT {} ->", arg);
        LOG.info("{}, {}, {}", arg.topic(), arg.key(), arg.value());
        assertThat(arg.topic(), is("MY_TOPIC"));
        assertThat(arg.key(), is(key));
        assertThat(arg.value(), is(value));

        // Force a send error
        KafkaException ex = new KafkaException();
        producer.errorNext(ex);
        verify(collector).reportError(ex);
        verify(collector).fail(testTuple);
    }

    @Test
    public void testCustomCallbackIsWrappedByDefaultCallbackBehavior() {
        MockProducer<String, String> producer = new MockProducer<>(Cluster.empty(), false, null, null, null);
        KafkaBolt<String, String> bolt = makeBolt(producer);

        PreparableCallback customCallback = mock(PreparableCallback.class);
        bolt.withProducerCallback(customCallback);

        OutputCollector collector = mock(OutputCollector.class);
        TopologyContext context = mock(TopologyContext.class);
        Map<String, Object> topoConfig = new HashMap<>();
        bolt.prepare(topoConfig, context, collector);

        verify(customCallback).prepare(topoConfig, context);

        String key = "KEY";
        String value = "VALUE";
        Tuple testTuple = createTestTuple(key, value);
        bolt.execute(testTuple);

        assertThat(producer.history().size(), is(1));
        ProducerRecord<String, String> arg = producer.history().get(0);

        LOG.info("GOT {} ->", arg);
        LOG.info("{}, {}, {}", arg.topic(), arg.key(), arg.value());
        assertThat(arg.topic(), is("MY_TOPIC"));
        assertThat(arg.key(), is(key));
        assertThat(arg.value(), is(value));

        // Force a send error
        KafkaException ex = new KafkaException();
        producer.errorNext(ex);
        verify(customCallback).onCompletion(any(), eq(ex));
        verify(collector).reportError(ex);
        verify(collector).fail(testTuple);
    }
}

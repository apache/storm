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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.Testing;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.MkTupleParam;
import org.apache.storm.tuple.Tuple;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaBoltTest {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaBoltTest.class);
    
    @SuppressWarnings({ "unchecked", "serial" })
    @Test
    public void testSimple() {
        final KafkaProducer<String, String> producer = mock(KafkaProducer.class);
        when(producer.send(any(), any())).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Callback c = (Callback)invocation.getArguments()[1];
                c.onCompletion(null, null);
                return null;
            }
        });
        KafkaBolt<String, String> bolt = new KafkaBolt<String, String>() {
            @Override
            protected KafkaProducer<String, String> mkProducer(Properties props) {
                return producer;
            }
        };
        bolt.withTopicSelector("MY_TOPIC");
        
        OutputCollector collector = mock(OutputCollector.class);
        TopologyContext context = mock(TopologyContext.class);
        Map<String, Object> conf = new HashMap<>();
        bolt.prepare(conf, context, collector);
        MkTupleParam param = new MkTupleParam();
        param.setFields("key", "message");
        Tuple testTuple = Testing.testTuple(Arrays.asList("KEY", "VALUE"), param);
        bolt.execute(testTuple);
        verify(producer).send(argThat(new ArgumentMatcher<ProducerRecord<String, String>>() {
            @Override
            public boolean matches(Object argument) {
                LOG.info("GOT {} ->", argument);
                ProducerRecord<String, String> arg = (ProducerRecord<String, String>) argument;
                LOG.info("  {} {} {}", arg.topic(), arg.key(), arg.value());
                return "MY_TOPIC".equals(arg.topic()) &&
                        "KEY".equals(arg.key()) &&
                        "VALUE".equals(arg.value());
            }
        }), any(Callback.class));
        verify(collector).ack(testTuple);
    }

}

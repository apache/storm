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
package org.apache.storm.kafka;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collections;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper.BOLT_KEY;
import static org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper.BOLT_MESSAGE;

public class StringKeyValueSchemeTest {

    private StringKeyValueScheme scheme = new StringKeyValueScheme();

    @Test
    public void testDeserialize() throws Exception {
        assertEquals(Collections.singletonList("test"), scheme.deserialize(wrapString("test")));
    }

    @Test
    public void testGetOutputFields() throws Exception {
        assertEquals(asList(BOLT_KEY, BOLT_MESSAGE), scheme.getOutputFields().toList());
    }

    @Test
    public void testDeserializeWithNullKeyAndValue() throws Exception {
        assertEquals(Collections.singletonList("test"),
            scheme.deserializeKeyAndValue(null, wrapString("test")));
    }

    @Test
    public void testDeserializeWithKeyAndValue() throws Exception {
        assertEquals(Collections.singletonList(ImmutableMap.of("key", "test")),
                scheme.deserializeKeyAndValue(wrapString("key"), wrapString("test")));
    }

    private static ByteBuffer wrapString(String s) {
        return ByteBuffer.wrap(s.getBytes(Charset.defaultCharset()));
    }
}

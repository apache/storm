/*
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

package org.apache.storm.serialization;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.testing.TestSerObject;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

// FIXME: it should be moved to storm-client when serialization-test.clj can be removed
public class SerializationTest {

    @Test
    public void testJavaSerialization() throws IOException {
        Object obj = new TestSerObject(1, 2);
        List<Object> vals = Lists.newArrayList(obj);

        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_KRYO_REGISTER, new HashMap<String, String>() {{
            put("org.apache.storm.testing.TestSerObject", null);
        }});
        conf.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION, false);
        assertThrows(Exception.class, () -> roundtrip(vals, conf));

        conf.clear();
        conf.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION, true);
        assertEquals(vals, roundtrip(vals, conf));
    }

    @Test
    public void testKryoDecorator() throws IOException {
        Object obj = new TestSerObject(1, 2);
        List<Object> vals = Lists.newArrayList(obj);

        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION, false);
        assertThrows(Exception.class, () -> roundtrip(vals, conf),
            "Expected Exception not Thrown for config: " + conf);

        conf.put(Config.TOPOLOGY_KRYO_DECORATORS, Lists.newArrayList("org.apache.storm.testing.TestKryoDecorator"));
        assertEquals(vals, roundtrip(vals, conf));
    }

    @Test
    public void testStringSerialization() throws IOException {
        isRoundtrip(Lists.newArrayList("a", "bb", "cbe"));
        isRoundtrip(Lists.newArrayList(mkString(64 * 1024)));
        isRoundtrip(Lists.newArrayList(mkString(1024 * 1024)));
        isRoundtrip(Lists.newArrayList(mkString(1024 * 1024 * 2)));
    }

    private Map<String, Object> mkConf(Map<String, Object> extra) {
        Map<String, Object> config = Utils.readDefaultConfig();
        config.putAll(extra);
        return config;
    }

    private byte[] serialize(List<Object> vals, Map<String, Object> conf) {
        KryoValuesSerializer serializer = new KryoValuesSerializer(mkConf(conf));
        return serializer.serialize(vals);
    }

    private List<Object> deserialize(byte[] bytes, Map<String, Object> conf) {
        KryoValuesDeserializer deserializer = new KryoValuesDeserializer(mkConf(conf));
        return deserializer.deserialize(bytes);
    }

    private List<Object> roundtrip(List<Object> vals) throws IOException {
        return roundtrip(vals, new HashMap<>());
    }

    private List<Object> roundtrip(List<Object> vals, Map<String, Object> conf) {
        return deserialize(serialize(vals, conf), conf);
    }

    private String mkString(int size) {
        StringBuilder sb = new StringBuilder();
        while (size-- > 0) {
            sb.append("a");
        }
        return sb.toString();
    }

    public void isRoundtrip(List vals) throws IOException {
        assertEquals(vals, roundtrip(vals));
    }
}

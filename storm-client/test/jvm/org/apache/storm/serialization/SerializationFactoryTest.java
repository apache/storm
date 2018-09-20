/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.serialization;

import com.esotericsoftware.kryo.Kryo;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.security.serialization.BlowfishTupleSerializer;
import org.apache.storm.utils.ListDelegate;
import org.apache.storm.utils.Utils;
import org.junit.Assert;
import org.junit.Test;

public class SerializationFactoryTest {

    @Test
    public void test_registers_default_when_not_in_conf() throws ClassNotFoundException {
        Map<String, Object> conf = Utils.readDefaultConfig();
        String className = (String) conf.get(Config.TOPOLOGY_TUPLE_SERIALIZER);
        Class configuredClass = Class.forName(className);
        Kryo kryo = SerializationFactory.getKryo(conf);
        Assert.assertEquals(configuredClass, kryo.getSerializer(ListDelegate.class).getClass());
    }

    @Test(expected = RuntimeException.class)
    public void test_throws_runtimeexception_when_no_such_class() {
        Map<String, Object> conf = Utils.readDefaultConfig();
        conf.put(Config.TOPOLOGY_TUPLE_SERIALIZER, "null.this.class.does.not.exist");
        SerializationFactory.getKryo(conf);
    }

    @Test
    public void test_registers_when_valid_class_name() {
        Class arbitraryClass = BlowfishTupleSerializer.class;
        String secretKey = "0123456789abcdef";
        Map<String, Object> conf = Utils.readDefaultConfig();
        conf.put(Config.TOPOLOGY_TUPLE_SERIALIZER, arbitraryClass.getName());
        conf.put(BlowfishTupleSerializer.SECRET_KEY, secretKey);
        Kryo kryo = SerializationFactory.getKryo(conf);
        Assert.assertEquals(arbitraryClass, kryo.getSerializer(ListDelegate.class).getClass());

    }

}

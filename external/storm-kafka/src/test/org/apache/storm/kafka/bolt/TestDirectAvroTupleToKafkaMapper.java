/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.kafka.bolt;

import java.io.IOException;
import java.util.HashMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.storm.Config;
import org.apache.storm.avro.DefaultDirectAvroSerializer;
import org.apache.storm.kafka.bolt.mapper.DirectAvroTupleToKafkaMapper;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.junit.Assert;
import org.junit.Test;

public class TestDirectAvroTupleToKafkaMapper {
    private static final String schemaString1 = "{\"type\":\"record\"," +
            "\"name\":\"stormtest1\"," +
            "\"fields\":[{\"name\":\"foo1\",\"type\":\"string\"}," +
            "{ \"name\":\"int1\", \"type\":\"int\" }]}";

    @Test
    public void testMapper() {
        GenericRecord record = new GenericData.Record(new Schema.Parser().parse(schemaString1));
        record.put("foo1", "xin");
        record.put("int1", 2016);
        String key = "my_key";

        DirectAvroTupleToKafkaMapper mapper = new DirectAvroTupleToKafkaMapper("bolt-key", "bolt-msg");
        Tuple tuple = generateTestTuple(key, record);

        Object _key = mapper.getKeyFromTuple(tuple);
        byte[] _message = mapper.getMessageFromTuple(tuple);

        byte[] bytes = null;
        try {
            bytes = new DefaultDirectAvroSerializer().serialize(record);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Assert.assertEquals(key, _key);
        Assert.assertArrayEquals(bytes, _message);
    }

    private Tuple generateTestTuple(Object key, Object msg) {
        TopologyBuilder builder = new TopologyBuilder();
        GeneralTopologyContext topologyContext = new GeneralTopologyContext(builder.createTopology(),
                new Config(), new HashMap(), new HashMap(), new HashMap(), "") {
            @Override
            public Fields getComponentOutputFields(String componentId, String streamId) {
                return new Fields("bolt-key", "bolt-msg");
            }
        };
        return new TupleImpl(topologyContext, new Values(key, msg), 1, "");
    }
}

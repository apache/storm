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

package org.apache.storm.tuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.serialization.KryoValuesDeserializer;
import org.apache.storm.serialization.KryoValuesSerializer;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link DetachedTuple}.
 */
public class DetachedTupleTest {

    private Tuple sourceTuple;
    private DetachedTuple detached;

    private GeneralTopologyContext getContext(final Fields fields) {
        TopologyBuilder builder = new TopologyBuilder();
        return new GeneralTopologyContext(
            builder.createTopology(), new Config(), new HashMap<>(), new HashMap<>(), new HashMap<>(), "") {

            @Override
            public Fields getComponentOutputFields(String componentId, String streamId) {
                return fields;
            }

        };
    }

    @BeforeEach
    public void setUp() {
        Fields fields = new Fields("id", "ts");
        sourceTuple = new TupleImpl(getContext(fields), new Values(42, 1000L), "srcComponent", 7, "srcStream");
        detached = new DetachedTuple(sourceTuple);
    }

    @Test
    public void testSnapshotsSourceMetadata() {
        assertEquals("srcComponent", detached.getSourceComponent());
        assertEquals(7, detached.getSourceTask());
        assertEquals("srcStream", detached.getSourceStreamId());
        assertEquals(new GlobalStreamId("srcComponent", "srcStream"), detached.getSourceGlobalStreamId());
    }

    @Test
    public void testSnapshotsFieldsAndValues() {
        assertEquals(2, detached.size());
        assertEquals(Arrays.asList("id", "ts"), detached.getFields().toList());
        assertEquals(sourceTuple.getValues(), detached.getValues());
        assertTrue(detached.contains("id"));
        assertEquals(1, detached.fieldIndex("ts"));
        assertEquals(42, detached.getValue(0));
        assertEquals(Integer.valueOf(42), detached.getIntegerByField("id"));
        assertEquals(Long.valueOf(1000L), detached.getLongByField("ts"));
        assertEquals(Arrays.asList(1000L), detached.select(new Fields("ts")));
    }

    @Test
    public void testIsUnanchoredAndDetachedFromContext() {
        assertEquals(MessageId.makeUnanchored(), detached.getMessageId());
        assertThrows(UnsupportedOperationException.class, () -> detached.getContext());
    }

    @Test
    public void testValueBasedEquality() {
        DetachedTuple other = new DetachedTuple(sourceTuple);
        assertEquals(detached, other);
        assertEquals(detached.hashCode(), other.hashCode());

        Tuple differentTuple = new TupleImpl(getContext(new Fields("id", "ts")), new Values(43, 1000L), "srcComponent", 7, "srcStream");
        assertNotEquals(detached, new DetachedTuple(differentTuple));
    }

    /**
     * STORM-4000 regression: a tuple emitted on the late tuple stream must survive Kryo serialization. With
     * {@link Config#TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION} disabled, serializing a {@link TupleImpl} (with its
     * attached topology context) fails, while a {@link DetachedTuple} round-trips cleanly.
     */
    @Test
    public void testKryoRoundTripWithoutJavaFallback() {
        Map<String, Object> conf = Utils.readDefaultConfig();
        conf.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION, false);

        byte[] serialized = new KryoValuesSerializer(conf).serialize(new Values(detached));
        List<Object> roundTripped = new KryoValuesDeserializer(conf).deserialize(serialized);

        assertInstanceOf(DetachedTuple.class, roundTripped.get(0));
        DetachedTuple deserialized = (DetachedTuple) roundTripped.get(0);
        assertEquals(detached, deserialized);
        // Fields are rebuilt lazily after deserialization
        assertEquals(Arrays.asList("id", "ts"), deserialized.getFields().toList());
        assertEquals(Long.valueOf(1000L), deserialized.getLongByField("ts"));
    }
}

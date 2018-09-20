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

package org.apache.storm.state;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.storm.Config;
import org.apache.storm.serialization.KryoTupleDeserializer;
import org.apache.storm.serialization.KryoTupleSerializer;
import org.apache.storm.serialization.SerializationFactory;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.TupleImpl;
import org.objenesis.strategy.StdInstantiatorStrategy;

/**
 * A default implementation that uses Kryo to serialize and de-serialize the state.
 */
public class DefaultStateSerializer<T> implements Serializer<T> {
    private final TopologyContext context;
    private final Map<String, Object> topoConf;
    private final List<String> registrations = new ArrayList<>();

    private final ThreadLocal<Kryo> kryo = new ThreadLocal<Kryo>() {
        @Override
        protected Kryo initialValue() {
            Kryo obj = new Kryo();
            if (context != null && topoConf != null) {
                KryoTupleSerializer ser = new KryoTupleSerializer(topoConf, context);
                KryoTupleDeserializer deser = new KryoTupleDeserializer(topoConf, context);
                obj.register(TupleImpl.class, new TupleSerializer(ser, deser));
            }
            if (!registrations.isEmpty()) {
                SerializationFactory.register(obj, registrations);
            }
            obj.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
            return obj;
        }
    };

    private final ThreadLocal<Output> output = new ThreadLocal<Output>() {
        @Override
        protected Output initialValue() {
            return new Output(2000, 2000000000);
        }
    };

    /**
     * Constructs a {@link DefaultStateSerializer} instance with the given list of classes registered in kryo.
     *
     * @param classesToRegister the classes to register.
     */
    public DefaultStateSerializer(Map<String, Object> topoConf, TopologyContext context, List<Class<?>> classesToRegister) {
        this.context = context;
        this.topoConf = topoConf;
        registrations.addAll(classesToRegister.stream().map(Class::getName).collect(Collectors.toSet()));
        // other classes from config
        registrations.addAll((List<String>) topoConf.getOrDefault(Config.TOPOLOGY_STATE_KRYO_REGISTER, Collections.emptyList()));
        // defaults
        registrations.add(Optional.class.getName());
    }

    public DefaultStateSerializer(Map<String, Object> topoConf, TopologyContext context) {
        this(topoConf, context, Collections.emptyList());
    }

    public DefaultStateSerializer() {
        this(Collections.emptyMap(), null);
    }

    @Override
    public byte[] serialize(T obj) {
        output.get().clear();
        kryo.get().writeClassAndObject(output.get(), obj);
        return output.get().toBytes();
    }

    @Override
    public T deserialize(byte[] b) {
        Input input = new Input(b);
        return (T) kryo.get().readClassAndObject(input);
    }

    private static class TupleSerializer extends com.esotericsoftware.kryo.Serializer<TupleImpl> {
        private final KryoTupleSerializer tupleSerializer;
        private final KryoTupleDeserializer tupleDeserializer;

        TupleSerializer(KryoTupleSerializer tupleSerializer, KryoTupleDeserializer tupleDeserializer) {
            this.tupleSerializer = tupleSerializer;
            this.tupleDeserializer = tupleDeserializer;
        }

        @Override
        public void write(Kryo kryo, Output output, TupleImpl tuple) {
            byte[] bytes = tupleSerializer.serialize(tuple);
            output.writeInt(bytes.length);
            output.write(bytes);
        }

        @Override
        public TupleImpl read(Kryo kryo, Input input, Class<TupleImpl> type) {
            int length = input.readInt();
            byte[] bytes = input.readBytes(length);
            return tupleDeserializer.deserialize(bytes);
        }
    }
}

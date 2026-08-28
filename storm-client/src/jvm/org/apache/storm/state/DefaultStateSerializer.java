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
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;
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
import org.apache.storm.spout.CheckPointState;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.utils.ObjectReader;
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
            // Same as new Kryo(), except for the class resolver: the no-arg constructor is
            // new Kryo(new DefaultClassResolver(), null).
            Kryo obj = new Kryo(new StateClassResolver(), null);
            // Registration bounds the set of classes this serializer will construct from stored
            // bytes to the ones the topology declared. It is keyed off the same config as the tuple
            // path in DefaultKryoFactory, so a topology that has opted into permissive Kryo globally
            // keeps the previous behaviour.
            // Note this Kryo is independent of the one SerializationFactory builds for tuples: the
            // two id spaces must not be merged, since the tuple ids are a wire format between workers.
            boolean fallBackOnJavaSerialization = ObjectReader.getBoolean(
                topoConf == null ? null : topoConf.get(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION), false);
            obj.setRegistrationRequired(!fallBackOnJavaSerialization);
            if (context != null && topoConf != null) {
                KryoTupleSerializer ser = new KryoTupleSerializer(topoConf, context);
                KryoTupleDeserializer deser = new KryoTupleDeserializer(topoConf, context);
                obj.register(TupleImpl.class, new TupleSerializer(ser, deser));
            }
            if (!registrations.isEmpty()) {
                SerializationFactory.register(obj, registrations);
            }
            registerInternalClasses(obj);
            obj.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
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
        output.get().reset();
        kryo.get().writeClassAndObject(output.get(), obj);
        return output.get().toBytes();
    }

    @Override
    public T deserialize(byte[] b) {
        Input input = new Input(b);
        return (T) kryo.get().readClassAndObject(input);
    }

    /**
     * Registers the types Storm's own state encoding and checkpointing persist without the component
     * declaring them.
     *
     * <p>Called after the configured registrations so that the ids assigned to those are unaffected.
     * {@link Kryo#register(Class)} returns any existing registration, so a class already declared
     * through {@link Config#TOPOLOGY_STATE_KRYO_REGISTER} keeps the id assigned there.
     *
     * <p>Deliberately limited to types Storm itself writes without registering them elsewhere. The
     * windowing types reachable from a persisted {@code WindowState.WindowPartition} are registered
     * by {@code PersistentWindowedBoltExecutor} through {@link Config#TOPOLOGY_STATE_KRYO_REGISTER}
     * and must not be added here: that list includes JDK types in {@code java.base} whose eager
     * {@code FieldSerializer} construction needs reflective access the module system denies unless
     * the worker is started with a matching {@code --add-opens}.
     */
    private static void registerInternalClasses(Kryo kryo) {
        // DefaultStateEncoder wraps every value as Optional<byte[]>.
        kryo.register(byte[].class);
        // CheckpointSpout's own state, which it stores without registering.
        kryo.register(CheckPointState.class);
        kryo.register(CheckPointState.State.class);
    }

    /**
     * A class resolver that always clears its name caches on reset.
     *
     * <p>{@link DefaultClassResolver#reset()} returns immediately when registration is required, on
     * the assumption that name encoding cannot occur in that mode. That assumption does not hold
     * when reading back state: bytes written by an earlier release carry name-encoded classes, and
     * Kryo skips the class name in the stream for a name id it believes it has already seen. Left
     * uncleared, the second such read on a Kryo instance desynchronises from the stream and fails
     * with a buffer underflow rather than reading the value.
     */
    private static class StateClassResolver extends DefaultClassResolver {
        @Override
        public void reset() {
            super.reset();
            if (classToNameId != null) {
                classToNameId.clear(2048);
            }
            if (nameIdToClass != null) {
                nameIdToClass.clear();
            }
            nextNameId = 0;
        }
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
        public TupleImpl read(Kryo kryo, Input input, Class<? extends TupleImpl> type) {
            int length = input.readInt();
            byte[] bytes = input.readBytes(length);
            return tupleDeserializer.deserialize(bytes);
        }
    }
}

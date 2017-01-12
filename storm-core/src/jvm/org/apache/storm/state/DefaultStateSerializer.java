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
package org.apache.storm.state;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import org.objenesis.strategy.StdInstantiatorStrategy;

import java.util.Collections;
import java.util.List;

/**
 * A default implementation that uses Kryo to serialize and de-serialize
 * the state.
 */
public class DefaultStateSerializer<T> implements Serializer<T> {
    private final ThreadLocal<Kryo> kryo = new ThreadLocal<Kryo>() {
        @Override
        protected Kryo initialValue() {
            Kryo obj = new Kryo();
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
     * Constructs a {@link DefaultStateSerializer} instance with the given list
     * of classes registered in kryo.
     *
     * @param classesToRegister the classes to register.
     */
    public DefaultStateSerializer(List<Class<?>> classesToRegister) {
        for (Class<?> klazz : classesToRegister) {
            kryo.get().register(klazz);
        }
    }

    public DefaultStateSerializer() {
        this(Collections.<Class<?>>emptyList());
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
}

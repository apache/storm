/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.kinesis.spout;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Utility class for serialization and deserialization.
 */
class SerializationHelper {
    private static final Logger LOG = LoggerFactory.getLogger(SerializationHelper.class);

    // Utility class should not be instantiated.
    private SerializationHelper() { }

    /**
     * If the ByteBuffer is backed by an array, return this array. Otherwise, dump the ByteBuffer
     * to a byte array (the latter is an expensive operation).
     * @param buf  buffer to read from.
     * @return data or copy of data in buf as a byte array.
     */
    public static byte[] copyData(ByteBuffer buf) {
        if (buf.hasArray()) {
            return buf.array();
        } else {
            LOG.trace("ByteBuffer is not backed by byte[], copying.");

            byte[] data = new byte[buf.remaining()];
            buf.get(data);
            return data;
        }
    }

    public static byte[] kryoSerializeObject(final Object obj) {
        final Kryo kryo = new Kryo();
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        final Output output = new Output(os);

        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        kryo.writeClassAndObject(output, obj);

        output.flush();
        return os.toByteArray();
    }

    public static Object kryoDeserializeObject(final byte[] ser) {
        final Kryo kryo = new Kryo();
        final Input input = new Input(new ByteArrayInputStream(ser));

        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        return kryo.readClassAndObject(input);
    }
}

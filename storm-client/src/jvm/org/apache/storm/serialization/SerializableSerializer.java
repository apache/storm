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
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;


public class SerializableSerializer extends Serializer<Object> {

    /**
     * Optional JEP-290 filter applied to each ObjectInputStream used for deserialization (null means unfiltered,
     * as before). The filter itself is created once from
     * {@link org.apache.storm.Config#TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION_FILTER} by {@link DefaultKryoFactory};
     * instances returned by {@link ObjectInputFilter.Config#createFilter} are immutable and safe to share across streams.
     */
    private final ObjectInputFilter serialFilter;

    public SerializableSerializer() {
        this(null);
    }

    public SerializableSerializer(ObjectInputFilter serialFilter) {
        this.serialFilter = serialFilter;
    }

    @Override
    public void write(Kryo kryo, Output output, Object object) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(object);
            oos.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        byte[] ser = bos.toByteArray();
        output.writeInt(ser.length);
        output.writeBytes(ser);
    }

    @Override
    public Object read(Kryo kryo, Input input, Class c) {
        int len = input.readInt();
        if (len < 0) {
            throw new KryoException("Invalid java-serialized value length: " + len);
        }
        // For a buffer-backed Input the remaining bytes are known (position/limit), so a declared length larger than the
        // bytes actually left is refused before the new byte[len] allocation; a stream-backed Input may still deliver the
        // declared bytes later, so the upper bound is not checked there.
        if (input.getInputStream() == null) {
            int remaining = input.limit() - input.position();
            if (len > remaining) {
                throw new KryoException("Declared java-serialized value length exceeds the input's remaining bytes "
                        + "(declared: " + len + ", remaining: " + remaining + ")");
            }
        }
        byte[] ser = new byte[len];
        input.readBytes(ser);
        ByteArrayInputStream bis = new ByteArrayInputStream(ser);
        try {
            ObjectInputStream ois = new ObjectInputStream(bis);
            if (serialFilter != null) {
                ois.setObjectInputFilter(mergeWithExisting(serialFilter, ois.getObjectInputFilter()));
            }
            return ois.readObject();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Combines the configured filter with the stream's existing filter (a JVM-wide {@code jdk.serialFilter}, if any), so both
     * the configured pattern and any process-wide filter apply to the stream: per JEP-290,
     * {@link ObjectInputStream#setObjectInputFilter} overrides the process-wide filter for that stream unless the two are merged.
     */
    static ObjectInputFilter mergeWithExisting(ObjectInputFilter configured, ObjectInputFilter existing) {
        return existing != null ? ObjectInputFilter.merge(configured, existing) : configured;
    }
}

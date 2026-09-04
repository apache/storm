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
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.util.Util;
import java.io.ObjectInputFilter;
import java.util.Map;
import org.apache.storm.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DefaultKryoFactory implements IKryoFactory {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultKryoFactory.class);

    @Override
    public Kryo getKryo(Map<String, Object> conf) {
        KryoSerializableDefault k = new KryoSerializableDefault();
        k.setRegistrationRequired(!((Boolean) conf.get(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION)));
        k.setReferences(false);
        k.setJavaSerializationFilter(getJavaSerializationFilter(conf));
        return k;
    }

    /**
     * Parses the pattern once at kryo construction so an invalid pattern fails worker setup with a clear error
     * instead of failing per-tuple on the read path. Returns null when the key is unset or empty (no filter).
     */
    private static ObjectInputFilter getJavaSerializationFilter(Map<String, Object> conf) {
        String filterSpec = (String) conf.get(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION_FILTER);
        if (filterSpec == null || filterSpec.isEmpty()) {
            return null;
        }
        try {
            return ObjectInputFilter.Config.createFilter(filterSpec);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Invalid " + Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION_FILTER
                    + " pattern: \"" + filterSpec + "\"", e);
        }
    }

    @Override
    public void preRegister(Kryo k, Map<String, Object> conf) {
    }

    @Override
    public void postRegister(Kryo k, Map<String, Object> conf) {
        ((KryoSerializableDefault) k).overrideDefault(true);
    }

    @Override
    public void postDecorate(Kryo k, Map<String, Object> conf) {
    }

    public static class KryoSerializableDefault extends Kryo {
        boolean override = false;
        private ObjectInputFilter javaSerializationFilter;

        public void setJavaSerializationFilter(ObjectInputFilter filter) {
            this.javaSerializationFilter = filter;
        }

        public void overrideDefault(boolean value) {
            override = value;
        }

        @Override
        public Serializer getDefaultSerializer(Class type) {
            if (override) {
                LOG.warn("Class is not registered: {}\n"
                        + "Note: To register this class use: kryo.register({});\n"
                        + "Falling back to java serialization.",
                        Util.className(type), Util.className(type)
                );

                return new SerializableSerializer(javaSerializationFilter);
            } else {
                return super.getDefaultSerializer(type);
            }
        }
    }
}

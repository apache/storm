/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.trident.windowing;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import org.apache.storm.shade.com.google.common.base.Preconditions;

/**
 * Store for storing window related entities like windowed tuples, triggers etc. {@link WindowKryoSerializer} can be used for kryo
 * serialization/deserialization of keys and values.
 */
public interface WindowsStore extends Serializable {

    /**
     * This can be used as a separator while generating a key from sequence of strings.
     */
    String KEY_SEPARATOR = "|";

    Object get(String key);

    Iterable<Object> get(List<String> keys);

    Iterable<String> getAllKeys();

    void put(String key, Object value);

    void putAll(Collection<Entry> entries);

    void remove(String key);

    void removeAll(Collection<String> keys);

    void shutdown();

    /**
     * This class wraps key and value objects which can be passed to {@code putAll} method.
     */
    class Entry implements Serializable {
        public final String key;
        public final Object value;

        public Entry(String key, Object value) {
            nonNullCheckForKey(key);
            nonNullCheckForValue(value);
            this.key = key;
            this.value = value;
        }

        public static void nonNullCheckForKey(Object key) {
            Preconditions.checkArgument(key != null, "key argument can not be null");
        }

        public static void nonNullCheckForValue(Object value) {
            Preconditions.checkArgument(value != null, "value argument can not be null");
        }

    }

}

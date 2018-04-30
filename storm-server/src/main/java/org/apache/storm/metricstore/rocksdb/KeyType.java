/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.metricstore.rocksdb;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Specifies all the valid types of keys and their values.
 */
public enum KeyType {
    TOPOLOGY_BLOB(0), // reserved for saving topology data
    METADATA_STRING_START(1),
    TOPOLOGY_STRING(1),
    METRIC_STRING(2),
    COMPONENT_STRING(3),
    EXEC_ID_STRING(4),
    HOST_STRING(5),
    STREAM_ID_STRING(6),
    METADATA_STRING_END(7),
    METRIC_DATA(0x80);

    private static Map<Byte, KeyType> MAP;

    static {
        MAP = new HashMap<>();
        for (KeyType type : EnumSet.allOf(KeyType.class)) {
            MAP.put(type.getValue(), type);
        }
        MAP = Collections.unmodifiableMap(MAP);
    }

    private final byte value;

    KeyType(int value) {
        this.value = (byte) value;
    }

    static KeyType getKeyType(byte value) {
        KeyType type = MAP.get(value);
        if (type == null) {
            throw new RuntimeException("Invalid key type " + value);
        } else {
            return type;
        }
    }

    byte getValue() {
        return this.value;
    }
}



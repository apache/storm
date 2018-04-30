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

package org.apache.storm.command;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Utils;

/**
 * Read a value from the topology config map.
 */
public final class ConfigValue {

    /**
     * Utility classes should not have a public constructor.
     */
    private ConfigValue() {
    }

    /**
     * Read the topology config and return the value for the given key.
     * @param args - an array of length 1 containing the key to fetch.
     */
    public static void main(final String[] args) {
        String name = args[0];
        Map<String, Object> conf = Utils.readStormConfig();
        Object value = conf.get(name);
        if (value instanceof List) {
            List<String> stringValues = ((List<?>) value)
                .stream()
                .map(ObjectReader::getString)
                .collect(Collectors.toList());
            value = String.join(" ", stringValues);
        }
        System.out.println("VALUE: " + value);
    }
}

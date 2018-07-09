/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.storm.utils;

import java.util.EnumMap;
import java.util.function.Function;

public class EnumUtil {
    public static String toMetricName(Enum type) {
        return type.name().toLowerCase().replace('_', '-');
    }

    /**
     * Create an Enum map with given lambda mapper.
     * @param klass the Enum class
     * @param mapper The mapper producing value with key (enum constant)
     * @param <T> An Enum class
     * @param <U> Mapped class
     * @return An Enum map
     */
    public static <T extends Enum<T>, U> EnumMap<T, U> toEnumMap(Class<T> klass, Function<? super T, ? extends U> mapper) {
        EnumMap<T, U> map = new EnumMap<>(klass);
        for (T elem : klass.getEnumConstants()) {
            map.put(elem, mapper.apply(elem));
        }
        return map;
    }
}

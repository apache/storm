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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.sql.runtime.trident;

public class NumberConverter {
    public static Number convert(Number num, Class<? extends Number> toType) {
        // no need to convert
        if (num.getClass().equals(toType)) {
            return num;
        }

        if (toType.equals(Byte.class)) {
            return num.byteValue();
        } else if (toType.equals(Short.class)) {
            return num.shortValue();
        } else if (toType.equals(Integer.class)) {
            return num.intValue();
        } else if (toType.equals(Long.class)) {
            return num.longValue();
        } else if (toType.equals(Float.class)) {
            return num.floatValue();
        } else if (toType.equals(Double.class)) {
            return num.doubleValue();
        } else if (toType.isAssignableFrom(num.getClass())) {
            // isAssignable is true so safe to return
            return num;
        } else {
            throw new IllegalArgumentException("Can't convert Number " + num + " (class " + num.getClass() + " to " + toType);
        }
    }
}

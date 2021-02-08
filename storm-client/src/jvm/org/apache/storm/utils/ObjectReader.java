/*
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

package org.apache.storm.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ObjectReader {

    /**
     * Convert the input into a list of string; ignore null members.
     * @param o the input object
     * @return a list of string
     */
    public static List<String> getStrings(final Object o) {
        if (o == null) {
            return new ArrayList<>();
        } else if (o instanceof String) {
            return new ArrayList<String>() {
                {
                    add((String) o);
                }
            };
        } else if (o instanceof Collection) {
            List<String> answer = new ArrayList<>();
            for (Object v : (Collection) o) {
                if (v != null) {
                    answer.add(v.toString());
                }
            }
            return answer;
        } else {
            throw new IllegalArgumentException("Don't know how to convert to string list");
        }
    }

    public static String getString(Object o) {
        if (null == o) {
            throw new IllegalArgumentException("Don't know how to convert null to String");
        }
        return o.toString();
    }

    public static String getString(Object o, String defaultValue) {
        if (null == o) {
            return defaultValue;
        }
        if (o instanceof String) {
            return (String) o;
        } else {
            throw new IllegalArgumentException("Don't know how to convert " + o + " to String");
        }
    }

    public static Integer getInt(Object o) {
        Integer result = getInt(o, null);
        if (null == result) {
            throw new IllegalArgumentException("Don't know how to convert null to int");
        }
        return result;
    }

    public static Integer getInt(Object o, Integer defaultValue) {
        if (null == o) {
            return defaultValue;
        }

        if (o instanceof Integer
                || o instanceof Short
                || o instanceof Byte) {
            return ((Number) o).intValue();
        } else if (o instanceof Long) {
            final long l = (Long) o;
            if (l <= Integer.MAX_VALUE && l >= Integer.MIN_VALUE) {
                return (int) l;
            }
        } else if (o instanceof String) {
            return Integer.parseInt((String) o);
        }

        throw new IllegalArgumentException("Don't know how to convert " + o + " to int");
    }

    public static Long getLong(Object o) {
        return getLong(o, null);
    }

    public static Long getLong(Object o, Long defaultValue) {
        if (null == o) {
            return defaultValue;
        }
        if (o instanceof Number) {
            return ((Number) o).longValue();
        } else if (o instanceof String) {
            return Long.valueOf((String) o);
        }
        throw new IllegalArgumentException("Don't know how to convert " + o + " to a long");
    }

    public static Double getDouble(Object o) {
        Double result = getDouble(o, null);
        if (null == result) {
            throw new IllegalArgumentException("Don't know how to convert null to double");
        }
        return result;
    }

    public static Double getDouble(Object o, Double defaultValue) {
        if (null == o) {
            return defaultValue;
        }
        if (o instanceof Number) {
            return ((Number) o).doubleValue();
        } else {
            throw new IllegalArgumentException("Don't know how to convert (" + o + ") to double");
        }
    }

    public static boolean getBoolean(Object o, boolean defaultValue) {
        if (null == o) {
            return defaultValue;
        }
        if (o instanceof Boolean) {
            return (Boolean) o;
        } else {
            throw new IllegalArgumentException("Don't know how to convert " + o + " to boolean");
        }
    }
}

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
package org.apache.storm.sql.compiler.backends.standalone;

import com.google.common.collect.ImmutableList;
import org.apache.storm.tuple.Values;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Built-in implementations for some of the standard aggregation operations.
 * Aggregations can be implemented as a class with the following methods viz. init, add and result.
 * The class could contain only static methods, only non-static methods or be generic.
 */
public class BuiltinAggregateFunctions {
    // binds the type information and the class implementing the aggregation
    public static class TypeClass {
        public static class GenericType {
        }

        public final Type ty;
        public final Class<?> clazz;

        private TypeClass(Type ty, Class<?> clazz) {
            this.ty = ty;
            this.clazz = clazz;
        }

        static TypeClass of(Type ty, Class<?> clazz) {
            return new TypeClass(ty, clazz);
        }
    }

    static final Map<String, List<TypeClass>> TABLE = new HashMap<>();

    public static class ByteSum {
        public static Byte init() {
            return 0;
        }

        public static Byte add(Byte accumulator, Byte val) {
            return (byte) (accumulator + val);
        }

        public static Byte result(Byte accumulator) {
            return accumulator;
        }
    }

    public static class ShortSum {
        public static Short init() {
            return 0;
        }

        public static Short add(Short accumulator, Short val) {
            return (short) (accumulator + val);
        }

        public static Short result(Short accumulator) {
            return accumulator;
        }
    }

    public static class IntSum {
        public static Integer init() {
            return 0;
        }

        public static Integer add(Integer accumulator, Integer val) {
            return accumulator + val;
        }

        public static Integer result(Integer accumulator) {
            return accumulator;
        }
    }

    public static class LongSum {
        public static Long init() {
            return 0L;
        }

        public static Long add(Long accumulator, Long val) {
            return accumulator + val;
        }

        public static Long result(Long accumulator) {
            return accumulator;
        }
    }

    public static class FloatSum {
        public static Float init() {
            return 0.0f;
        }

        public static Float add(Float accumulator, Float val) {
            return accumulator + val;
        }

        public static Float result(Float accumulator) {
            return accumulator;
        }
    }

    public static class DoubleSum {
        public static Double init() {
            return 0.0;
        }

        public static Double add(Double accumulator, Double val) {
            return accumulator + val;
        }

        public static Double result(Double accumulator) {
            return accumulator;
        }
    }

    public static class Max<T extends Comparable<T>> {
        public T init() {
            return null;
        }

        public T add(T accumulator, T val) {
            return (accumulator == null || accumulator.compareTo(val) < 0) ? val : accumulator;
        }

        public T result(T accumulator) {
            return accumulator;
        }
    }

    public static class Min<T extends Comparable<T>> {
        public T init() {
            return null;
        }

        public T add(T accumulator, T val) {
            return (accumulator == null || accumulator.compareTo(val) > 0) ? val : accumulator;
        }

        public T result(T accumulator) {
            return accumulator;
        }
    }

    public static class IntAvg {
        private int count;

        public Integer init() {
            return 0;
        }

        public Integer add(Integer accumulator, Integer val) {
            ++count;
            return accumulator + val;
        }

        public Integer result(Integer accumulator) {
            Integer result = accumulator / count;
            count = 0;
            return result;
        }
    }

    public static class DoubleAvg {
        private int count;

        public Double init() {
            return 0.0;
        }

        public Double add(Double accumulator, Double val) {
            ++count;
            return accumulator + val;
        }

        public Double result(Double accumulator) {
            Double result = accumulator / count;
            count = 0;
            return result;
        }
    }

    public static class Count {
        public static Long init() {
            return 0L;
        }

        public static Long add(Long accumulator, Values vals) {
            for (Object val : vals) {
                if (val == null) {
                    return accumulator;
                }
            }
            return accumulator + 1;
        }

        public static Long result(Long accumulator) {
            return accumulator;
        }
    }

    static {
        TABLE.put("SUM", ImmutableList.of(
                TypeClass.of(float.class, FloatSum.class),
                TypeClass.of(double.class, DoubleSum.class),
                TypeClass.of(byte.class, ByteSum.class),
                TypeClass.of(short.class, ShortSum.class),
                TypeClass.of(long.class, LongSum.class),
                TypeClass.of(int.class, IntSum.class)));
        TABLE.put("AVG", ImmutableList.of(
                TypeClass.of(double.class, DoubleAvg.class),
                TypeClass.of(int.class, IntAvg.class)));
        TABLE.put("COUNT", ImmutableList.of(TypeClass.of(long.class, Count.class)));
        TABLE.put("MAX", ImmutableList.of(TypeClass.of(TypeClass.GenericType.class, Max.class)));
        TABLE.put("MIN", ImmutableList.of(TypeClass.of(TypeClass.GenericType.class, Min.class)));
    }
}

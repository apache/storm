/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.streams.operations.mappers;

import org.apache.storm.streams.tuple.Tuple10;
import org.apache.storm.streams.tuple.Tuple3;
import org.apache.storm.streams.tuple.Tuple4;
import org.apache.storm.streams.tuple.Tuple5;
import org.apache.storm.streams.tuple.Tuple6;
import org.apache.storm.streams.tuple.Tuple7;
import org.apache.storm.streams.tuple.Tuple8;
import org.apache.storm.streams.tuple.Tuple9;
import org.apache.storm.tuple.Tuple;

/**
 * Factory for constructing typed tuples from a {@link Tuple} based on indicies.
 */
@SuppressWarnings("unchecked")
public final class TupleValueMappers {
    private TupleValueMappers() {
    }

    public static <T1, T2, T3> TupleValueMapper<Tuple3<T1, T2, T3>> of(int index1,
                                            int index2,
                                            int index3) {
        return input -> new Tuple3<>(
            (T1) input.getValue(index1),
            (T2) input.getValue(index2),
            (T3) input.getValue(index3));
    }

    public static <T1, T2, T3, T4> TupleValueMapper<Tuple4<T1, T2, T3, T4>> of(int index1,
                                                int index2,
                                                int index3,
                                                int index4) {
        return input -> new Tuple4<>(
            (T1) input.getValue(index1),
            (T2) input.getValue(index2),
            (T3) input.getValue(index3),
            (T4) input.getValue(index4));
    }

    public static <T1, T2, T3, T4, T5> TupleValueMapper<Tuple5<T1, T2, T3, T4, T5>> of(int index1,
                                                    int index2,
                                                    int index3,
                                                    int index4,
                                                    int index5) {
        return input -> new Tuple5<>(
            (T1) input.getValue(index1),
            (T2) input.getValue(index2),
            (T3) input.getValue(index3),
            (T4) input.getValue(index4),
            (T5) input.getValue(index5));
    }

    public static <T1, T2, T3, T4, T5, T6> TupleValueMapper<Tuple6<T1, T2, T3, T4, T5, T6>> of(int index1,
                                                        int index2,
                                                        int index3,
                                                        int index4,
                                                        int index5,
                                                        int index6) {
        return input -> new Tuple6<>(
            (T1) input.getValue(index1),
            (T2) input.getValue(index2),
            (T3) input.getValue(index3),
            (T4) input.getValue(index4),
            (T5) input.getValue(index5),
            (T6) input.getValue(index6));
    }

    public static <T1, T2, T3, T4, T5, T6, T7> TupleValueMapper<Tuple7<T1, T2, T3, T4, T5, T6, T7>> of(int index1,
                                                            int index2,
                                                            int index3,
                                                            int index4,
                                                            int index5,
                                                            int index6,
                                                            int index7) {
        return input -> new Tuple7<>(
            (T1) input.getValue(index1),
            (T2) input.getValue(index2),
            (T3) input.getValue(index3),
            (T4) input.getValue(index4),
            (T5) input.getValue(index5),
            (T6) input.getValue(index6),
            (T7) input.getValue(index7));
    }

    public static <T1, T2, T3, T4, T5, T6, T7, T8> TupleValueMapper<Tuple8<T1, T2, T3, T4, T5, T6, T7, T8>> of(int index1,
                                                                int index2,
                                                                int index3,
                                                                int index4,
                                                                int index5,
                                                                int index6,
                                                                int index7,
                                                                int index8) {
        return input -> new Tuple8<>(
            (T1) input.getValue(index1),
            (T2) input.getValue(index2),
            (T3) input.getValue(index3),
            (T4) input.getValue(index4),
            (T5) input.getValue(index5),
            (T6) input.getValue(index6),
            (T7) input.getValue(index7),
            (T8) input.getValue(index8));
    }

    public static <T1, T2, T3, T4, T5, T6, T7, T8, T9> TupleValueMapper<Tuple9<T1, T2, T3, T4, T5, T6, T7, T8, T9>> of(int index1,
                                                                    int index2,
                                                                    int index3,
                                                                    int index4,
                                                                    int index5,
                                                                    int index6,
                                                                    int index7,
                                                                    int index8,
                                                                    int index9) {
        return input -> new Tuple9<>(
            (T1) input.getValue(index1),
            (T2) input.getValue(index2),
            (T3) input.getValue(index3),
            (T4) input.getValue(index4),
            (T5) input.getValue(index5),
            (T6) input.getValue(index6),
            (T7) input.getValue(index7),
            (T8) input.getValue(index8),
            (T9) input.getValue(index9));
    }

    @SuppressWarnings("checkstyle:MethodTypeParameterName")
    public static <T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>
        TupleValueMapper<Tuple10<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> of(int index1,
                                                                          int index2,
                                                                          int index3,
                                                                          int index4,
                                                                          int index5,
                                                                          int index6,
                                                                          int index7,
                                                                          int index8,
                                                                          int index9,
                                                                          int index10) {
        return input -> new Tuple10<>(
            (T1) input.getValue(index1),
            (T2) input.getValue(index2),
            (T3) input.getValue(index3),
            (T4) input.getValue(index4),
            (T5) input.getValue(index5),
            (T6) input.getValue(index6),
            (T7) input.getValue(index7),
            (T8) input.getValue(index8),
            (T9) input.getValue(index9),
            (T10) input.getValue(index10));
    }
}

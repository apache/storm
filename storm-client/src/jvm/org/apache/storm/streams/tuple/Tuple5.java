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

package org.apache.storm.streams.tuple;

/**
 * A tuple of five elements along the lines of Scala's Tuple.
 *
 * @param <T1> the type of the first element
 * @param <T2> the type of the second element
 * @param <T3> the type of the third element
 * @param <T4> the type of the fourth element
 * @param <T5> the type of the fifth element
 */
public class Tuple5<T1, T2, T3, T4, T5> {
    public final T1 value1;
    public final T2 value2;
    public final T3 value3;
    public final T4 value4;
    public final T5 value5;

    /**
     * Constructs a new tuple.
     *
     * @param value1 the first element
     * @param value2 the second element
     * @param value3 the third element
     * @param value4 the fourth element
     * @param value5 the fifth element
     */
    public Tuple5(T1 value1, T2 value2, T3 value3, T4 value4, T5 value5) {
        this.value1 = value1;
        this.value2 = value2;
        this.value3 = value3;
        this.value4 = value4;
        this.value5 = value5;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Tuple5<?, ?, ?, ?, ?> tuple5 = (Tuple5<?, ?, ?, ?, ?>) o;

        if (value1 != null ? !value1.equals(tuple5.value1) : tuple5.value1 != null) {
            return false;
        }
        if (value2 != null ? !value2.equals(tuple5.value2) : tuple5.value2 != null) {
            return false;
        }
        if (value3 != null ? !value3.equals(tuple5.value3) : tuple5.value3 != null) {
            return false;
        }
        if (value4 != null ? !value4.equals(tuple5.value4) : tuple5.value4 != null) {
            return false;
        }
        return value5 != null ? value5.equals(tuple5.value5) : tuple5.value5 == null;

    }

    @Override
    public int hashCode() {
        int result = value1 != null ? value1.hashCode() : 0;
        result = 31 * result + (value2 != null ? value2.hashCode() : 0);
        result = 31 * result + (value3 != null ? value3.hashCode() : 0);
        result = 31 * result + (value4 != null ? value4.hashCode() : 0);
        result = 31 * result + (value5 != null ? value5.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "(" + value1 + "," + value2 + "," + value3 + "," + value4 + "," + value5 + ")";
    }
}

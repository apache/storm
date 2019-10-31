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
 * A tuple of three elements along the lines of Scala's Tuple.
 *
 * @param <T1> the type of the first element
 * @param <T2> the type of the second element
 * @param <T3> the type of the third element
 */
public final class Tuple3<T1, T2, T3> {
    public final T1 value1;
    public final T2 value2;
    public final T3 value3;

    /**
     * Constructs a new tuple.
     *
     * @param value1 the first element
     * @param value2 the second element
     * @param value3 the third element
     */
    public Tuple3(T1 value1, T2 value2, T3 value3) {
        this.value1 = value1;
        this.value2 = value2;
        this.value3 = value3;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Tuple3<?, ?, ?> tuple3 = (Tuple3<?, ?, ?>) o;

        if (value1 != null ? !value1.equals(tuple3.value1) : tuple3.value1 != null) {
            return false;
        }
        if (value2 != null ? !value2.equals(tuple3.value2) : tuple3.value2 != null) {
            return false;
        }
        return value3 != null ? value3.equals(tuple3.value3) : tuple3.value3 == null;

    }

    @Override
    public int hashCode() {
        int result = value1 != null ? value1.hashCode() : 0;
        result = 31 * result + (value2 != null ? value2.hashCode() : 0);
        result = 31 * result + (value3 != null ? value3.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "(" + value1 + "," + value2 + "," + value3 + ")";
    }
}

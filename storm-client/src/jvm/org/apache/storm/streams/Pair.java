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
package org.apache.storm.streams;

import java.io.Serializable;

/**
 * A pair of values.
 *
 * @param <T1> the type of the first value
 * @param <T2> the type of the second value
 */
public final class Pair<T1, T2> implements Serializable {
    /**
     * The first value
     */
    public final T1 _1;
    /**
     * The second value
     */
    public final T2 _2;

    /**
     * Constructs a new pair of values
     *
     * @param first  the first value
     * @param second the second value
     */
    private Pair(T1 first, T2 second) {
        _1 = first;
        _2 = second;
    }

    /**
     * Returns the first value in a pair.
     *
     * @return the first value
     */
    public T1 getFirst() {
        return _1;
    }

    /**
     * Returns the second value in a pair.
     *
     * @return the second value
     */
    public T2 getSecond() {
        return _2;
    }

    /**
     * Constructs a new pair of values.
     *
     * @param first  the first value
     * @param second the second value
     * @param <T1>   the type of the first value
     * @param <T2>   the type of the second value
     * @return a new pair of values
     */
    public static <T1, T2> Pair<T1, T2> of(T1 first, T2 second) {
        return new Pair<>(first, second);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Pair<?, ?> pair = (Pair<?, ?>) o;

        if (_1 != null ? !_1.equals(pair._1) : pair._1 != null) return false;
        return _2 != null ? _2.equals(pair._2) : pair._2 == null;

    }

    @Override
    public int hashCode() {
        int result = _1 != null ? _1.hashCode() : 0;
        result = 31 * result + (_2 != null ? _2.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "(" + _1 + ", " + _2 + ')';
    }
}

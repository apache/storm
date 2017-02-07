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
package org.apache.storm.streams.tuple;

/**
 * A tuple of ten elements along the lines of Scala's Tuple.
 *
 * @param <T1>  the type of the first element
 * @param <T2>  the type of the second element
 * @param <T3>  the type of the third element
 * @param <T4>  the type of the fourth element
 * @param <T5>  the type of the fifth element
 * @param <T6>  the type of the sixth element
 * @param <T7>  the type of the seventh element
 * @param <T8>  the type of the eighth element
 * @param <T9>  the type of the ninth element
 * @param <T10> the type of the tenth element
 */
public class Tuple10<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> {
    public final T1 _1;
    public final T2 _2;
    public final T3 _3;
    public final T4 _4;
    public final T5 _5;
    public final T6 _6;
    public final T7 _7;
    public final T8 _8;
    public final T9 _9;
    public final T10 _10;

    /**
     * Constructs a new tuple.
     *
     * @param _1  the first element
     * @param _2  the second element
     * @param _3  the third element
     * @param _4  the fourth element
     * @param _5  the fifth element
     * @param _6  the sixth element
     * @param _7  the seventh element
     * @param _8  the eighth element
     * @param _9  the ninth element
     * @param _10 the tenth element
     */
    public Tuple10(T1 _1, T2 _2, T3 _3, T4 _4, T5 _5, T6 _6, T7 _7, T8 _8, T9 _9, T10 _10) {
        this._1 = _1;
        this._2 = _2;
        this._3 = _3;
        this._4 = _4;
        this._5 = _5;
        this._6 = _6;
        this._7 = _7;
        this._8 = _8;
        this._9 = _9;
        this._10 = _10;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tuple10<?, ?, ?, ?, ?, ?, ?, ?, ?, ?> tuple10 = (Tuple10<?, ?, ?, ?, ?, ?, ?, ?, ?, ?>) o;

        if (_1 != null ? !_1.equals(tuple10._1) : tuple10._1 != null) return false;
        if (_2 != null ? !_2.equals(tuple10._2) : tuple10._2 != null) return false;
        if (_3 != null ? !_3.equals(tuple10._3) : tuple10._3 != null) return false;
        if (_4 != null ? !_4.equals(tuple10._4) : tuple10._4 != null) return false;
        if (_5 != null ? !_5.equals(tuple10._5) : tuple10._5 != null) return false;
        if (_6 != null ? !_6.equals(tuple10._6) : tuple10._6 != null) return false;
        if (_7 != null ? !_7.equals(tuple10._7) : tuple10._7 != null) return false;
        if (_8 != null ? !_8.equals(tuple10._8) : tuple10._8 != null) return false;
        if (_9 != null ? !_9.equals(tuple10._9) : tuple10._9 != null) return false;
        return _10 != null ? _10.equals(tuple10._10) : tuple10._10 == null;

    }

    @Override
    public int hashCode() {
        int result = _1 != null ? _1.hashCode() : 0;
        result = 31 * result + (_2 != null ? _2.hashCode() : 0);
        result = 31 * result + (_3 != null ? _3.hashCode() : 0);
        result = 31 * result + (_4 != null ? _4.hashCode() : 0);
        result = 31 * result + (_5 != null ? _5.hashCode() : 0);
        result = 31 * result + (_6 != null ? _6.hashCode() : 0);
        result = 31 * result + (_7 != null ? _7.hashCode() : 0);
        result = 31 * result + (_8 != null ? _8.hashCode() : 0);
        result = 31 * result + (_9 != null ? _9.hashCode() : 0);
        result = 31 * result + (_10 != null ? _10.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "(" + _1 + "," + _2 + "," + _3 + "," + _4 + "," + _5 + "," + _6 + "," + _7 + "," + _8 + "," + _9 + "," + _10 + ")";
    }
}

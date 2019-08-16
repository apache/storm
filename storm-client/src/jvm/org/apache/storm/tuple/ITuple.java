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

package org.apache.storm.tuple;

import java.util.List;

public interface ITuple {

    /**
     * Returns the number of fields in this tuple.
     */
    int size();

    /**
     * Returns true if this tuple contains the specified name of the field.
     */
    boolean contains(String field);

    /**
     * Gets the names of the fields in this tuple.
     */
    Fields getFields();

    /**
     * Returns the position of the specified field in this tuple.
     *
     * @throws IllegalArgumentException - if field does not exist
     */
    int fieldIndex(String field);

    /**
     * Returns a subset of the tuple based on the fields selector.
     */
    List<Object> select(Fields selector);

    /**
     * Gets the field at position i in the tuple. Returns object since tuples are dynamically typed.
     *
     * @throws IndexOutOfBoundsException - if the index is out of range `(index < 0 || index >= size())`
     */
    Object getValue(int i);

    /**
     * Returns the String at position i in the tuple.
     *
     * @throws ClassCastException        If that field is not a String
     * @throws IndexOutOfBoundsException - if the index is out of range `(index < 0 || index >= size())`
     */
    String getString(int i);

    /**
     * Returns the Integer at position i in the tuple.
     *
     * @throws ClassCastException        If that field is not a Integer
     * @throws IndexOutOfBoundsException - if the index is out of range `(index < 0 || index >= size())`
     */
    Integer getInteger(int i);

    /**
     * Returns the Long at position i in the tuple.
     *
     * @throws ClassCastException        If that field is not a Long
     * @throws IndexOutOfBoundsException - if the index is out of range `(index < 0 || index >= size())`
     */
    Long getLong(int i);

    /**
     * Returns the Boolean at position i in the tuple.
     *
     * @throws ClassCastException        If that field is not a Boolean
     * @throws IndexOutOfBoundsException - if the index is out of range `(index < 0 || index >= size())`
     */
    Boolean getBoolean(int i);

    /**
     * Returns the Short at position i in the tuple.
     *
     * @throws ClassCastException        If that field is not a Short
     * @throws IndexOutOfBoundsException - if the index is out of range `(index < 0 || index >= size())`
     */
    Short getShort(int i);

    /**
     * Returns the Byte at position i in the tuple.
     *
     * @throws ClassCastException        If that field is not a Byte
     * @throws IndexOutOfBoundsException - if the index is out of range `(index < 0 || index >= size())`
     */
    Byte getByte(int i);

    /**
     * Returns the Double at position i in the tuple.
     *
     * @throws ClassCastException        If that field is not a Double
     * @throws IndexOutOfBoundsException - if the index is out of range `(index < 0 || index >= size())`
     */
    Double getDouble(int i);

    /**
     * Returns the Float at position i in the tuple.
     *
     * @throws ClassCastException        If that field is not a Float
     * @throws IndexOutOfBoundsException - if the index is out of range `(index < 0 || index >= size())`
     */
    Float getFloat(int i);

    /**
     * Returns the byte array at position i in the tuple.
     *
     * @throws ClassCastException        If that field is not a byte array
     * @throws IndexOutOfBoundsException - if the index is out of range `(index < 0 || index >= size())`
     */
    byte[] getBinary(int i);

    /**
     * Gets the field with a specific name. Returns object since tuples are dynamically typed.
     *
     * @throws IllegalArgumentException - if field does not exist
     */
    Object getValueByField(String field);

    /**
     * Gets the String field with a specific name.
     *
     * @throws ClassCastException       If that field is not a String
     * @throws IllegalArgumentException - if field does not exist
     */
    String getStringByField(String field);

    /**
     * Gets the Integer field with a specific name.
     *
     * @throws ClassCastException       If that field is not an Integer
     * @throws IllegalArgumentException - if field does not exist
     */
    Integer getIntegerByField(String field);

    /**
     * Gets the Long field with a specific name.
     *
     * @throws ClassCastException       If that field is not a Long
     * @throws IllegalArgumentException - if field does not exist
     */
    Long getLongByField(String field);

    /**
     * Gets the Boolean field with a specific name.
     *
     * @throws ClassCastException       If that field is not a Boolean
     * @throws IllegalArgumentException - if field does not exist
     */
    Boolean getBooleanByField(String field);

    /**
     * Gets the Short field with a specific name.
     *
     * @throws ClassCastException       If that field is not a Short
     * @throws IllegalArgumentException - if field does not exist
     */
    Short getShortByField(String field);

    /**
     * Gets the Byte field with a specific name.
     *
     * @throws ClassCastException       If that field is not a Byte
     * @throws IllegalArgumentException - if field does not exist
     */
    Byte getByteByField(String field);

    /**
     * Gets the Double field with a specific name.
     *
     * @throws ClassCastException       If that field is not a Double
     * @throws IllegalArgumentException - if field does not exist
     */
    Double getDoubleByField(String field);

    /**
     * Gets the Float field with a specific name.
     *
     * @throws ClassCastException       If that field is not a Float
     * @throws IllegalArgumentException - if field does not exist
     */
    Float getFloatByField(String field);

    /**
     * Gets the Byte array field with a specific name.
     *
     * @throws ClassCastException       If that field is not a byte array
     * @throws IllegalArgumentException - if field does not exist
     */
    byte[] getBinaryByField(String field);

    /**
     * Gets all the values in this tuple.
     */
    List<Object> getValues();

}

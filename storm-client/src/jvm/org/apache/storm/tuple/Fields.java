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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Collection of unique named fields using in an ITuple.
 */
public class Fields implements Iterable<String>, Serializable {
    private static final long serialVersionUID = -3377931843059975424L;
    private List<String> fields;
    private Map<String, Integer> index = new HashMap<>();

    public Fields(String... fields) {
        this(Arrays.asList(fields));
    }

    public Fields(List<String> fields) {
        this.fields = new ArrayList<>(fields.size());
        for (String field : fields) {
            if (this.fields.contains(field)) {
                throw new IllegalArgumentException(
                    String.format("duplicate field '%s'", field)
                );
            }
            this.fields.add(field);
        }
        index();
    }

    /**
     * Select values out of tuple given a Fields selector Note that this function can throw a NullPointerException if the fields in selector
     * are not found in the index.
     *
     * @param selector Fields to select
     * @param tuple    tuple to select from
     */
    public List<Object> select(Fields selector, List<Object> tuple) {
        List<Object> ret = new ArrayList<>(selector.size());
        for (String s : selector) {
            ret.add(tuple.get(fieldIndex(s)));
        }
        return ret;
    }

    public List<String> toList() {
        return new ArrayList<>(fields);
    }

    /**
     * Returns the number of fields in this collection.
     */
    public int size() {
        return fields.size();
    }

    /**
     * Gets the field at position index in the collection.
     *
     * @param index index of the field to return
     * @throws IndexOutOfBoundsException - if the index is out of range (index < 0 || index >= size())
     */
    public String get(int index) {
        return fields.get(index);
    }

    @Override
    public Iterator<String> iterator() {
        return fields.iterator();
    }

    /**
     * Returns the position of the specified named field.
     *
     * @param field Named field to evaluate
     * @throws IllegalArgumentException - if field does not exist
     */
    public int fieldIndex(String field) {
        Integer ret = index.get(field);
        if (ret == null) {
            throw new IllegalArgumentException(field + " does not exist");
        }
        return ret;
    }

    /**
     * Check contains.
     * @return true if this contains the specified name of the field.
     */
    public boolean contains(String field) {
        return index.containsKey(field);
    }

    private void index() {
        for (int i = 0; i < fields.size(); i++) {
            index.put(fields.get(i), i);
        }
    }

    @Override
    public String toString() {
        return fields.toString();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other instanceof Fields) {
            Fields of = (Fields) other;
            return fields.equals(of.fields);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return fields.hashCode();
    }
}

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

package org.apache.storm.trident.tuple;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

/**
 * Extends AbstractList so that it can be emitted directly as Storm tuples.
 */
public class TridentTupleView extends AbstractList<Object> implements TridentTuple {
    public static final TridentTupleView EMPTY_TUPLE = new TridentTupleView(null, new ValuePointer[0], new HashMap());
    private final ValuePointer[] index;
    private final Map<String, ValuePointer> fieldIndex;
    private final List<List<Object>> delegates;

    // index and fieldIndex are precomputed, delegates built up over many operations using persistent data structures
    public TridentTupleView(List delegates, ValuePointer[] index, Map<String, ValuePointer> fieldIndex) {
        this.delegates = delegates;
        this.index = index;
        this.fieldIndex = fieldIndex;
    }

    private static List<String> indexToFieldsList(ValuePointer[] index) {
        List<String> ret = new ArrayList<>();
        for (ValuePointer p : index) {
            ret.add(p.field);
        }
        return ret;
    }

    public static TridentTuple createFreshTuple(Fields fields, List<Object> values) {
        FreshOutputFactory factory = new FreshOutputFactory(fields);
        return factory.create(values);
    }

    public static TridentTuple createFreshTuple(Fields fields, Object... values) {
        FreshOutputFactory factory = new FreshOutputFactory(fields);
        return factory.create(Arrays.asList(values));
    }

    @Override
    public List<Object> getValues() {
        return this;
    }

    @Override
    public int size() {
        return index.length;
    }

    @Override
    public boolean contains(String field) {
        return getFields().contains(field);
    }

    @Override
    public Fields getFields() {
        return new Fields(indexToFieldsList(index));
    }

    @Override
    public int fieldIndex(String field) {
        return getFields().fieldIndex(field);
    }

    @Override
    public List<Object> select(Fields selector) {
        return getFields().select(selector, getValues());
    }

    @Override
    public Object get(int i) {
        return getValue(i);
    }

    @Override
    public Object getValue(int i) {
        return getValueByPointer(index[i]);
    }

    @Override
    public String getString(int i) {
        return (String) getValue(i);
    }

    @Override
    public Integer getInteger(int i) {
        return (Integer) getValue(i);
    }

    @Override
    public Long getLong(int i) {
        return (Long) getValue(i);
    }

    @Override
    public Boolean getBoolean(int i) {
        return (Boolean) getValue(i);
    }

    @Override
    public Short getShort(int i) {
        return (Short) getValue(i);
    }

    @Override
    public Byte getByte(int i) {
        return (Byte) getValue(i);
    }

    @Override
    public Double getDouble(int i) {
        return (Double) getValue(i);
    }

    @Override
    public Float getFloat(int i) {
        return (Float) getValue(i);
    }

    @Override
    public byte[] getBinary(int i) {
        return (byte[]) getValue(i);
    }

    @Override
    public Object getValueByField(String field) {
        return getValueByPointer(fieldIndex.get(field));
    }

    @Override
    public String getStringByField(String field) {
        return (String) getValueByField(field);
    }

    @Override
    public Integer getIntegerByField(String field) {
        return (Integer) getValueByField(field);
    }

    @Override
    public Long getLongByField(String field) {
        return (Long) getValueByField(field);
    }

    @Override
    public Boolean getBooleanByField(String field) {
        return (Boolean) getValueByField(field);
    }

    @Override
    public Short getShortByField(String field) {
        return (Short) getValueByField(field);
    }

    @Override
    public Byte getByteByField(String field) {
        return (Byte) getValueByField(field);
    }

    @Override
    public Double getDoubleByField(String field) {
        return (Double) getValueByField(field);
    }

    @Override
    public Float getFloatByField(String field) {
        return (Float) getValueByField(field);
    }

    @Override
    public byte[] getBinaryByField(String field) {
        return (byte[]) getValueByField(field);
    }

    private Object getValueByPointer(ValuePointer ptr) {
        return delegates.get(ptr.delegateIndex).get(ptr.index);
    }

    public static class ProjectionFactory implements Factory {
        Map<String, ValuePointer> fieldIndex;
        ValuePointer[] index;
        Factory parent;

        public ProjectionFactory(Factory parent, Fields projectFields) {
            this.parent = parent;
            if (projectFields == null) {
                projectFields = new Fields();
            }
            Map<String, ValuePointer> parentFieldIndex = parent.getFieldIndex();
            fieldIndex = new HashMap<>();
            for (String f : projectFields) {
                fieldIndex.put(f, parentFieldIndex.get(f));
            }
            index = ValuePointer.buildIndex(projectFields, fieldIndex);
        }

        public TridentTuple create(TridentTuple parent) {
            if (index.length == 0) {
                return EMPTY_TUPLE;
            } else {
                return new TridentTupleView(((TridentTupleView) parent).delegates, index, fieldIndex);
            }
        }

        @Override
        public Map<String, ValuePointer> getFieldIndex() {
            return fieldIndex;
        }

        @Override
        public int numDelegates() {
            return parent.numDelegates();
        }

        @Override
        public List<String> getOutputFields() {
            return indexToFieldsList(index);
        }
    }

    public static class FreshOutputFactory implements Factory {
        Map<String, ValuePointer> fieldIndex;
        ValuePointer[] index;

        public FreshOutputFactory(Fields selfFields) {
            fieldIndex = new HashMap<>();
            for (int i = 0; i < selfFields.size(); i++) {
                String field = selfFields.get(i);
                fieldIndex.put(field, new ValuePointer(0, i, field));
            }
            index = ValuePointer.buildIndex(selfFields, fieldIndex);
        }

        public TridentTuple create(List<Object> selfVals) {
            return new TridentTupleView(Arrays.asList(selfVals), index, fieldIndex);
        }

        @Override
        public Map<String, ValuePointer> getFieldIndex() {
            return fieldIndex;
        }

        @Override
        public int numDelegates() {
            return 1;
        }

        @Override
        public List<String> getOutputFields() {
            return indexToFieldsList(index);
        }
    }

    public static class OperationOutputFactory implements Factory {
        Map<String, ValuePointer> fieldIndex;
        ValuePointer[] index;
        Factory parent;

        public OperationOutputFactory(Factory parent, Fields selfFields) {
            this.parent = parent;
            fieldIndex = new HashMap<>(parent.getFieldIndex());
            int myIndex = parent.numDelegates();
            for (int i = 0; i < selfFields.size(); i++) {
                String field = selfFields.get(i);
                fieldIndex.put(field, new ValuePointer(myIndex, i, field));
            }
            List<String> myOrder = new ArrayList<>(parent.getOutputFields());

            Set<String> parentFieldsSet = new HashSet<>(myOrder);
            for (String f : selfFields) {
                if (parentFieldsSet.contains(f)) {
                    throw new IllegalArgumentException(
                        "Additive operations cannot add fields with same name as already exists. "
                        + "Tried adding " + selfFields + " to " + parent.getOutputFields());
                }
                myOrder.add(f);
            }

            index = ValuePointer.buildIndex(new Fields(myOrder), fieldIndex);
        }

        public TridentTuple create(TridentTupleView parent, List<Object> selfVals) {
            List<List<Object>> curr = new ArrayList<>(parent.delegates);
            curr.add(selfVals);
            return new TridentTupleView(curr, index, fieldIndex);
        }

        @Override
        public Map<String, ValuePointer> getFieldIndex() {
            return fieldIndex;
        }

        @Override
        public int numDelegates() {
            return parent.numDelegates() + 1;
        }

        @Override
        public List<String> getOutputFields() {
            return indexToFieldsList(index);
        }
    }

    public static class RootFactory implements Factory {
        ValuePointer[] index;
        Map<String, ValuePointer> fieldIndex;

        public RootFactory(Fields inputFields) {
            index = new ValuePointer[inputFields.size()];
            int i = 0;
            for (String f : inputFields) {
                index[i] = new ValuePointer(0, i, f);
                i++;
            }
            fieldIndex = ValuePointer.buildFieldIndex(index);
        }

        public TridentTuple create(Tuple parent) {
            return new TridentTupleView(Arrays.asList(parent.getValues()), index, fieldIndex);
        }

        @Override
        public Map<String, ValuePointer> getFieldIndex() {
            return fieldIndex;
        }

        @Override
        public int numDelegates() {
            return 1;
        }

        @Override
        public List<String> getOutputFields() {
            return indexToFieldsList(this.index);
        }
    }
}

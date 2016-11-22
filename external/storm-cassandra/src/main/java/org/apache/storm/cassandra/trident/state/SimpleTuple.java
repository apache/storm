package org.apache.storm.cassandra.trident.state;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility class for passing around ordered key/value data with an immutable key set.
 */
public class SimpleTuple implements ITuple, Serializable {

    private static final long serialVersionUID = -4656331293513898312L;

    private final List<String> keys;
    private List<Object> values;

    public SimpleTuple(Fields keyFields, List<Object> values) {
        this.keys = keyFields.toList();
        this.values = new ArrayList<>();
        this.values.addAll(values);
        while (this.values.size() < keys.size()) {
            this.values.add(null);
        }
    }

    public SimpleTuple(Fields keyFields, List<Object>... values) {
        this.keys = keyFields.toList();
        this.values = new ArrayList<>();
        for (List<Object> valueList : values) {
            this.values.addAll(valueList);
        }
        while (this.values.size() < keys.size()) {
            this.values.add(null);
        }
    }

    public SimpleTuple put(String key, Object value) {
        int index = keys.indexOf(key);
        if (index >= 0) {
            values.set(index, value);
        }
        else {
            throw new IllegalArgumentException("Field " + key + " does not exist.");
        }
        return this;
    }

    public SimpleTuple setValues(List<Object> values) {
        this.values = new ArrayList<>(values);
        return this;
    }

    @Override
    public int size() {
        return keys.size();
    }

    @Override
    public boolean contains(String field) {
        return keys.contains(field);
    }

    @Override
    public Fields getFields() {
        return new Fields(keys);
    }

    @Override
    public int fieldIndex(String field) {
        return keys.indexOf(field);
    }

    @Override
    public List<Object> select(Fields selector) {
        List<Object> values = new ArrayList<>();
        for (String field : selector) {
            values.add(getValueByField(field));
        }
        return values;
    }

    @Override
    public Object getValue(int i) {
        return values.get(i);
    }

    @Override
    public String getString(int i) {
        return (String) values.get(i);
    }

    @Override
    public Integer getInteger(int i) {
        return (Integer) values.get(i);
    }

    @Override
    public Long getLong(int i) {
        return (Long) values.get(i);
    }

    @Override
    public Boolean getBoolean(int i) {
        return (Boolean) values.get(i);
    }

    @Override
    public Short getShort(int i) {
        return (Short) values.get(i);
    }

    @Override
    public Byte getByte(int i) {
        return (Byte) values.get(i);
    }

    @Override
    public Double getDouble(int i) {
        return (Double) values.get(i);
    }

    @Override
    public Float getFloat(int i) {
        return (Float) values.get(i);
    }

    @Override
    public byte[] getBinary(int i) {
        return (byte[]) values.get(i);
    }

    @Override
    public Object getValueByField(String field) {
        return values.get(keys.indexOf(field));
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

    @Override
    public List<Object> getValues() {
        return Collections.unmodifiableList(values);
    }

    public List<String> getKeys() {
        return Collections.unmodifiableList(keys);
    }

}
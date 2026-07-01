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
import java.util.List;
import java.util.Objects;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.task.GeneralTopologyContext;

/**
 * A self-contained, serializable copy of a {@link Tuple} that is detached from the topology context.
 *
 * <p>A regular {@link TupleImpl} holds a reference to the {@link GeneralTopologyContext} it was created in, which cannot be
 * serialized. A {@code DetachedTuple} instead snapshots the source component, task, stream, output fields and values of the
 * original tuple, so it can be emitted as a value inside another tuple and cross worker boundaries (see STORM-4000).
 *
 * <p>Detached tuples are unanchored: {@link #getMessageId()} always returns an unanchored message id, and
 * {@link #getContext()} throws {@link UnsupportedOperationException} since no topology context is available.
 *
 * <p>Unlike {@link TupleImpl}, which uses identity-based equality, two detached tuples are equal if they snapshot
 * the same source metadata, fields and values, so a detached tuple keeps comparing equal after a
 * serialization round-trip.
 */
public class DetachedTuple implements Tuple, Serializable {
    private static final long serialVersionUID = 1L;

    private final String srcComponent;
    private final int srcTask;
    private final String srcStream;
    private final List<String> fieldNames;
    private final List<Object> values;
    private transient Fields fields;

    /**
     * Creates a detached copy of the given tuple. The tuple must still be attached to its topology context, since the
     * output fields of the source component are resolved through it.
     *
     * @param tuple the tuple to detach
     */
    public DetachedTuple(Tuple tuple) {
        this.srcComponent = tuple.getSourceComponent();
        this.srcTask = tuple.getSourceTask();
        this.srcStream = tuple.getSourceStreamId();
        this.fieldNames = new ArrayList<>(tuple.getFields().toList());
        this.values = new ArrayList<>(tuple.getValues());
    }

    /**
     * No-arg constructor for serialization frameworks only.
     */
    private DetachedTuple() {
        this.srcComponent = null;
        this.srcTask = 0;
        this.srcStream = null;
        this.fieldNames = null;
        this.values = null;
    }

    @Override
    public int size() {
        return values.size();
    }

    @Override
    public boolean contains(String field) {
        return getFields().contains(field);
    }

    @Override
    public Fields getFields() {
        if (fields == null) {
            fields = new Fields(fieldNames);
        }
        return fields;
    }

    @Override
    public int fieldIndex(String field) {
        return getFields().fieldIndex(field);
    }

    @Override
    public List<Object> select(Fields selector) {
        return getFields().select(selector, values);
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
        return values.get(fieldIndex(field));
    }

    @Override
    public String getStringByField(String field) {
        return (String) values.get(fieldIndex(field));
    }

    @Override
    public Integer getIntegerByField(String field) {
        return (Integer) values.get(fieldIndex(field));
    }

    @Override
    public Long getLongByField(String field) {
        return (Long) values.get(fieldIndex(field));
    }

    @Override
    public Boolean getBooleanByField(String field) {
        return (Boolean) values.get(fieldIndex(field));
    }

    @Override
    public Short getShortByField(String field) {
        return (Short) values.get(fieldIndex(field));
    }

    @Override
    public Byte getByteByField(String field) {
        return (Byte) values.get(fieldIndex(field));
    }

    @Override
    public Double getDoubleByField(String field) {
        return (Double) values.get(fieldIndex(field));
    }

    @Override
    public Float getFloatByField(String field) {
        return (Float) values.get(fieldIndex(field));
    }

    @Override
    public byte[] getBinaryByField(String field) {
        return (byte[]) values.get(fieldIndex(field));
    }

    @Override
    public List<Object> getValues() {
        return values;
    }

    @Override
    public GlobalStreamId getSourceGlobalStreamId() {
        return new GlobalStreamId(srcComponent, srcStream);
    }

    @Override
    public String getSourceComponent() {
        return srcComponent;
    }

    @Override
    public int getSourceTask() {
        return srcTask;
    }

    @Override
    public String getSourceStreamId() {
        return srcStream;
    }

    @Override
    public MessageId getMessageId() {
        return MessageId.makeUnanchored();
    }

    @Override
    public GeneralTopologyContext getContext() {
        throw new UnsupportedOperationException("A detached tuple has no topology context");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DetachedTuple)) {
            return false;
        }
        DetachedTuple other = (DetachedTuple) o;
        return srcTask == other.srcTask
                && Objects.equals(srcComponent, other.srcComponent)
                && Objects.equals(srcStream, other.srcStream)
                && Objects.equals(fieldNames, other.fieldNames)
                && Objects.equals(values, other.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(srcComponent, srcTask, srcStream, fieldNames, values);
    }

    @Override
    public String toString() {
        return "source: " + srcComponent + ":" + srcTask
                + ", stream: " + srcStream
                + ", fields: " + fieldNames
                + ", " + values;
    }
}

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

package org.apache.storm.redis.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.shade.org.apache.commons.lang.NotImplementedException;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.Tuple;


/**
 * Partial Implementation of the Tuple interface for tests.
 */
public class StubTuple implements Tuple {

    final Map<String, Object> values;

    public StubTuple(final Map<String, Object> values) {
        this.values = Collections.unmodifiableMap(new HashMap<>(Objects.requireNonNull(values)));
    }

    @Override
    public int size() {
        return values.size();
    }

    @Override
    public boolean contains(final String field) {
        return values.containsKey(field);
    }

    @Override
    public Fields getFields() {
        return new Fields(values.keySet().toArray(new String[0]));
    }

    @Override
    public int fieldIndex(final String field) {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public List<Object> select(final Fields selector) {
        return null;
    }

    @Override
    public Object getValue(final int i) {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public String getString(final int i) {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public Integer getInteger(final int i) {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public Long getLong(final int i) {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public Boolean getBoolean(final int i) {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public Short getShort(final int i) {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public Byte getByte(final int i) {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public Double getDouble(final int i) {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public Float getFloat(final int i) {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public byte[] getBinary(final int i) {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public Object getValueByField(final String field) {
        return values.get(field);
    }

    @Override
    public String getStringByField(final String field) {
        return values.get(field).toString();
    }

    @Override
    public Integer getIntegerByField(final String field) {
        return (Integer) values.get(field);
    }

    @Override
    public Long getLongByField(final String field) {
        return (Long) values.get(field);
    }

    @Override
    public Boolean getBooleanByField(final String field) {
        return (Boolean) values.get(field);
    }

    @Override
    public Short getShortByField(final String field) {
        return (Short) values.get(field);
    }

    @Override
    public Byte getByteByField(final String field) {
        return (Byte) values.get(field);
    }

    @Override
    public Double getDoubleByField(final String field) {
        return (Double) values.get(field);
    }

    @Override
    public Float getFloatByField(final String field) {
        return (Float) values.get(field);
    }

    @Override
    public byte[] getBinaryByField(final String field) {
        return (byte[]) values.get(field);
    }

    @Override
    public List<Object> getValues() {
        return new ArrayList<>(values.values());
    }

    @Override
    public GlobalStreamId getSourceGlobalStreamId() {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public String getSourceComponent() {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public int getSourceTask() {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public String getSourceStreamId() {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public MessageId getMessageId() {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public GeneralTopologyContext getContext() {
        throw new NotImplementedException("Not implemented");
    }
}

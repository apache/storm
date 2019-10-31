/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.cassandra.trident.state;

import java.util.ArrayList;
import java.util.List;
import org.apache.storm.trident.state.TransactionalValue;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;

/**
 * State mapper that maps a transactional tuple to separate state fields.
 */
public class TransactionalTupleStateMapper implements StateMapper<TransactionalValue<ITuple>> {

    private final Fields tupleFields;
    private final Fields tableFields;

    public TransactionalTupleStateMapper(String txIdField, String... fields) {
        this(txIdField, new Fields(fields));
    }

    public TransactionalTupleStateMapper(String txIdField, Fields fields) {
        tupleFields = fields;
        ArrayList<String> fieldList = new ArrayList<>();
        fieldList.add(txIdField);
        for (String field : fields) {
            fieldList.add(field);
        }
        tableFields = new Fields(fieldList);
    }

    @Override
    public Fields getStateFields() {
        return tableFields;
    }

    @Override
    public Values toValues(TransactionalValue<ITuple> tuple) {
        Values values = new Values();
        values.add(tuple.getTxid());

        for (String valueField : tupleFields) {
            if (tuple.getVal() != null) {
                values.add(tuple.getVal().getValueByField(valueField));
            } else {
                values.add(null);
            }
        }

        return values;
    }

    @Override
    public TransactionalValue<ITuple> fromValues(List<Values> valuesList) {
        if (valuesList == null || valuesList.size() == 0) {
            return null;
        }
        Values values = valuesList.get(0);
        int index = 0;
        @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
        Long txId = (Long) values.get(index++);

        SimpleTuple curr = new SimpleTuple(tupleFields);
        for (String valueField : tupleFields) {
            curr.put(valueField, values.get(index++));
        }

        boolean isAllNull = true;
        for (Object value : curr.getValues()) {
            if (value != null) {
                isAllNull = false;
                break;
            }
        }
        if (isAllNull) {
            curr = null;
        }

        return new TransactionalValue<ITuple>(txId, curr);
    }

    @Override
    public String toString() {
        return String.format("{type: %s, fields: %s}", this.getClass().getSimpleName(), tableFields);
    }
}

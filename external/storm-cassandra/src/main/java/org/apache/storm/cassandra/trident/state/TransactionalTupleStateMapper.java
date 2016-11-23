package org.apache.storm.cassandra.trident.state;

import org.apache.storm.trident.state.TransactionalValue;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;

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
            }
            else {
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

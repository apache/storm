package org.apache.storm.cassandra.trident.state;

import org.apache.storm.trident.state.OpaqueValue;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;

/**
 * State mapper that maps an opaque tuple to separate state fields.
 */
public class OpaqueTupleStateMapper implements StateMapper<OpaqueValue<ITuple>> {

    private final Fields tupleFields;
    private final Fields tableFields;

    public OpaqueTupleStateMapper(String currTxIdField, String currPrefix, String prevPrefix, String... fields) {
        this(currTxIdField, currPrefix, prevPrefix, new Fields(fields));
    }

    public OpaqueTupleStateMapper(String currTxIdField, String currPrefix, String prevPrefix, Fields fields) {
        tupleFields = fields;
        ArrayList<String> fieldList = new ArrayList<>();
        fieldList.add(currTxIdField);
        for (String field : fields) {
            fieldList.add(currPrefix + field);
        }
        for (String field : fields) {
            fieldList.add(prevPrefix + field);
        }
        tableFields = new Fields(fieldList);
    }

    @Override
    public Fields getStateFields() {
        return tableFields;
    }

    @Override
    public Values toValues(OpaqueValue<ITuple> tuple) {
        Values values = new Values();
        values.add(tuple.getCurrTxid());

        for (String valueField : tupleFields) {
            if (tuple.getCurr() != null) {
                values.add(tuple.getCurr().getValueByField(valueField));
            }
            else {
                values.add(null);
            }
        }

        for (String valueField : tupleFields) {
            if (tuple.getPrev() != null) {
                values.add(tuple.getPrev().getValueByField(valueField));
            }
            else {
                values.add(null);
            }
        }

        return values;
    }

    @Override
    public OpaqueValue<ITuple> fromValues(List<Values> valuesList) {
        if (valuesList == null || valuesList.size() == 0) {
            return null;
        }
        Values values = valuesList.get(0);
        int index = 0;
        Long currTx = (Long) values.get(index++);

        SimpleTuple curr = new SimpleTuple(tupleFields);
        for (String valueField : tupleFields) {
            curr.put(valueField, values.get(index++));
        }

        if (isAllNull(curr)) {
            curr = null;
        }

        SimpleTuple prev = new SimpleTuple(tupleFields);
        for (String valueField : tupleFields) {
            prev.put(valueField, values.get(index++));
        }
        if (isAllNull(prev)) {
            prev = null;
        }

        return new OpaqueValue<ITuple>(currTx, curr, prev);
    }

    private boolean isAllNull(SimpleTuple tuple) {
        for (Object value : tuple.getValues()) {
            if (value != null) {
                return false;
            }
        }
        return true;
    }
}

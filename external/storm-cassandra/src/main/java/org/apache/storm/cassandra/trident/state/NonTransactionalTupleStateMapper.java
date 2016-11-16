package org.apache.storm.cassandra.trident.state;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;

import java.util.List;

/**
 * State mapper that maps a tuple to separate state fields.
 */
public class NonTransactionalTupleStateMapper implements StateMapper<ITuple> {

    private final Fields fields;

    public NonTransactionalTupleStateMapper(String... fields) {
        this.fields = new Fields(fields);
    }

    public NonTransactionalTupleStateMapper(Fields fields) {
        this.fields = fields;
    }

    @Override
    public Fields getStateFields() {
        return fields;
    }

    @Override
    public Values toValues(ITuple t) {
        return new Values(t.getValues());
    }

    @Override
    public ITuple fromValues(List<Values> values) {
        if (values == null || values.size() == 0) {
            return null;
        }
        return new SimpleTuple(fields, values.get(0));
    }
}

package org.apache.storm.cassandra.trident.state;

import org.apache.storm.trident.state.OpaqueValue;
import org.apache.storm.trident.state.StateType;
import org.apache.storm.trident.state.TransactionalValue;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.List;

public class SimpleStateMapper<T> implements StateMapper<T> {

    private final Fields fields;
    private final StateType stateType;

    public SimpleStateMapper(Fields fields, StateType stateType) {
        this.fields = fields;
        this.stateType = stateType;
    }

    public static <U> StateMapper<OpaqueValue<U>> opaque(String txIdField, String previousField, String field) {
        return new SimpleStateMapper<>(new Fields(txIdField, field, previousField), StateType.OPAQUE);
    }

    public static <U> StateMapper<TransactionalValue<U>> opaque(String txIdField, String field) {
        return new SimpleStateMapper<>(new Fields(txIdField, field), StateType.TRANSACTIONAL);
    }

    public static <U> StateMapper<U> nontransactional(String field) {
        return new SimpleStateMapper<>(new Fields(field), StateType.NON_TRANSACTIONAL);
    }

    @Override
    public Fields getStateFields() {
        return fields;
    }

    @Override
    public Values toValues(T value) {
        if (value == null) {
            return null;
        }
        switch (stateType) {
            case NON_TRANSACTIONAL:
                return new Values(value);
            case TRANSACTIONAL:
                TransactionalValue transactional = (TransactionalValue) value;
                return new Values(transactional.getTxid(), transactional.getVal());
            case OPAQUE:
                OpaqueValue opaque = (OpaqueValue) value;
                return new Values(opaque.getCurrTxid(), opaque.getCurr(), opaque.getPrev());
            default:
                throw new IllegalStateException("Unknown state type " + stateType);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public T fromValues(List<Values> valuesSet) {
        if (valuesSet == null || valuesSet.size() == 0) {
            return null;
        }
        else if (valuesSet.size() == 1) {
            Values values = valuesSet.get(0);
            if (values == null) {
                return null;
            }
            switch (stateType) {
                case NON_TRANSACTIONAL:
                    return (T) values.get(0);
                case TRANSACTIONAL:
                    return (T) new TransactionalValue((Long) values.get(0), values.get(1));
                case OPAQUE:
                    return (T) new OpaqueValue((Long) values.get(0), values.get(1), values.get(2));
                default:
                    throw new IllegalStateException("Unknown state type " + stateType);
            }
        }
        throw new IllegalStateException("State query returned multiple results.");
    }
}

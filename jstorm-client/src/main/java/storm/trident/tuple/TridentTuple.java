package storm.trident.tuple;

import backtype.storm.tuple.ITuple;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public interface TridentTuple extends ITuple, List<Object> {

    public static interface Factory extends Serializable {
        Map<String, ValuePointer> getFieldIndex();
        List<String> getOutputFields();
        int numDelegates();
    }

}

package storm.trident.testing;

import java.util.List;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.utils.Utils;

public class TuplifyArgs extends BaseFunction {

    @Override
    public void execute(TridentTuple input, TridentCollector collector) {
        String args = input.getString(0);
        List<List<Object>> tuples = (List) Utils.from_json(args);
        for(List<Object> tuple: tuples) {
            collector.emit(tuple);
        }
    }
    
}

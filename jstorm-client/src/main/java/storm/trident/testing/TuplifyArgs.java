package storm.trident.testing;

import java.util.List;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import com.alibaba.fastjson.JSON;

public class TuplifyArgs extends BaseFunction {

    @Override
    public void execute(TridentTuple input, TridentCollector collector) {
        String args = input.getString(0);
        List<List<Object>> tuples = (List) JSON.parse(args);
        for(List<Object> tuple: tuples) {
            collector.emit(tuple);
        }
    }
    
}

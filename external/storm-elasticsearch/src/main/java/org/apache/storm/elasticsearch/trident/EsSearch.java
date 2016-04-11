package org.apache.storm.elasticsearch.trident;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseQueryFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.Collection;
import java.util.List;

public class EsSearch extends BaseQueryFunction<EsState, Collection<Values>> {

    @Override
    public List<Collection<Values>> batchRetrieve(EsState state, List<TridentTuple> tridentTuples) {
        return state.batchRetrieve(tridentTuples);
    }

    @Override
    public void execute(TridentTuple tuple, Collection<Values> valuesList, TridentCollector collector) {
        for (Values values : valuesList) {
            collector.emit(values);
        }
    }
}

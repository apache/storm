package org.apache.storm.beam.translation.runtime;

import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class GroupByKeyInitBolt extends BaseRichBolt {
    private OutputCollector collector;


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public GroupByKeyInitBolt() {
    }

    @Override
    public void execute(Tuple input) {

        List<WindowedValue<KV>> values = (List<WindowedValue<KV>>)input.getValue(0);
        for(WindowedValue<KV> value : values) {
            KV kv = value.getValue();
            Object key = kv.getKey();
            this.collector.emit(input, new Values(key, value));
            this.collector.ack(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("keyValue", "windowedValue"));
    }
}

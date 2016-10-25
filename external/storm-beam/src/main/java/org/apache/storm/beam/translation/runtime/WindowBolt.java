package org.apache.storm.beam.translation.runtime;

import com.google.common.collect.Lists;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class WindowBolt extends BaseWindowedBolt {
    private static final Logger LOG = LoggerFactory.getLogger(WindowBolt.class);

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        LOG.info("*******************Executing window with size {}", inputWindow.get().size());
        LOG.info("Type: {}", inputWindow.get().getClass());
        List values = Lists.newArrayList();
        for(Tuple t :inputWindow.get()){
            values.add(t.getValue(0));
        }
        collector.emit(inputWindow.get(), new Values(values));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("window"));
    }
}

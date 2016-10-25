package org.apache.storm.beam.translation.runtime;

import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.storm.beam.StormPipelineOptions;
import org.apache.storm.beam.util.SerializedPipelineOptions;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * Spout implementation that wraps a Beam UnboundedSource
 */
public class UnboundedSourceSpout extends BaseRichSpout{
    private static final Logger LOG = LoggerFactory.getLogger(UnboundedSourceSpout.class);

    private UnboundedSource source;
    private transient UnboundedSource.UnboundedReader reader;
    private SerializedPipelineOptions serializedOptions;
    private transient StormPipelineOptions pipelineOptions;

    private SpoutOutputCollector collector;

    public UnboundedSourceSpout(UnboundedSource source, StormPipelineOptions options){
        this.source = source;
        this.serializedOptions = new SerializedPipelineOptions(options);
    }

    @Override
    public void close() {
        super.close();
        try {
            this.reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void activate() {
        super.activate();
    }

    @Override
    public void deactivate() {
        super.deactivate();
    }

    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
    }

    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return super.getComponentConfiguration();
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("value"));

    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        try {

            this.collector = collector;
            this.pipelineOptions = this.serializedOptions.getPipelineOptions().as(StormPipelineOptions.class);
            this.reader = this.source.createReader(this.pipelineOptions, null);
            this.reader.start();
        } catch (IOException e) {
            throw new RuntimeException("Unable to create unbounded reader.", e);
        }

    }

    public void nextTuple() {
        try {
            if(this.reader.advance()){
                Object value = reader.getCurrent();
                Instant timestamp = reader.getCurrentTimestamp();
                Instant watermark = reader.getWatermark();

                WindowedValue wv = WindowedValue.of(value, timestamp, GlobalWindow.INSTANCE, PaneInfo.NO_FIRING);
                collector.emit(new Values(wv), UUID.randomUUID());
            }
        } catch (IOException e) {
            throw new RuntimeException("Exception reading values from source.", e);
        }
    }
}

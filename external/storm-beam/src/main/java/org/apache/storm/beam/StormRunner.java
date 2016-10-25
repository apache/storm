package org.apache.storm.beam;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.runners.AggregatorRetrievalException;
import org.apache.beam.sdk.runners.AggregatorValues;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.beam.translation.StormPipelineTranslator;
import org.apache.storm.beam.translation.TranslationContext;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.*;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Main entry point into the Storm Runner.
 * 
 * After reading the user defined pipeline, Beam will invoke the run() method with a representation
 * of the pipeline.
 * 
 * TODO: Only supports storm local mode for now.
 */
public class StormRunner extends PipelineRunner<StormRunner.StormPipelineResult> {
    private static final Logger LOG = LoggerFactory.getLogger(StormRunner.class);
    
    private StormPipelineOptions options;

    public StormRunner(StormPipelineOptions options){
        this.options = options;
    }

    public static StormRunner fromOptions(PipelineOptions options){
        StormPipelineOptions pipelineOptions = PipelineOptionsValidator.validate(StormPipelineOptions.class, options);
        return new StormRunner(pipelineOptions);

    }

    @Override
    public StormPipelineResult run(Pipeline pipeline) {
        LOG.info("Running pipeline...");
        TranslationContext context = new TranslationContext(this.options);
        StormPipelineTranslator transformer = new StormPipelineTranslator(context);
        transformer.translate(pipeline);

        for(TranslationContext.Stream stream : context.getStreams()){
            LOG.info(stream.getFrom() + " --> " + stream.getTo());
        }

        runTopologyLocal(getTopology(context));
        return null;
    }

    private void runTopologyLocal(StormTopology topology){
        Config conf = new Config();
        conf.setMaxSpoutPending(1000);
//        conf.setDebug(true);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count", conf, topology);

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

//        cluster.shutdown();
    }

    public static class StormPipelineResult implements PipelineResult {
        private State state;

        public State getState() {
            return this.state;
        }

        public <T> AggregatorValues<T> getAggregatorValues(Aggregator<?, T> aggregator) throws AggregatorRetrievalException {
            return null;
        }
    }

    private StormTopology getTopology(TranslationContext context){
        TopologyBuilder builder = new TopologyBuilder();
        Map<String, IRichSpout> spouts = context.getSpouts();
        for(String id : spouts.keySet()){
            builder.setSpout(id, spouts.get(id));
        }

        HashMap<String, BoltDeclarer> declarers = new HashMap<String, BoltDeclarer>();
        for (TranslationContext.Stream stream : context.getStreams()) {
            Object boltObj = context.getBolt(stream.getTo());
            BoltDeclarer declarer = declarers.get(stream.getTo());
            if (boltObj instanceof IRichBolt) {
                if(declarer == null) {
                    declarer = builder.setBolt(stream.getTo(),
                            (IRichBolt) boltObj);
                    declarers.put(stream.getTo(), declarer);
                }
            } else if (boltObj instanceof IBasicBolt) {
                if(declarer == null) {
                    declarer = builder.setBolt(
                            stream.getTo(),
                            (IBasicBolt) boltObj);
                    declarers.put(stream.getTo(), declarer);
                }
            } else if (boltObj instanceof IWindowedBolt) {
                if(declarer == null) {
                    declarer = builder.setBolt(
                            stream.getTo(),
                            (IWindowedBolt) boltObj);
                    declarers.put(stream.getTo(), declarer);
                }
            } else if (boltObj instanceof IStatefulBolt) {
                if(declarer == null) {
                    declarer = builder.setBolt(
                            stream.getTo(),
                            (IStatefulBolt) boltObj);
                    declarers.put(stream.getTo(), declarer);
                }
            } else {
                throw new IllegalArgumentException("Class does not appear to be a bolt: " +
                        boltObj.getClass().getName());
            }

            TranslationContext.Grouping grouping = stream.getGrouping();
            // if the streamId is defined, use it for the grouping, otherwise assume storm's default stream
            String streamId = (grouping.getStreamId() == null ? Utils.DEFAULT_STREAM_ID : grouping.getStreamId());


            switch (grouping.getType()) {
                case SHUFFLE:
                    declarer.shuffleGrouping(stream.getFrom(), streamId);
                    break;
                case FIELDS:
                    //TODO check for null grouping args
                    declarer.fieldsGrouping(stream.getFrom(), streamId, new Fields(grouping.getArgs()));
                    break;
                case ALL:
                    declarer.allGrouping(stream.getFrom(), streamId);
                    break;
                case DIRECT:
                    declarer.directGrouping(stream.getFrom(), streamId);
                    break;
                case GLOBAL:
                    declarer.globalGrouping(stream.getFrom(), streamId);
                    break;
                case LOCAL_OR_SHUFFLE:
                    declarer.localOrShuffleGrouping(stream.getFrom(), streamId);
                    break;
                case NONE:
                    declarer.noneGrouping(stream.getFrom(), streamId);
                    break;
                default:
                    throw new UnsupportedOperationException("unsupported grouping type: " + grouping);
            }
        }

        return builder.createTopology();
    }
}

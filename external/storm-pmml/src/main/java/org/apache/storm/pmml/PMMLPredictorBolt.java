/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.storm.pmml;

import java.util.List;
import java.util.Map;

import org.apache.storm.pmml.model.ModelOutputs;
import org.apache.storm.pmml.runner.ModelRunner;
import org.apache.storm.pmml.runner.ModelRunnerFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseTickTupleAwareRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class PMMLPredictorBolt extends BaseTickTupleAwareRichBolt {
    protected static final Logger LOG = LoggerFactory.getLogger(PMMLPredictorBolt.class);

    private final ModelOutputs outputs;
    private final ModelRunnerFactory runnerFactory;
    private ModelRunner runner;
    private OutputCollector collector;

    /*
     * Passing a factory rather than the actual object to avoid enforcing the strong
     * requirement of having to have ModelRunner to be Serializable
     */

    /**
     * Creates an instance of {@link PMMLPredictorBolt} that executes, for every tuple, the runner constructed with
     * the {@link ModelRunnerFactory} specified in the parameter
     * The {@link PMMLPredictorBolt} instantiated with this constructor declares the output fields as specified
     * by the {@link ModelOutputs} parameter.
     */
    public PMMLPredictorBolt(ModelRunnerFactory modelRunnerFactory, ModelOutputs modelOutputs) {
        this.outputs = modelOutputs;
        this.runnerFactory = modelRunnerFactory;
        LOG.info("Instantiated {}", this);
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.runner = runnerFactory.newModelRunner();
        this.collector = collector;
    }

    @Override
    protected void process(Tuple input) {
        try {
            final Map<String, List<Object>> scoresPerStream = runner.scoredTuplePerStream(input);
            LOG.debug("Input tuple [{}] generated predicted scores [{}]", input, scoresPerStream);
            if (scoresPerStream != null) {
                for (Map.Entry<String, List<Object>> streamToTuple : scoresPerStream.entrySet()) {
                    collector.emit(streamToTuple.getKey(), input, streamToTuple.getValue());
                }
                collector.ack(input);
            } else {
                LOG.debug("Input tuple [{}] generated NULL scores", input);
            }
        } catch (Exception e) {
            collector.reportError(e);
            collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        LOG.info("Declaring output fields [{}]", outputs);
        for (Map.Entry<String, ? extends Fields> streamToFields : outputs.streamFields().entrySet()) {
            declarer.declareStream(streamToFields.getKey(), streamToFields.getValue());
        }
    }

    @Override
    public String toString() {
        return "PMMLPredictorBolt{"
                + "outputFields=" + outputs
                + ", runnerFactory=" + runnerFactory.getClass().getName()
                + ", runner=" + runner
                + ", collector=" + collector
                + "} ";
    }
}

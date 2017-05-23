/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.st.topology.window;

import com.google.common.collect.Lists;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.apache.storm.st.topology.TestableTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.st.utils.StringDecorator;
import org.apache.storm.st.utils.TimeUtil;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Computes sliding window sum
 */
public class TumblingWindowCorrectness implements TestableTopology {
    private static final Logger LOG = LoggerFactory.getLogger(TumblingWindowCorrectness.class);
    private static final String NUMBER_FIELD = "number";
    private static final String STRING_FIELD = "numAsStr";
    private final int tumbleSize;
    private final String spoutName;
    private final String boltName;

    public TumblingWindowCorrectness(final int tumbleSize) {
        this.tumbleSize = tumbleSize;
        final String prefix = this.getClass().getSimpleName() + "-tubleSize" + tumbleSize;
        spoutName = prefix + "IncrementingSpout";
        boltName = prefix + "VerificationBolt";
    }

    public String getBoltName() {
        return boltName;
    }

    public String getSpoutName() {
        return spoutName;
    }

    public StormTopology newTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(getSpoutName(), new IncrementingSpout(), 1);
        builder.setBolt(getBoltName(),
                new VerificationBolt()
                        .withTumblingWindow(new BaseWindowedBolt.Count(tumbleSize)), 1)
                .shuffleGrouping(getSpoutName());
        return builder.createTopology();
    }

    public List<String> getExpectedOutput() {
        return Lists.newArrayList(
                StringDecorator.decorate(getBoltName(), "tuplesInWindow.size() = " + tumbleSize),
                StringDecorator.decorate(getBoltName(), "newTuples.size() = " + tumbleSize),
                StringDecorator.decorate(getBoltName(), "expiredTuples.size() = " + tumbleSize)
        );
    }

    public static class IncrementingSpout extends BaseRichSpout {
        private static final Logger LOG = LoggerFactory.getLogger(IncrementingSpout.class);
        private SpoutOutputCollector collector;
        private static int currentNum;
        private static Random rng = new Random();
        private String componentId;

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(NUMBER_FIELD, STRING_FIELD));
        }

        @Override
        public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
            componentId = context.getThisComponentId();
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            TimeUtil.sleepMilliSec(rng.nextInt(10));
            currentNum++;
            final String numAsStr = "str(" + currentNum + ")str";
            final Values tuple = new Values(currentNum, numAsStr);
            LOG.info(StringDecorator.decorate(componentId, tuple.toString()));
            collector.emit(tuple, currentNum);
        }

        @Override
        public void ack(Object msgId) {
            LOG.info("Received ACK for msgId : " + msgId);
        }

        @Override
        public void fail(Object msgId) {
            LOG.info("Received FAIL for msgId : " + msgId);
        }
    }

    public static class VerificationBolt extends BaseWindowedBolt {
        private OutputCollector collector;
        private String componentId;

        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
            componentId = context.getThisComponentId();
            this.collector = collector;
        }

        @Override
        public void execute(TupleWindow inputWindow) {
            List<Tuple> tuplesInWindow = inputWindow.get();
            List<Tuple> newTuples = inputWindow.getNew();
            List<Tuple> expiredTuples = inputWindow.getExpired();
            LOG.info("tuplesInWindow.size() = " + tuplesInWindow.size());
            LOG.info("newTuples.size() = " + newTuples.size());
            LOG.info("expiredTuples.size() = " + expiredTuples.size());
            LOG.info(StringDecorator.decorate(componentId, "tuplesInWindow = " + tuplesInWindow.toString()));
            collector.emit(new Values("dummyValue"));
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(DUMMY_FIELD));
        }
    }
}

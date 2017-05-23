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

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.st.topology.TestableTopology;
import org.apache.storm.st.topology.window.data.TimeData;
import org.apache.storm.st.utils.TimeUtil;
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
import org.apache.storm.st.utils.StringDecorator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Computes sliding window sum
 */
public class TumblingTimeCorrectness implements TestableTopology {
    private static final Logger LOG = LoggerFactory.getLogger(TumblingTimeCorrectness.class);
    private final int tumbleSec;
    private final String spoutName;
    private final String boltName;

    public TumblingTimeCorrectness(int timbleSec) {
        this.tumbleSec = timbleSec;
        final String prefix = this.getClass().getSimpleName() + "-timbleSec" + timbleSec;
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
        builder.setSpout(getSpoutName(), new IncrementingSpout(), 2);
        builder.setBolt(getBoltName(),
                new VerificationBolt()
                        .withTumblingWindow(new BaseWindowedBolt.Duration(tumbleSec, TimeUnit.SECONDS))
                        .withLag(new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS))
                        .withTimestampField(TimeData.getTimestampFieldName()),
                1)
                .globalGrouping(getSpoutName());
        return builder.createTopology();
    }

    public List<String> getExpectedOutput() {
        return Lists.newArrayList(
                StringDecorator.decorate(getBoltName(), "tuplesInWindow.size() = " + tumbleSec),
                StringDecorator.decorate(getBoltName(), "newTuples.size() = " + tumbleSec),
                StringDecorator.decorate(getBoltName(), "expiredTuples.size() = " + tumbleSec)
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
            declarer.declare(TimeData.getFields());
        }

        @Override
        public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
            componentId = context.getThisComponentId();
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            //Emitting too quickly can lead to spurious test failures because the worker log may roll right before we read it
            //Sleep a bit between emits
            TimeUtil.sleepMilliSec(rng.nextInt(100));
            currentNum++;
            TimeData data = TimeData.newData(currentNum);
            final Values tuple = data.getValues();
            collector.emit(tuple);
            LOG.info(StringDecorator.decorate(componentId, data.toString()));
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
            Collection<TimeData> dataInWindow = Collections2.transform(tuplesInWindow, new Function<Tuple, TimeData>() {
                @Nullable
                @Override
                public TimeData apply(@Nullable Tuple input) {
                    return TimeData.fromTuple(input);
                }
            });
            final String jsonData = TimeData.toString(dataInWindow);
            LOG.info(StringDecorator.decorate(componentId, jsonData));
            collector.emit(new Values("dummyValue"));
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(DUMMY_FIELD));
        }
    }
}

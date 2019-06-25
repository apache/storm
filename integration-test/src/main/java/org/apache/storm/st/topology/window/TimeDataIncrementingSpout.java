/*
 * Copyright 2018 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.st.topology.window;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.st.topology.TestableTopology;
import org.apache.storm.st.topology.window.data.TimeData;
import org.apache.storm.st.utils.StringDecorator;
import org.apache.storm.st.utils.TimeUtil;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeDataIncrementingSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(TimeDataIncrementingSpout.class);
    private SpoutOutputCollector collector;
    private int currentNum;
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
        if (currentNum >= TestableTopology.MAX_SPOUT_EMITS) {
            //Stop emitting at a certain point, because log rolling breaks the tests.
            return;
        }
        //Sleep a bit between emits to ensure that we don't reach the cap too quickly, since this spout is used to test time based windows
        TimeUtil.sleepMilliSec(TestableTopology.TIMEDATA_SLEEP_BETWEEN_EMITS_MS);
        currentNum++;
        TimeData data = TimeData.newData(currentNum);
        final Values tuple = data.getValues();
        collector.emit(tuple);
        LOG.info(StringDecorator.decorate(componentId, data.toString()));
    }
}

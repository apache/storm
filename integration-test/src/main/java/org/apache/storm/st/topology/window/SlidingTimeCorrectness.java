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

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.st.topology.TestableTopology;
import org.apache.storm.st.topology.window.data.TimeData;
import org.apache.storm.st.utils.StringDecorator;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Computes sliding window sum.
 */
public class SlidingTimeCorrectness implements TestableTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SlidingTimeCorrectness.class);
    private final int windowSec;
    private final int slideSec;
    private final String spoutName;
    private final int spoutExecutors = 2;
    private final String boltName;
    private final int boltExecutors = 1;

    public SlidingTimeCorrectness(int windowSec, int slideSec) {
        this.windowSec = windowSec;
        this.slideSec = slideSec;
        final String prefix = this.getClass().getSimpleName() + "-winSec" + windowSec + "slideSec" + slideSec;
        spoutName = prefix + "IncrementingSpout";
        boltName = prefix + "VerificationBolt";
    }

    @Override
    public String getBoltName() {
        return boltName;
    }

    @Override
    public String getSpoutName() {
        return spoutName;
    }

    @Override
    public int getBoltExecutors() {
        return boltExecutors;
    }

    @Override
    public int getSpoutExecutors() {
        return spoutExecutors;
    }

    @Override
    public StormTopology newTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(getSpoutName(), new TimeDataIncrementingSpout(), spoutExecutors);
        builder.setBolt(getBoltName(),
                new TimeDataVerificationBolt()
                        .withWindow(new BaseWindowedBolt.Duration(windowSec, TimeUnit.SECONDS),
                                new BaseWindowedBolt.Duration(slideSec, TimeUnit.SECONDS))
                        .withTimestampField(TimeData.getTimestampFieldName())
                        .withLag(new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS)),
                boltExecutors)
                .globalGrouping(getSpoutName());
        return builder.createTopology();
    }
}

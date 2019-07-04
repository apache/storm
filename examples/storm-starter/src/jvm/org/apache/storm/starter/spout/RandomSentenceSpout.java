/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.starter.spout;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Random;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RandomSentenceSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(RandomSentenceSpout.class);

    SpoutOutputCollector collector;
    Random rand;


    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        rand = new Random();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        String[] sentences = new String[]{
            sentence("the cow jumped over the moon"), sentence("an apple a day keeps the doctor away"),
            sentence("four score and seven years ago"), sentence("snow white and the seven dwarfs"), sentence("i am at two with nature")
        };
        final String sentence = sentences[rand.nextInt(sentences.length)];

        LOG.debug("Emitting tuple: {}", sentence);

        collector.emit(new Values(sentence));
    }

    protected String sentence(String input) {
        return input;
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    // Add unique identifier to each tuple, which is helpful for debugging
    public static class TimeStamped extends RandomSentenceSpout {
        private final String prefix;

        public TimeStamped() {
            this("");
        }

        public TimeStamped(String prefix) {
            this.prefix = prefix;
        }

        @Override
        protected String sentence(String input) {
            return prefix + currentDate() + " " + input;
        }

        private String currentDate() {
            return new SimpleDateFormat("yyyy.MM.dd_HH:mm:ss.SSSSSSSSS").format(new Date());
        }
    }
}

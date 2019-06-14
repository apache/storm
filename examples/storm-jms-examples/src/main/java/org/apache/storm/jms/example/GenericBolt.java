/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.jms.example;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A generic <code>org.apache.storm.topology.IRichBolt</code> implementation
 * for testing/debugging the Storm JMS Spout and example topologies.
 *
 * <p>For debugging purposes, set the log level of the
 * <code>org.apache.storm.contrib.jms</code> package to DEBUG for debugging
 * output.</p>
 *
 * @author tgoetz
 */
@SuppressWarnings("serial")
public class GenericBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(GenericBolt.class);
    private OutputCollector collector;
    private boolean autoAck = false;
    private boolean autoAnchor = false;
    private Fields declaredFields;
    private String name;

    /**
     * Constructs a new <code>GenericBolt</code> instance.
     *
     * @param name           The name of the bolt (used in DEBUG logging)
     * @param autoAck        Whether or not this bolt should automatically acknowledge received tuples.
     * @param autoAnchor     Whether or not this bolt should automatically anchor to received tuples.
     * @param declaredFields The fields this bolt declares as output.
     */
    public GenericBolt(String name, boolean autoAck, boolean autoAnchor, Fields declaredFields) {
        this.name = name;
        this.autoAck = autoAck;
        this.autoAnchor = autoAnchor;
        this.declaredFields = declaredFields;
    }

    public GenericBolt(String name, boolean autoAck, boolean autoAnchor) {
        this(name, autoAck, autoAnchor, null);
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;

    }

    @Override
    public void execute(Tuple input) {
        LOG.debug("[" + this.name + "] Received message: " + input);


        // only emit if we have declared fields.
        if (this.declaredFields != null) {
            LOG.debug("[" + this.name + "] emitting: " + input);
            if (this.autoAnchor) {
                this.collector.emit(input, input.getValues());
            } else {
                this.collector.emit(input.getValues());
            }
        }

        if (this.autoAck) {
            LOG.debug("[" + this.name + "] ACKing tuple: " + input);
            this.collector.ack(input);
        }

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (this.declaredFields != null) {
            declarer.declare(this.declaredFields);
        }
    }

    public boolean isAutoAck() {
        return this.autoAck;
    }

    public void setAutoAck(boolean autoAck) {
        this.autoAck = autoAck;
    }

}

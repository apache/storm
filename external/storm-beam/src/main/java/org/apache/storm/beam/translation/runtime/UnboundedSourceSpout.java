/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

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

import com.google.api.client.util.Lists;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.*;
import org.apache.beam.sdk.util.common.Counter;
import org.apache.beam.sdk.util.common.CounterSet;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.storm.beam.StormPipelineOptions;
import org.apache.storm.beam.translation.util.DefaultStepContext;
import org.apache.storm.beam.util.SerializedPipelineOptions;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.*;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class DoFnBolt<InputT, OutputT> extends BaseRichBolt implements DoFnRunners.OutputManager{

    private transient DoFnRunner<InputT, OutputT> runner = null;

    private final TupleTag<OutputT> tupleTag = new TupleTag<OutputT>() {};

    private transient OutputCollector collector;

    private List<WindowedValue<OutputT>> output = Lists.newArrayList();

    private SerializedPipelineOptions serializedOptions;
    private transient StormPipelineOptions pipelineOptions;

    private DoFn<InputT, OutputT> doFn;
    private WindowingStrategy<?, ?> windowingStrategy;
    private SideInputReader sideInputReader;

    public DoFnBolt(
            StormPipelineOptions pipelineOptions,
            DoFn<InputT, OutputT> doFn,
            WindowingStrategy<?, ?> windowingStrategy,
            SideInputReader sideInputReader){
        this.serializedOptions = new SerializedPipelineOptions(pipelineOptions);
        this.doFn = doFn;
        this.windowingStrategy = windowingStrategy;
        this.sideInputReader = sideInputReader;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.pipelineOptions = this.serializedOptions.getPipelineOptions().as(StormPipelineOptions.class);

        Counter<Integer> counter = Counter.ints("foo", Counter.AggregationKind.SUM);
        CounterSet counters = new CounterSet(counter);

        this.runner = new StormDoFnRunner(this.pipelineOptions, this.doFn, this.sideInputReader, this, this.tupleTag, TupleTagList.empty().getAll(), new DefaultStepContext(), counters.getAddCounterMutator(), this.windowingStrategy);
    }

    @Override
    public void execute(Tuple input) {
        System.out.println("Type: " + input.getValue(0).getClass());
        Object value = input.getValue(0);
        this.output = Lists.newArrayList();
        this.runner.startBundle();
        if(value instanceof List){
            for(Object o : ((List)value)){
                this.runner.processElement((WindowedValue)o);
            }

        } else {
            this.runner.processElement((WindowedValue) input.getValue(0));
        }
        this.runner.finishBundle();

//        for(WindowedValue val : this.output){
//            this.collector.emit(input, new Values(val));
//        }
        this.collector.emit(new Values(this.output));
        this.collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("value"));
    }


    @Override
    public <T> void output(TupleTag<T> tupleTag, WindowedValue<T> windowedValue) {
        if(this.tupleTag.equals(tupleTag)){
            this.output.add((WindowedValue<OutputT>)windowedValue);
        } else {
            throw new RuntimeException("Wrong tag");
        }
    }
}

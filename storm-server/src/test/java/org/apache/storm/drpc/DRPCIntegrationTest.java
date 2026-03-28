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

package org.apache.storm.drpc;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.ILocalDRPC;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.Thrift;
import org.apache.storm.coordination.CoordinatedBolt;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.generated.DRPCRequest;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.FailedException;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.ServiceRegistry;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration and unit tests for DRPC topology patterns.
 *
 * Ported from storm-core/test/clj/org/apache/storm/drpc_test.clj
 */
public class DRPCIntegrationTest {

    // --- Bolt implementations (replacements for Clojure defbolt) ---

    /**
     * Appends "!!!" to input and forwards with return-info.
     * Used with raw DRPCSpout + ReturnResults topology.
     */
    static class ExclamationBolt extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            collector.emit(new Values(tuple.getString(0) + "!!!", tuple.getValue(1)));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("result", "return-info"));
        }
    }

    /**
     * Appends "!!!" to input. Used with LinearDRPCTopologyBuilder.
     * Fields are [id, result] per DRPC builder convention.
     */
    static class ExclamationBoltDrpc extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            collector.emit(new Values(tuple.getValue(0), tuple.getString(1) + "!!!"));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "result"));
        }
    }

    /**
     * Emits N*N tuples for input N (used for coordination counting test).
     */
    static class CreateTuples extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            Object id = tuple.getValue(0);
            int amt = Integer.parseInt((String) tuple.getValue(1));
            for (int i = 0; i < amt * amt; i++) {
                collector.emit(new Values(id));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("request"));
        }
    }

    /**
     * Counts tuples per request ID, emits count on finishedId.
     */
    static class PartialCount extends BaseRichBolt implements CoordinatedBolt.FinishedCallback {
        private OutputCollector collector;
        private final Map<Object, Integer> counts = new ConcurrentHashMap<>();

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            Object id = tuple.getValue(0);
            counts.merge(id, 1, Integer::sum);
            collector.ack(tuple);
        }

        @Override
        public void finishedId(Object id) {
            collector.emit(new Values(id, counts.getOrDefault(id, 0)));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("request", "count"));
        }
    }

    /**
     * Aggregates counts per request ID, emits total on finishedId.
     */
    static class CountAggregator extends BaseRichBolt implements CoordinatedBolt.FinishedCallback {
        private OutputCollector collector;
        private final Map<Object, Integer> counts = new ConcurrentHashMap<>();

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            Object id = tuple.getValue(0);
            int count = (Integer) tuple.getValue(1);
            counts.merge(id, count, Integer::sum);
            collector.ack(tuple);
        }

        @Override
        public void finishedId(Object id) {
            collector.emit(new Values(id, counts.getOrDefault(id, 0)));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("request", "total"));
        }
    }

    /**
     * Passes tuples through unchanged.
     */
    static class IdBolt extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            collector.emit(tuple.getValues());
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("request", "val"));
        }
    }

    /**
     * Acks all tuples, emits "done" on finishedId.
     */
    static class EmitFinish extends BaseRichBolt implements CoordinatedBolt.FinishedCallback {
        private OutputCollector collector;

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            collector.ack(tuple);
        }

        @Override
        public void finishedId(Object id) {
            collector.emit(new Values(id, "done"));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("request", "result"));
        }
    }

    /**
     * Throws FailedException on finishedId.
     */
    static class FailFinishBolt extends BaseRichBolt implements CoordinatedBolt.FinishedCallback {
        private OutputCollector collector;

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            collector.ack(tuple);
        }

        @Override
        public void finishedId(Object id) {
            throw new FailedException();
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("request", "result"));
        }
    }

    // --- Integration tests ---

    @Test
    public void testDrpcFlow() throws Exception {
        try (LocalCluster cluster = new LocalCluster();
             LocalDRPC drpc = new LocalDRPC(cluster.getMetricRegistry())) {

            DRPCSpout spout = new DRPCSpout("test", drpc);
            StormTopology topology = Thrift.buildTopology(
                Map.of("1", Thrift.prepareSpoutDetails(spout)),
                Map.of(
                    "2", Thrift.prepareBoltDetails(
                        Map.of(Utils.getGlobalStreamId("1", null), Thrift.prepareShuffleGrouping()),
                        new ExclamationBolt()),
                    "3", Thrift.prepareBoltDetails(
                        Map.of(Utils.getGlobalStreamId("2", null), Thrift.prepareGlobalGrouping()),
                        new ReturnResults())));

            cluster.submitTopology("test", Map.of(), topology);
            assertEquals("aaa!!!", drpc.execute("test", "aaa"));
            assertEquals("b!!!", drpc.execute("test", "b"));
            assertEquals("c!!!", drpc.execute("test", "c"));
        }
    }

    @Test
    public void testDrpcBuilder() throws Exception {
        try (LocalCluster cluster = new LocalCluster();
             LocalDRPC drpc = new LocalDRPC(cluster.getMetricRegistry())) {

            LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("test");
            builder.addBolt(new ExclamationBoltDrpc(), 3);

            cluster.submitTopology("builder-test", Map.of(), builder.createLocalTopology(drpc));
            assertEquals("aaa!!!", drpc.execute("test", "aaa"));
            assertEquals("b!!!", drpc.execute("test", "b"));
            assertEquals("c!!!", drpc.execute("test", "c"));
        }
    }

    @Test
    public void testDrpcCoordination() throws Exception {
        try (LocalCluster cluster = new LocalCluster();
             LocalDRPC drpc = new LocalDRPC(cluster.getMetricRegistry())) {

            LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("square");
            builder.addBolt(new CreateTuples(), 3);
            builder.addBolt(new PartialCount(), 3).shuffleGrouping();
            builder.addBolt(new CountAggregator(), 3).fieldsGrouping(new Fields("request"));

            cluster.submitTopology("squared", Map.of(), builder.createLocalTopology(drpc));
            assertEquals("4", drpc.execute("square", "2"));
            assertEquals("100", drpc.execute("square", "10"));
            assertEquals("1", drpc.execute("square", "1"));
            assertEquals("0", drpc.execute("square", "0"));
        }
    }

    @Test
    public void testDrpcCoordinationTricky() throws Exception {
        try (LocalCluster cluster = new LocalCluster();
             LocalDRPC drpc = new LocalDRPC(cluster.getMetricRegistry())) {

            LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("tricky");
            builder.addBolt(new IdBolt(), 3);
            builder.addBolt(new IdBolt(), 3).shuffleGrouping();
            builder.addBolt(new EmitFinish(), 3).fieldsGrouping(new Fields("request"));

            cluster.submitTopology("tricky", Map.of(), builder.createLocalTopology(drpc));
            assertEquals("done", drpc.execute("tricky", "2"));
            assertEquals("done", drpc.execute("tricky", "3"));
            assertEquals("done", drpc.execute("tricky", "4"));
        }
    }

    @Test
    public void testDrpcFailFinish() throws Exception {
        try (LocalCluster cluster = new LocalCluster();
             LocalDRPC drpc = new LocalDRPC(cluster.getMetricRegistry())) {

            LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("fail2");
            builder.addBolt(new FailFinishBolt(), 3);

            cluster.submitTopology("fail2", Map.of(), builder.createLocalTopology(drpc));
            assertThrows(DRPCExecutionException.class, () -> drpc.execute("fail2", "2"));
        }
    }

    // --- Mockito tests for DRPCSpout reconnect behavior ---

    @Test
    public void testDrpcAttemptsTwoReconnectsInFailRequest() throws Exception {
        DRPCInvocationsClient handler = Mockito.mock(DRPCInvocationsClient.class,
            Mockito.withSettings().extraInterfaces(ILocalDRPC.class));
        SpoutOutputCollector outputCollector = Mockito.mock(SpoutOutputCollector.class);
        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);

        String serviceId = ServiceRegistry.registerService(handler);
        try {
            Mockito.when(((ILocalDRPC) handler).getServiceId()).thenReturn(serviceId);
            Mockito.when(handler.fetchRequest(ArgumentMatchers.anyString()))
                .thenReturn(new DRPCRequest("square 2", "bar"));
            Mockito.doThrow(new DRPCExecutionException())
                .when(handler).failRequest(ArgumentMatchers.anyString());

            DRPCSpout spout = new DRPCSpout("test", (ILocalDRPC) handler);
            spout.open(null, null, outputCollector);
            spout.nextTuple();
            Mockito.verify(outputCollector).emit(Mockito.anyList(), captor.capture());
            spout.fail(captor.getValue());

            Mockito.verify(handler, Mockito.times(2)).reconnectClient();
            Mockito.verify(handler, Mockito.times(3)).failRequest(ArgumentMatchers.anyString());
        } finally {
            ServiceRegistry.unregisterService(serviceId);
        }
    }

    @Test
    public void testDrpcStopsRetryingAfterSuccessfulReconnect() throws Exception {
        DRPCInvocationsClient handler = Mockito.mock(DRPCInvocationsClient.class,
            Mockito.withSettings().extraInterfaces(ILocalDRPC.class));
        SpoutOutputCollector outputCollector = Mockito.mock(SpoutOutputCollector.class);
        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);

        String serviceId = ServiceRegistry.registerService(handler);
        try {
            Mockito.when(((ILocalDRPC) handler).getServiceId()).thenReturn(serviceId);
            Mockito.when(handler.fetchRequest(ArgumentMatchers.anyString()))
                .thenReturn(new DRPCRequest("square 2", "bar"));
            Mockito.doThrow(new DRPCExecutionException())
                .doNothing()
                .when(handler).failRequest(ArgumentMatchers.anyString());

            DRPCSpout spout = new DRPCSpout("test", (ILocalDRPC) handler);
            spout.open(null, null, outputCollector);
            spout.nextTuple();
            Mockito.verify(outputCollector).emit(Mockito.anyList(), captor.capture());
            spout.fail(captor.getValue());

            Mockito.verify(handler, Mockito.times(1)).reconnectClient();
            Mockito.verify(handler, Mockito.times(2)).failRequest(ArgumentMatchers.anyString());
        } finally {
            ServiceRegistry.unregisterService(serviceId);
        }
    }
}

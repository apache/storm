/*
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

package org.apache.storm.executor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.storm.Constants;
import org.apache.storm.daemon.worker.WorkerState;
import org.apache.storm.daemon.worker.WorkerTransfer;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.messaging.TaskMessage;
import org.apache.storm.metrics2.StormMetricRegistry;
import org.apache.storm.policy.WaitStrategyPark;
import org.apache.storm.serialization.KryoTupleDeserializer;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.testing.TestWordCounter;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.JCQueue;
import org.apache.storm.utils.Utils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.FieldSetter;

/**
 * Some topologies might spawn extra threads inside components to perform real processing work and emit processed results.
 * This unit test is to mimic these scenarios in the {@link ExecutorTransfer} level
 * and make sure the tuples sent out in a multi-threading fashion to the workerTransferQueue
 * is properly handled and received by the remote Worker (consumer)
 *
 * The topology structure this test mimics is
 * {worker1: taskId=1, component="1"} --> {worker2: taskId=2, component="2"}.
 */
public class ExecutorTransferMultiThreadingTest {

    private WorkerState workerState;
    private Map<String, Object> topoConf;
    private JCQueue transferQueue;
    private GeneralTopologyContext generalTopologyContext;
    private int selfTaskId = 1;
    private String sourceComp = "1";
    private int remoteTaskId = 2;
    private String destComp = "2";
    private static String value1 = "string-value";
    private static int value2 = 1234;

    @Before
    public void setup() throws NoSuchFieldException {
        topoConf = Utils.readStormConfig();
        String topologyId = "multi-threaded-topo-test";
        StormTopology stormTopology = createStormTopology();

        WorkerTopologyContext workerTopologyContext = mock(WorkerTopologyContext.class);
        when(workerTopologyContext.getRawTopology()).thenReturn(stormTopology);
        when(workerTopologyContext.getComponentId(selfTaskId)).thenReturn(sourceComp);
        when(workerTopologyContext.getComponentId(remoteTaskId)).thenReturn(destComp);

        workerState = mock(WorkerState.class);
        when(workerState.getWorkerTopologyContext()).thenReturn(workerTopologyContext);
        Map<Integer, JCQueue> receiveQMap = new HashMap<>();
        //local recvQ is not important in this test; simple mock it
        receiveQMap.put(selfTaskId, mock(JCQueue.class));
        when(workerState.getLocalReceiveQueues()).thenReturn(receiveQMap);
        when(workerState.getTopologyId()).thenReturn(topologyId);
        when(workerState.getPort()).thenReturn(6701);
        when(workerState.getMetricRegistry()).thenReturn(new StormMetricRegistry());
        when(workerState.tryTransferRemote(any(), any(), any())).thenCallRealMethod();

        //the actual worker transfer queue to be used in this test
        //taskId for worker transfer queue should be -1.
        //But there is already one worker transfer queue initialized by WorkerTransfer class (taskId=-1).
        //However the taskId is only used for metrics and it is not important here. Making it -100 to avoid collision.
        transferQueue = new JCQueue("worker-transfer-queue", "worker-transfer-queue", 1024, 0, 1, new WaitStrategyPark(100),
            workerState.getTopologyId(), Constants.SYSTEM_COMPONENT_ID, Collections.singletonList(-100), workerState.getPort(),
            workerState.getMetricRegistry());

        //Replace the transferQueue inside WorkerTransfer (inside WorkerState) with the customized transferQueue to be used in this test
        WorkerTransfer workerTransfer = new WorkerTransfer(workerState, topoConf, 2);
        FieldSetter.setField(workerTransfer, workerTransfer.getClass().getDeclaredField("transferQueue"), transferQueue);
        FieldSetter.setField(workerState, workerState.getClass().getDeclaredField("workerTransfer"), workerTransfer);

        generalTopologyContext = mock(GeneralTopologyContext.class);
    }

    @Test
    public void testExecutorTransfer() throws InterruptedException {
        //There is one ExecutorTransfer per executor
        ExecutorTransfer executorTransfer = new ExecutorTransfer(workerState, topoConf);
        executorTransfer.initLocalRecvQueues();
        ExecutorService executorService = Executors.newFixedThreadPool(5);

        //There can be multiple producer threads sending out tuples inside each executor
        //This mimics the case of multi-threading components where a component spawns extra threads to emit tuples.
        int producerTaskNum = 10;
        Runnable[] producerTasks = new Runnable[producerTaskNum];
        for (int i = 0; i < producerTaskNum; i++) {
            producerTasks[i] = createProducerTask(executorTransfer);
        }
        for (Runnable task : producerTasks) {
            executorService.submit(task);
        }

        //give producers enough time to insert messages into the queue
        executorService.awaitTermination(1000, TimeUnit.MILLISECONDS);

        //consume all the tuples in the queue and deserialize them one by one
        //this mimics a remote worker.
        KryoTupleDeserializer deserializer = new KryoTupleDeserializer(topoConf, workerState.getWorkerTopologyContext());
        SingleThreadedConsumer consumer = new SingleThreadedConsumer(deserializer, producerTaskNum);
        transferQueue.consume(consumer);
        consumer.finalCheck();
        executorService.shutdown();
    }

    private Runnable createProducerTask(ExecutorTransfer executorTransfer) {
        return new Runnable() {
            Tuple tuple = new TupleImpl(generalTopologyContext, new Values(value1, value2), sourceComp, selfTaskId, "default");
            AddressedTuple addressedTuple = new AddressedTuple(remoteTaskId, tuple);

            @Override
            public void run() {
                executorTransfer.tryTransfer(addressedTuple, null);
            }
        };
    }

    private StormTopology createStormTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(sourceComp, new TestWordSpout(true), 1);
        builder.setBolt(destComp, new TestWordCounter(), 1).fieldsGrouping(sourceComp, new Fields("word"));
        return builder.createTopology();
    }

    private static class SingleThreadedConsumer implements JCQueue.Consumer {
        KryoTupleDeserializer deserializer;
        int numMessages;
        int msgCount = 0;

        public SingleThreadedConsumer(KryoTupleDeserializer deserializer, int numMessages) {
            this.deserializer = deserializer;
            this.numMessages = numMessages;
        }

        /**
         * There are multiple producers sending messages to the queue simultaneously.
         * The consumer receives messages one by one and tries to deserialize them.
         * If there is any issues/exceptions during the process, it basically means data corruption is happening.
         * @param o the received object
         */
        @Override
        public void accept(Object o) {
            TaskMessage taskMessage = (TaskMessage) o;
            TupleImpl receivedTuple = deserializer.deserialize(taskMessage.message());
            Assert.assertEquals(receivedTuple.getValue(0), value1);
            Assert.assertEquals(receivedTuple.getValue(1), value2);
            msgCount++;
        }

        /**
         * This makes sure every message sent by the producers are received by this consumer.
         */
        public void finalCheck() {
            Assert.assertEquals(numMessages, msgCount);
        }

        @Override
        public void flush() {
            //no op
        }
    }
}
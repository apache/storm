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


package org.apache.storm.executor;

import org.apache.storm.Constants;
import org.apache.storm.cluster.IStateStorage;
import org.apache.storm.daemon.worker.WorkerState;
import org.apache.storm.executor.spout.SpoutExecutor;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.metrics2.RateCounter;
import org.apache.storm.metrics2.StormMetricRegistry;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.utils.RotatingMap;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.ReflectionUtils;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;

public class SpoutExecutorTest {


    @Test
    public void testPendingTuplesRotateShouldBeCalledOnlyOnce() throws Exception {

        RateCounter rateCounter = Mockito.mock(RateCounter.class);

        StormMetricRegistry stormMetricRegistry = Mockito.mock(StormMetricRegistry.class);
        Mockito.when(stormMetricRegistry.rateCounter(anyString(),anyString(),anyInt())).thenReturn(rateCounter);

        Map<String,Object> hashmap = Utils.readDefaultConfig();

        IStateStorage stateStorage = Mockito.mock(IStateStorage.class);

        ComponentCommon componentCommon = Mockito.mock(ComponentCommon.class);
        Mockito.when(componentCommon.get_json_conf()).thenReturn(null);

        WorkerTopologyContext workerTopologyContext = Mockito.mock(WorkerTopologyContext.class);
        Mockito.when(workerTopologyContext.getComponentId(anyInt())).thenReturn("1");
        Mockito.when(workerTopologyContext.getComponentCommon(anyString())).thenReturn(componentCommon);

        WorkerState workerState = Mockito.mock(WorkerState.class);
        Mockito.when(workerState.getWorkerTopologyContext()).thenReturn(workerTopologyContext);
        Mockito.when(workerState.getStateStorage()).thenReturn(stateStorage);
        Mockito.when(workerState.getTopologyConf()).thenReturn(hashmap);
        Mockito.when(workerState.getMetricRegistry()).thenReturn(stormMetricRegistry);

        SpoutExecutor spoutExecutor = new SpoutExecutor(workerState,List.of(1L,5L),new HashMap<>());

        TupleImpl tuple =   Mockito.mock(TupleImpl.class);
        Mockito.when(tuple.getSourceStreamId()).thenReturn(Constants.SYSTEM_TICK_STREAM_ID);
        AddressedTuple addressedTuple =   Mockito.mock(AddressedTuple.class);
        Mockito.when(addressedTuple.getDest()).thenReturn(AddressedTuple.BROADCAST_DEST);
        Mockito.when(addressedTuple.getTuple()).thenReturn(tuple);

        RotatingMap rotatingMap = Mockito.mock(RotatingMap.class);
        Field fieldRotatingMap = ReflectionUtils
                .findFields(SpoutExecutor.class, f -> f.getName().equals("pending"),
                        ReflectionUtils.HierarchyTraversalMode.TOP_DOWN)
                .get(0);
        fieldRotatingMap.setAccessible(true);
        fieldRotatingMap.set(spoutExecutor, rotatingMap);

        spoutExecutor.accept(addressedTuple);

        Mockito.verify(rotatingMap,Mockito.times(1)).rotate();
    }
}

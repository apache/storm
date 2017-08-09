/**
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

package org.apache.storm.topology;

import com.google.common.collect.ImmutableMap;
import org.apache.storm.Config;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.streams.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.Event;
import org.apache.storm.windowing.TimestampExtractor;
import org.apache.storm.windowing.TupleWindow;
import org.apache.storm.windowing.WaterMarkEvent;
import org.apache.storm.windowing.WaterMarkEventGenerator;
import org.apache.storm.windowing.persistence.WindowState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.mockito.AdditionalAnswers.returnsArgAt;

/**
 * Unit tests for {@link PersistentWindowedBoltExecutor}
 */
@RunWith(MockitoJUnitRunner.class)
public class PersistentWindowedBoltExecutorTest {
    private static final String LATE_STREAM = "late_stream";
    private static final String PARTITION_KEY = "pk";
    private static final String EVICTION_STATE_KEY = "es";
    private static final String TRIGGER_STATE_KEY = "ts";
    private static final int WINDOW_EVENT_COUNT = 5;

    private long tupleTs;
    private PersistentWindowedBoltExecutor<KeyValueState<String, String>> executor;
    private IStatefulWindowedBolt<KeyValueState<String, String>> mockBolt;
    private Map<String, Object> testStormConf = new HashMap<>();
    private OutputCollector mockOutputCollector;
    private TopologyContext mockTopologyContext;
    private TimestampExtractor mockTimestampExtractor;
    private WaterMarkEventGenerator mockWaterMarkEventGenerator;

    @Mock
    private KeyValueState<String, Deque<Long>> mockPartitionState;
    @Mock
    private KeyValueState<Long, WindowState.WindowPartition<Tuple>> mockWindowState;
    @Mock
    private KeyValueState<String, Optional<?>> mockSystemState;

    @Captor
    private ArgumentCaptor<Tuple> tupleCaptor;
    @Captor
    private ArgumentCaptor<Collection<Tuple>> anchorCaptor;
    @Captor
    private ArgumentCaptor<Long> longCaptor;
    @Captor
    private ArgumentCaptor<Values> valuesCaptor;
    @Captor
    private ArgumentCaptor<TupleWindow> tupleWindowCaptor;
    @Captor
    private ArgumentCaptor<Deque<Long>> partitionValuesCaptor;
    @Captor
    private ArgumentCaptor<WindowState.WindowPartition<Tuple>> windowValuesCaptor;
    @Captor
    private ArgumentCaptor<Optional<?>> systemValuesCaptor;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        mockBolt = Mockito.mock(IStatefulWindowedBolt.class);
        mockWaterMarkEventGenerator = Mockito.mock(WaterMarkEventGenerator.class);
        mockTimestampExtractor = Mockito.mock(TimestampExtractor.class);
        tupleTs = System.currentTimeMillis();
        Mockito.when(mockTimestampExtractor.extractTimestamp(Mockito.any())).thenReturn(tupleTs);
        Mockito.when(mockBolt.getTimestampExtractor()).thenReturn(mockTimestampExtractor);
        Mockito.when(mockBolt.isPersistent()).thenReturn(true);
        mockTopologyContext = Mockito.mock(TopologyContext.class);
        Mockito.when(mockTopologyContext.getThisStreams()).thenReturn(Collections.singleton(LATE_STREAM));
        mockOutputCollector = Mockito.mock(OutputCollector.class);
        executor = new PersistentWindowedBoltExecutor<>(mockBolt);
        testStormConf.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT, WINDOW_EVENT_COUNT);
        testStormConf.put(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT, WINDOW_EVENT_COUNT);
        testStormConf.put(Config.TOPOLOGY_BOLTS_LATE_TUPLE_STREAM, LATE_STREAM);
        testStormConf.put(Config.TOPOLOGY_BOLTS_WATERMARK_EVENT_INTERVAL_MS, 100_000);
        testStormConf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 30);
        testStormConf.put(Config.TOPOLOGY_STATE_CHECKPOINT_INTERVAL, 1000);
        Mockito.when(mockPartitionState.get(Mockito.any(), Mockito.any())).then(returnsArgAt(1));
        Mockito.when(mockWindowState.get(Mockito.any(), Mockito.any())).then(returnsArgAt(1));
        Mockito.when(mockSystemState.get(Mockito.any(), Mockito.any())).then(returnsArgAt(1));
        Mockito.when(mockSystemState.iterator()).thenReturn(
            ImmutableMap.<String, Optional<?>>of("es", Optional.empty(), "ts", Optional.empty()).entrySet().iterator());
        executor.prepare(testStormConf, mockTopologyContext, mockOutputCollector,
            mockWindowState, mockPartitionState, mockSystemState);
    }

    @Test
    public void testExecuteTuple() throws Exception {
        Mockito.when(mockWaterMarkEventGenerator.track(Mockito.any(GlobalStreamId.class), Mockito.anyLong())).thenReturn(true);
        Tuple mockTuple = Mockito.mock(Tuple.class);
        executor.initState(null);
        executor.waterMarkEventGenerator = mockWaterMarkEventGenerator;
        executor.execute(mockTuple);
        // should be ack-ed once
        Mockito.verify(mockOutputCollector, Mockito.times(1)).ack(mockTuple);
    }

    @Test
    public void testExecuteLatetuple() throws Exception {
        Mockito.when(mockWaterMarkEventGenerator.track(Mockito.any(GlobalStreamId.class), Mockito.anyLong())).thenReturn(false);
        Tuple mockTuple = Mockito.mock(Tuple.class);
        executor.initState(null);
        executor.waterMarkEventGenerator = mockWaterMarkEventGenerator;
        executor.execute(mockTuple);
        // ack-ed once
        Mockito.verify(mockOutputCollector, Mockito.times(1)).ack(mockTuple);
        // late tuple emitted
        ArgumentCaptor<String> stringCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mockOutputCollector, Mockito.times(1))
            .emit(stringCaptor.capture(), anchorCaptor.capture(), valuesCaptor.capture());
        Assert.assertEquals(LATE_STREAM, stringCaptor.getValue());
        Assert.assertEquals(Collections.singletonList(mockTuple), anchorCaptor.getValue());
        Assert.assertEquals(new Values(mockTuple), valuesCaptor.getValue());
    }

    @Test
    public void testActivation() throws Exception {
        Mockito.when(mockWaterMarkEventGenerator.track(Mockito.any(GlobalStreamId.class), Mockito.anyLong())).thenReturn(true);
        executor.initState(null);
        executor.waterMarkEventGenerator = mockWaterMarkEventGenerator;

        List<Tuple> mockTuples = getMockTuples(WINDOW_EVENT_COUNT);
        mockTuples.forEach(t -> executor.execute(t));
        // all tuples acked
        Mockito.verify(mockOutputCollector, Mockito.times(WINDOW_EVENT_COUNT)).ack(tupleCaptor.capture());
        Assert.assertArrayEquals(mockTuples.toArray(), tupleCaptor.getAllValues().toArray());

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                TupleWindow window = (TupleWindow) invocation.getArguments()[0];
                // iterate the tuples
                Assert.assertEquals(WINDOW_EVENT_COUNT, window.get().size());
                // iterating multiple times should produce same events
                Assert.assertEquals(WINDOW_EVENT_COUNT, window.get().size());
                Assert.assertEquals(WINDOW_EVENT_COUNT, window.get().size());
                return null;
            }
        }).when(mockBolt).execute(Mockito.any());
        // trigger the window
        long activationTs = tupleTs + 1000;
        executor.getWindowManager().add(new WaterMarkEvent<>(activationTs));
        executor.prePrepare(0);

        // partition ids
        ArgumentCaptor<String> pkCatptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mockPartitionState, Mockito.times(1)).put(pkCatptor.capture(), partitionValuesCaptor.capture());
        Assert.assertEquals(PARTITION_KEY, pkCatptor.getValue());
        List<Long> expectedPartitionIds = Collections.singletonList(0L);
        Assert.assertEquals(expectedPartitionIds, partitionValuesCaptor.getValue());

        // window partitions
        Mockito.verify(mockWindowState, Mockito.times(1)).put(longCaptor.capture(), windowValuesCaptor.capture());
        Assert.assertEquals((long) expectedPartitionIds.get(0), (long) longCaptor.getValue());
        Assert.assertEquals(WINDOW_EVENT_COUNT, windowValuesCaptor.getValue().size());
        List<Tuple> tuples = windowValuesCaptor.getValue()
            .getEvents().stream().map(Event::get).collect(Collectors.toList());
        Assert.assertArrayEquals(mockTuples.toArray(), tuples.toArray());

        // window system state
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mockSystemState, Mockito.times(2)).put(keyCaptor.capture(), systemValuesCaptor.capture());
        Assert.assertEquals(EVICTION_STATE_KEY, keyCaptor.getAllValues().get(0));
        Assert.assertEquals(Optional.of(Pair.of((long)WINDOW_EVENT_COUNT, (long)WINDOW_EVENT_COUNT)), systemValuesCaptor.getAllValues().get(0));
        Assert.assertEquals(TRIGGER_STATE_KEY, keyCaptor.getAllValues().get(1));
        Assert.assertEquals(Optional.of(tupleTs), systemValuesCaptor.getAllValues().get(1));
    }

    @Test
    public void testCacheEviction() {
        Mockito.when(mockWaterMarkEventGenerator.track(Mockito.any(GlobalStreamId.class), Mockito.anyLong())).thenReturn(true);
        executor.initState(null);
        executor.waterMarkEventGenerator = mockWaterMarkEventGenerator;
        int tupleCount = 20000;
        List<Tuple> mockTuples = getMockTuples(tupleCount);
        mockTuples.forEach(t -> executor.execute(t));

        int numPartitions = tupleCount/WindowState.MAX_PARTITION_EVENTS;
        int numEvictedPartitions =  numPartitions - WindowState.MIN_PARTITIONS;
        Mockito.verify(mockWindowState, Mockito.times(numEvictedPartitions)).put(longCaptor.capture(), windowValuesCaptor.capture());
        // number of evicted events
        Assert.assertEquals(numEvictedPartitions*WindowState.MAX_PARTITION_EVENTS, windowValuesCaptor.getAllValues().stream()
            .mapToInt(x -> x.size()).sum());

        Map<Long, WindowState.WindowPartition<Tuple>> partitionMap = new HashMap<>();
        windowValuesCaptor.getAllValues().forEach(v -> partitionMap.put(v.getId(), v));

        ArgumentCaptor<String> stringCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mockPartitionState, Mockito.times(numPartitions)).put(stringCaptor.capture(), partitionValuesCaptor.capture());
        // partition ids 0 .. 19
        Assert.assertEquals(LongStream.range(0, numPartitions).boxed().collect(Collectors.toList()), partitionValuesCaptor.getAllValues().get(numPartitions-1));

        Mockito.when(mockWindowState.get(Mockito.any(), Mockito.any())).then(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                WindowState.WindowPartition<Tuple> evicted = partitionMap.get(args[0]);
                return evicted != null ? evicted : args[1];
            }
        });

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                partitionMap.put((long)args[0], (WindowState.WindowPartition<Tuple>)args[1]);
                return null;
            }
        }).when(mockWindowState).put(Mockito.any(), Mockito.any());

        // trigger the window
        long activationTs = tupleTs + 1000;
        executor.getWindowManager().add(new WaterMarkEvent<>(activationTs));

        Mockito.verify(mockBolt, Mockito.times(tupleCount/WINDOW_EVENT_COUNT)).execute(Mockito.any());
    }

    @Test
    public void testRollbackBeforeInit() throws Exception {
        executor.preRollback();
        Mockito.verify(mockBolt, Mockito.times(1)).preRollback();
        // partition ids
        ArgumentCaptor<String> pkCatptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mockPartitionState, Mockito.times(1)).rollback();
        Mockito.verify(mockWindowState, Mockito.times(1)).rollback();
        Mockito.verify(mockSystemState, Mockito.times(1)).rollback();
    }

    @Test
    public void testRollbackAfterInit() throws Exception {
        executor.initState(null);
        executor.prePrepare(0);
        executor.preRollback();
        Mockito.verify(mockBolt, Mockito.times(1)).preRollback();
        Mockito.verify(mockPartitionState, Mockito.times(1)).rollback();
        ArgumentCaptor<String> stringArgumentCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mockPartitionState, Mockito.times(2)).put(stringArgumentCaptor.capture(), partitionValuesCaptor.capture());
        Mockito.verify(mockWindowState, Mockito.times(1)).rollback();
        Mockito.verify(mockSystemState, Mockito.times(1)).rollback();
        Mockito.verify(mockSystemState, Mockito.times(2)).iterator();
    }

    private List<Tuple> getMockTuples(long count) {
        List<Tuple> tuples = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            tuples.add(Mockito.mock(Tuple.class));
        }
        return tuples;
    }
}
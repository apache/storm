/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.windowing.persistence;

import org.apache.storm.state.KeyValueState;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.Event;
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
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.mockito.AdditionalAnswers.returnsArgAt;

/**
 * Unit tests for {@link WindowState}
 */
@RunWith(MockitoJUnitRunner.class)
public class WindowStateTest {

    @Mock
    private KeyValueState<Long, WindowState.WindowPartition<Integer>> windowState;
    @Mock
    private KeyValueState<String, Deque<Long>> partitionIdsState;
    @Mock
    private KeyValueState<String, Optional<?>> systemState;
    @Mock
    private Supplier<Map<String, Optional<?>>> supplier;
    @Captor
    private ArgumentCaptor<Long> longCaptor;
    @Captor
    private ArgumentCaptor<WindowState.WindowPartition<Integer>> windowValuesCaptor;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testAdd() throws Exception {
        Mockito.when(partitionIdsState.get(Mockito.any(), Mockito.any())).then(returnsArgAt(1));
        Mockito.when(windowState.get(Mockito.any(), Mockito.any())).then(returnsArgAt(1));

        WindowState<Integer> ws = getWindowState(10 * WindowState.MAX_PARTITION_EVENTS);

        long partitions = 15;
        long numEvents = partitions * WindowState.MAX_PARTITION_EVENTS;
        for (int i = 0; i < numEvents; i++) {
            ws.add(getEvent(i));
        }
        // 5 partitions evicted to window state
        Mockito.verify(windowState, Mockito.times(5)).put(longCaptor.capture(), windowValuesCaptor.capture());
        Assert.assertEquals(5, longCaptor.getAllValues().size());
        // each evicted partition has MAX_EVENTS_PER_PARTITION
        windowValuesCaptor.getAllValues().forEach(wp -> {
            Assert.assertEquals(WindowState.MAX_PARTITION_EVENTS, wp.size());
        });
        // last partition is not evicted
        Assert.assertFalse(longCaptor.getAllValues().contains(partitions - 1));
    }

    @Test
    public void testIterator() throws Exception {
        Map<Long, WindowState.WindowPartition<Event<Tuple>>> partitionMap = new HashMap<>();
        Mockito.when(partitionIdsState.get(Mockito.any(), Mockito.any())).then(returnsArgAt(1));
        Mockito.when(windowState.get(Mockito.any(), Mockito.any())).then(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                WindowState.WindowPartition<Event<Tuple>> evicted = partitionMap.get(args[0]);
                return evicted != null ? evicted : args[1];
            }
        });

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                partitionMap.put((long)args[0], (WindowState.WindowPartition<Event<Tuple>>)args[1]);
                return null;
            }
        }).when(windowState).put(Mockito.any(), Mockito.any());

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                partitionMap.remove(args[0]);
                return null;
            }
        }).when(windowState).delete(Mockito.anyLong());

        Mockito.when(supplier.get()).thenReturn(Collections.emptyMap());

        WindowState<Integer> ws = getWindowState(10 * WindowState.MAX_PARTITION_EVENTS);

        long partitions = 15;

        long numEvents = partitions * WindowState.MAX_PARTITION_EVENTS;
        List<Event<Integer>> expected = new ArrayList<>();
        for (int i = 0; i < numEvents; i++) {
            Event<Integer> event = getEvent(i);
            expected.add(event);
            ws.add(event);
        }

        Assert.assertEquals(5, partitionMap.size());
        Iterator<Event<Integer>> it = ws.iterator();
        List<Event<Integer>> actual = new ArrayList<>();
        it.forEachRemaining(actual::add);
        Assert.assertEquals(expected, actual);

        // iterate again
        it = ws.iterator();
        actual.clear();
        it.forEachRemaining(actual::add);
        Assert.assertEquals(expected, actual);

        // remove
        it = ws.iterator();
        while (it.hasNext()) {
            it.next();
            it.remove();
        }

        it = ws.iterator();
        actual.clear();
        it.forEachRemaining(actual::add);
        Assert.assertEquals(Collections.emptyList(), actual);
    }

    @Test
    public void testIteratorPartitionNotEvicted() throws Exception {
        Map<Long, WindowState.WindowPartition<Event<Tuple>>> partitionMap = new HashMap<>();
        Mockito.when(partitionIdsState.get(Mockito.any(), Mockito.any())).then(returnsArgAt(1));
        Mockito.when(windowState.get(Mockito.any(), Mockito.any())).then(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                WindowState.WindowPartition<Event<Tuple>> evicted = partitionMap.get(args[0]);
                return evicted != null ? evicted : args[1];
            }
        });

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                partitionMap.put((long)args[0], (WindowState.WindowPartition<Event<Tuple>>)args[1]);
                return null;
            }
        }).when(windowState).put(Mockito.any(), Mockito.any());

        Mockito.when(supplier.get()).thenReturn(Collections.emptyMap());

        WindowState<Integer> ws = getWindowState(10 * WindowState.MAX_PARTITION_EVENTS);

        long partitions = 10;

        long numEvents = partitions * WindowState.MAX_PARTITION_EVENTS;
        List<Event<Integer>> expected = new ArrayList<>();
        for (int i = 0; i < numEvents; i++) {
            Event<Integer> event = getEvent(i);
            expected.add(event);
            ws.add(event);
        }

        // Stop iterating in the middle of the 10th partition
        Iterator<Event<Integer>> it = ws.iterator();
        for(int i=0; i<9500; i++) {
            it.next();
        }

        for (int i = 0; i < numEvents; i++) {
            Event<Integer> event = getEvent(i);
            expected.add(event);
            ws.add(event);
        }

        // 10th partition should not have been evicted
        Assert.assertFalse(partitionMap.containsKey(9L));
    }

    private Event<Integer> getEvent(int i) {
        return getEvent(i, 0);
    }

    private Event<Integer> getEvent(int i, long ts) {
        return new Event<Integer>() {
            @Override
            public long getTimestamp() {
                return ts;
            }

            @Override
            public Integer get() {
                return i;
            }

            @Override
            public boolean isWatermark() {
                return false;
            }
        };
    }

    private WindowState<Integer> getWindowState(int maxEvents) {
        return new WindowState<>(windowState, partitionIdsState, systemState,
            supplier, maxEvents);
    }
}
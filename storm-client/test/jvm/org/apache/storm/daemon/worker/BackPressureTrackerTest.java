/*
 * Copyright 2018 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.daemon.worker;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.apache.storm.daemon.worker.BackPressureTracker.BackpressureState;
import org.apache.storm.messaging.netty.BackPressureStatus;
import org.apache.storm.metrics2.StormMetricRegistry;
import org.apache.storm.shade.org.apache.curator.shaded.com.google.common.collect.ImmutableMap;
import org.apache.storm.utils.JCQueue;
import org.junit.Test;

public class BackPressureTrackerTest {

    private static final String WORKER_ID = "worker";

    @Test
    public void testGetBackpressure() {
        int taskIdNoBackPressure = 1;
        JCQueue noBackPressureQueue = mock(JCQueue.class);
        BackPressureTracker tracker = new BackPressureTracker(WORKER_ID,
                Collections.singletonMap(taskIdNoBackPressure, noBackPressureQueue),
                new StormMetricRegistry(),
                Collections.singletonMap(taskIdNoBackPressure, "testComponent"));

        BackPressureStatus status = tracker.getCurrStatus();

        assertThat(status.workerId, is(WORKER_ID));
        assertThat(status.nonBpTasks, contains(taskIdNoBackPressure));
        assertThat(status.bpTasks, is(empty()));
    }

    @Test
    public void testSetBackpressure() {
        int taskIdNoBackPressure = 1;
        JCQueue noBackPressureQueue = mock(JCQueue.class);
        int taskIdBackPressure = 2;
        JCQueue backPressureQueue = mock(JCQueue.class);
        BackPressureTracker tracker = new BackPressureTracker(WORKER_ID, ImmutableMap.of(
            taskIdNoBackPressure, noBackPressureQueue,
            taskIdBackPressure, backPressureQueue), new StormMetricRegistry(),
                ImmutableMap.of(
                        taskIdNoBackPressure, "NoBackPressureComponent",
                        taskIdBackPressure, "BackPressureComponent")
        );

        BackpressureState state = tracker.getBackpressureState(taskIdBackPressure);
        boolean backpressureChanged = tracker.recordBackPressure(state);
        BackPressureStatus status = tracker.getCurrStatus();

        assertThat(backpressureChanged, is(true));
        assertThat(status.workerId, is(WORKER_ID));
        assertThat(status.nonBpTasks, contains(taskIdNoBackPressure));
        assertThat(status.bpTasks, contains(taskIdBackPressure));
    }

    @Test
    public void testSetBackpressureWithExistingBackpressure() {
        int taskId = 1;
        JCQueue queue = mock(JCQueue.class);
        BackPressureTracker tracker = new BackPressureTracker(WORKER_ID, ImmutableMap.of(
            taskId, queue), new StormMetricRegistry(),
                ImmutableMap.of(taskId, "component-1"));
        BackpressureState state = tracker.getBackpressureState(taskId);
        tracker.recordBackPressure(state);

        boolean backpressureChanged = tracker.recordBackPressure(state);
        BackPressureStatus status = tracker.getCurrStatus();

        assertThat(backpressureChanged, is(false));
        assertThat(status.workerId, is(WORKER_ID));
        assertThat(status.bpTasks, contains(taskId));
    }

    @Test
    public void testRefreshBackpressureWithEmptyOverflow() {
        int taskId = 1;
        JCQueue queue = mock(JCQueue.class);
        when(queue.isEmptyOverflow()).thenReturn(true);
        BackPressureTracker tracker = new BackPressureTracker(WORKER_ID, ImmutableMap.of(
            taskId, queue), new StormMetricRegistry(),
                ImmutableMap.of(taskId, "component-1"));
        BackpressureState state = tracker.getBackpressureState(taskId);
        tracker.recordBackPressure(state);

        boolean backpressureChanged = tracker.refreshBpTaskList();
        BackPressureStatus status = tracker.getCurrStatus();

        assertThat(backpressureChanged, is(true));
        assertThat(status.workerId, is(WORKER_ID));
        assertThat(status.nonBpTasks, contains(taskId));
    }

    @Test
    public void testRefreshBackPressureWithNonEmptyOverflow() {
        int taskId = 1;
        JCQueue queue = mock(JCQueue.class);
        when(queue.isEmptyOverflow()).thenReturn(false);
        BackPressureTracker tracker = new BackPressureTracker(WORKER_ID, ImmutableMap.of(
            taskId, queue), new StormMetricRegistry(),
                ImmutableMap.of(taskId, "component-1"));
        BackpressureState state = tracker.getBackpressureState(taskId);
        tracker.recordBackPressure(state);

        boolean backpressureChanged = tracker.refreshBpTaskList();
        BackPressureStatus status = tracker.getCurrStatus();

        assertThat(backpressureChanged, is(false));
        assertThat(status.workerId, is(WORKER_ID));
        assertThat(status.bpTasks, contains(taskId));
    }

    @Test
    public void testSetLastOverflowCount() {
        int taskId = 1;
        int overflow = 5;
        JCQueue queue = mock(JCQueue.class);
        BackPressureTracker tracker = new BackPressureTracker(WORKER_ID, ImmutableMap.of(
            taskId, queue), new StormMetricRegistry(),
                ImmutableMap.of(taskId, "component-1"));
        BackpressureState state = tracker.getBackpressureState(taskId);
        tracker.recordBackPressure(state);
        tracker.setLastOverflowCount(state, overflow);

        BackpressureState retrievedState = tracker.getBackpressureState(taskId);
        int lastOverflowCount = tracker.getLastOverflowCount(retrievedState);

        assertThat(lastOverflowCount, is(overflow));
    }

}

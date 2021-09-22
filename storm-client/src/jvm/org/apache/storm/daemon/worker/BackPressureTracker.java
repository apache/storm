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
 * limitations under the License
 */

package org.apache.storm.daemon.worker;

import com.codahale.metrics.Gauge;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.storm.messaging.netty.BackPressureStatus;
import org.apache.storm.metrics2.StormMetricRegistry;
import org.apache.storm.shade.org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.storm.shade.org.apache.commons.lang.builder.ToStringStyle;
import org.apache.storm.utils.JCQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks the BackPressure status.
 */
public class BackPressureTracker {
    static final Logger LOG = LoggerFactory.getLogger(BackPressureTracker.class);
    private final Map<Integer, BackpressureState> tasks;
    private final String workerId;

    public BackPressureTracker(String workerId, Map<Integer, JCQueue> localTasksToQueues,
                               StormMetricRegistry metricRegistry, Map<Integer, String> taskToComponent) {
        this.workerId = workerId;
        this.tasks = localTasksToQueues.entrySet().stream()
            .collect(Collectors.toMap(
                entry -> entry.getKey(),
                entry -> new BackpressureState(entry.getValue(), entry.getKey(),
                        taskToComponent.get(entry.getKey()), metricRegistry)));
    }

    public BackpressureState getBackpressureState(Integer taskId) {
        return tasks.get(taskId);
    }

    private void recordNoBackPressure(BackpressureState state) {
        state.backpressure.set(false);
    }

    /**
     * Record BP for a task.
     *
     * <p>This is called by transferLocalBatch() on NettyWorker thread
     *
     * @return true if an update was recorded, false if taskId is already under BP
     */
    public boolean recordBackPressure(BackpressureState state) {
        return state.backpressure.getAndSet(true) == false;
    }

    // returns true if there was a change in the BP situation
    public boolean refreshBpTaskList() {
        boolean changed = false;
        LOG.debug("Running Back Pressure status change check");
        for (Entry<Integer, BackpressureState> entry : tasks.entrySet()) {
            BackpressureState state = entry.getValue();
            if (state.backpressure.get() && state.queue.isEmptyOverflow()) {
                recordNoBackPressure(state);
                changed = true;
            }
        }
        return changed;
    }

    public BackPressureStatus getCurrStatus() {
        ArrayList<Integer> bpTasks = new ArrayList<>(tasks.size());
        ArrayList<Integer> nonBpTasks = new ArrayList<>(tasks.size());

        for (Entry<Integer, BackpressureState> entry : tasks.entrySet()) {
            //System bolt is not a part of backpressure.
            if (entry.getKey() >= 0) {
                boolean backpressure = entry.getValue().backpressure.get();
                if (backpressure) {
                    bpTasks.add(entry.getKey());
                } else {
                    nonBpTasks.add(entry.getKey());
                }
            }
        }
        return new BackPressureStatus(workerId, bpTasks, nonBpTasks);
    }

    public int getLastOverflowCount(BackpressureState state) {
        return state.lastOverflowCount;
    }

    public void setLastOverflowCount(BackpressureState state, int value) {
        state.lastOverflowCount = value;
    }

    
    public static class BackpressureState {
        private final JCQueue queue;
        //No task is under backpressure initially
        private final AtomicBoolean backpressure = new AtomicBoolean(false);
        //The overflow count last time BP status was sent
        private int lastOverflowCount = 0;


        BackpressureState(JCQueue queue, Integer taskId, String componentId, StormMetricRegistry metricRegistry) {
            this.queue = queue;

            // System bolt is not a part of backpressure.
            if (taskId >= 0) {
                if (componentId == null) {
                    throw new RuntimeException("Missing componentId for task " + taskId);
                }

                Gauge<Integer> bpOverflowCount = new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        if (backpressure.get()) {
                            return Math.max(1, lastOverflowCount);
                        }
                        return 0;
                    }
                };
                metricRegistry.gauge("__backpressure-last-overflow-count", bpOverflowCount, componentId, taskId);
            }
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append(queue)
                .append(backpressure)
                .toString();
        }
    }
}

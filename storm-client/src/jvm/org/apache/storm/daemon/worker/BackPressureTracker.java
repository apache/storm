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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.storm.messaging.netty.BackPressureStatus;
import org.apache.storm.utils.JCQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

import java.util.stream.Collectors;
import org.apache.storm.shade.org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.storm.shade.org.apache.commons.lang.builder.ToStringStyle;

/***
 *   Tracks the BackPressure status.
 */
public class BackPressureTracker {
    static final Logger LOG = LoggerFactory.getLogger(BackPressureTracker.class);
    private final Map<Integer, BackpressureState> tasks;
    private final String workerId;

    public BackPressureTracker(String workerId, Map<Integer, JCQueue> localTasksToQueues) {
        this.workerId = workerId;
        this.tasks = localTasksToQueues.entrySet().stream()
            .collect(Collectors.toMap(
                entry -> entry.getKey(),
                entry -> new BackpressureState(entry.getValue())));
    }

    private void recordNoBackPressure(Integer taskId) {
        tasks.get(taskId).backpressure.set(false);
    }

    /***
     * Record BP for a task.
     * This is called by transferLocalBatch() on NettyWorker thread
     * @return true if an update was recorded, false if taskId is already under BP
     */
    public boolean recordBackPressure(Integer taskId) {
        return tasks.get(taskId).backpressure.getAndSet(true) == false;
    }

    // returns true if there was a change in the BP situation
    public boolean refreshBpTaskList() {
        boolean changed = false;
        LOG.debug("Running Back Pressure status change check");
        for (Entry<Integer, BackpressureState> entry : tasks.entrySet()) {
            BackpressureState state = entry.getValue();
            if (state.backpressure.get() && state.queue.isEmptyOverflow()) {
                recordNoBackPressure(entry.getKey());
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
    
    private static class BackpressureState {
        private final JCQueue queue;
        //No task is under backpressure initially
        private final AtomicBoolean backpressure = new AtomicBoolean(false);

        public BackpressureState(JCQueue queue) {
            this.queue = queue;
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

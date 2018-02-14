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
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.messaging.netty.BackPressureStatus;
import org.apache.storm.utils.JCQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.storm.Constants.SYSTEM_TASK_ID;

/***
 *   Tracks the BackPressure status using a Map<TaskId, JCQueue>.
 *   Special value NONE, is used to indicate that the task is not under BackPressure
 *   ConcurrentHashMap does not allow storing null values, so we use the special value NONE instead.
 */
public class BackPressureTracker {
    private static final JCQueue NONE =  new JCQueue ("NoneQ", 2, 0, 1, null) { };

    static final Logger LOG = LoggerFactory.getLogger(BackPressureTracker.class);

    private final Map<Integer, JCQueue> tasks = new ConcurrentHashMap<>(); // updates are more frequent than iteration
    private final String workerId;

    public BackPressureTracker(String workerId, List<Integer> allLocalTasks) {
        this.workerId = workerId;
        for (Integer taskId : allLocalTasks) {
            if(taskId != SYSTEM_TASK_ID) {
                tasks.put(taskId, NONE);  // all tasks are considered to be not under BP initially
            }
        }
    }

    private void recordNoBackPressure(Integer taskId) {
        tasks.put(taskId, NONE);
    }

    /***
     * Record BP for a task
     * This is called by transferLocalBatch() on NettyWorker thread
     * @return true if an update was recorded, false if taskId is already under BP
     */
    public boolean recordBackPressure(Integer taskId, JCQueue recvQ) {
        return tasks.put(taskId, recvQ) == NONE;
    }

    // returns true if there was a change in the BP situation
    public boolean refreshBpTaskList() {
        boolean changed = false;
        LOG.debug("Running Back Pressure status change check");
        for ( Entry<Integer, JCQueue> entry : tasks.entrySet()) {
            if (entry.getValue() != NONE  &&  entry.getValue().isEmptyOverflow()) {
                recordNoBackPressure(entry.getKey());
                changed = true;
            }
        }
        return changed;
    }

    public BackPressureStatus getCurrStatus() {
        ArrayList<Integer> bpTasks = new ArrayList<>(tasks.size());
        ArrayList<Integer> nonBpTasks = new ArrayList<>(tasks.size());

        for (Entry<Integer, JCQueue> entry : tasks.entrySet()) {
            JCQueue q = entry.getValue();
            if (q != NONE) {
                bpTasks.add(entry.getKey());
            } else {
                nonBpTasks.add(entry.getKey());
            }
        }
        return new BackPressureStatus(workerId, bpTasks, nonBpTasks);
    }
}

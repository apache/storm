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

import org.apache.storm.messaging.netty.BackPressureStatus;
import org.apache.storm.utils.JCQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.storm.Constants.SYSTEM_TASK_ID;

public class BackPressureTracker {
    static final Logger LOG = LoggerFactory.getLogger(BackPressureTracker.class);

    private Map<Integer, JCQueue> bpTasks = new ConcurrentHashMap<>(); // updates are more frequent than iteration
    private Set<Integer> nonBpTasks = ConcurrentHashMap.newKeySet();

    public BackPressureTracker(List<Integer> allLocalTasks) {
        nonBpTasks.addAll(allLocalTasks);    // all tasks are considered to be not under BP initially
        nonBpTasks.remove((int)SYSTEM_TASK_ID);   // not tracking system task
    }

    /* called by transferLocalBatch() on NettyWorker thread
     * returns true if an update was recorded, false if taskId is already under BP
     */
    public boolean recordBackpressure(Integer taskId, JCQueue recvQ) {
        if (nonBpTasks.remove(taskId)) {
            bpTasks.put(taskId, recvQ);
            return true;
        }
        return false;
    }

    // returns true if there was a change in the BP situation
    public boolean refreshBpTaskList() {
        boolean changed = false;
        LOG.debug("Running Back Pressure status change check");
        for (Iterator<Entry<Integer, JCQueue>> itr = bpTasks.entrySet().iterator(); itr.hasNext(); ) {
            Entry<Integer, JCQueue> entry = itr.next();
            if (entry.getValue().isEmptyOverflow()) {
                // move task from bpTasks to noBpTasks
                nonBpTasks.add(entry.getKey());
                itr.remove();
                changed = true;
            } else {
//                LOG.info("Task = {}, OverflowCount = {}, Q = {}", entry.getKey(), entry.getValue().getOverflowCount(), entry.getValue().getQueuedCount() );
            }
        }
        return changed;
    }

    public BackPressureStatus getCurrStatus() {
        ArrayList<Integer> bpTasksIds = new ArrayList<>(bpTasks.keySet());
        ArrayList<Integer> nonBpTasksIds = new ArrayList<>(nonBpTasks);
        return new BackPressureStatus(bpTasksIds, nonBpTasksIds);
    }
}

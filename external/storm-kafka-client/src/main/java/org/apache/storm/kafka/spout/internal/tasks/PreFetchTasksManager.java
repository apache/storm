/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.storm.kafka.spout.internal.tasks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Handles all tasks that must be performed before fetch can occur. In the case of the
 * KafkaBolt
 * may it be one time task, or recurrent tasks, for example, after a consumer rebalance
 */
public class PreFetchTasksManager {
    protected static final Logger LOG = LoggerFactory.getLogger(PreFetchTasksManager.class);

    private List<PreFetchTask> tasks;

    public PreFetchTasksManager() {
        this(new ArrayList<PreFetchTask>());
    }

    public PreFetchTasksManager(List<PreFetchTask> tasks) {
        this.tasks = tasks;
    }

    /**
     * @return the internal index of this task. Can be used to remove the task .
     */
    public int addTask(PreFetchTask task) {
        tasks.add(task);
        return tasks.size() - 1;
    }

    public void removeTask(int taskIndex) {
        tasks.remove(taskIndex);
    }

    public void startAll() {
        for (PreFetchTask task : tasks) {
            task.start();
        }
    }

    public boolean allCompleted() {
        boolean result = true;
        for (PreFetchTask task : tasks) {
            result &= task.isComplete();
        }
        LOG.debug("All prefetch tasks complete = {}", result);
        return result;
    }

    public void stopAll() {
        for (PreFetchTask task : tasks) {
            task.stop();
        }
    }

    public void closeAll() {
        for (PreFetchTask task : tasks) {
            task.close();
        }
    }

    public List<PreFetchTask> getTasks() {
        return Collections.unmodifiableList(tasks);
    }
}

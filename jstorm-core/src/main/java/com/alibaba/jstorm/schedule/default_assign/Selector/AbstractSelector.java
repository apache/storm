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
package com.alibaba.jstorm.schedule.default_assign.Selector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.schedule.default_assign.TaskAssignContext;

public abstract class AbstractSelector implements Selector {

    protected final TaskAssignContext context;

    protected WorkerComparator workerComparator;

    protected WorkerComparator supervisorComparator;

    public AbstractSelector(TaskAssignContext context) {
        this.context = context;
    }

    protected List<ResourceWorkerSlot> selectWorker(
            List<ResourceWorkerSlot> list, Comparator<ResourceWorkerSlot> c) {
        List<ResourceWorkerSlot> result = new ArrayList<ResourceWorkerSlot>();
        ResourceWorkerSlot best = null;
        for (ResourceWorkerSlot worker : list) {
            if (best == null) {
                best = worker;
                result.add(worker);
                continue;
            }
            if (c.compare(best, worker) == 0) {
                result.add(worker);
            } else if (c.compare(best, worker) > 0) {
                best = worker;
                result.clear();
                result.add(best);
            }
        }
        return result;
    }

    @Override
    public List<ResourceWorkerSlot> select(List<ResourceWorkerSlot> result,
            String name) {
        if (result.size() == 1)
            return result;
        result = this.selectWorker(result, workerComparator.get(name));
        if (result.size() == 1)
            return result;
        return this.selectWorker(result, supervisorComparator.get(name));
    }

}

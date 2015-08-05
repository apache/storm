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
package com.alibaba.jstorm.task.group;

import java.util.List;

import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.RandomRange;

public abstract class Shuffer {
    private WorkerData workerData;

    public Shuffer(WorkerData workerData) {
        this.workerData = workerData;
    }

    public abstract List<Integer> grouper(List<Object> values);

    protected int getActiveTask(RandomRange randomrange, List<Integer> outTasks) {
        int index = randomrange.nextInt();
        int size = outTasks.size();
        int i = 0;

        for (i = 0; i < size; i++) {
            if (workerData.isOutboundTaskActive(Integer.valueOf(outTasks
                    .get(index))))
                break;
            else
                index = randomrange.nextInt();
        }

        return (i < size ? index : -1);
    }
}
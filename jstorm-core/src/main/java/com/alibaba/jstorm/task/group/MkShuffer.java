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

public class MkShuffer extends Shuffer {

    private List<Integer> outTasks;
    private RandomRange randomrange;

    public MkShuffer(List<Integer> allOutTasks, WorkerData workerData) {
        super(workerData);

        outTasks = allOutTasks;
        randomrange = new RandomRange(outTasks.size());
    }

    public List<Integer> grouper(List<Object> values) {
        int index = getActiveTask(randomrange, outTasks);
        // If none active tasks were found, still send message to a task
        if (index == -1)
            index = randomrange.nextInt();

        return JStormUtils.mk_list(outTasks.get(index));
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

}

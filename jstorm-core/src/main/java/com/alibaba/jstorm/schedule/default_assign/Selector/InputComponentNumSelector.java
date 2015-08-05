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

import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.schedule.default_assign.TaskAssignContext;

public class InputComponentNumSelector extends AbstractSelector {

    public InputComponentNumSelector(final TaskAssignContext context) {
        super(context);
        // TODO Auto-generated constructor stub
        this.workerComparator = new WorkerComparator() {
            @Override
            public int compare(ResourceWorkerSlot o1, ResourceWorkerSlot o2) {
                // TODO Auto-generated method stub
                int o1Num = context.getInputComponentNumOnWorker(o1, name);
                int o2Num = context.getInputComponentNumOnWorker(o2, name);
                if (o1Num == o2Num)
                    return 0;
                return o1Num > o2Num ? -1 : 1;
            }
        };
        this.supervisorComparator = new WorkerComparator() {
            @Override
            public int compare(ResourceWorkerSlot o1, ResourceWorkerSlot o2) {
                // TODO Auto-generated method stub
                int o1Num =
                        context.getInputComponentNumOnSupervisor(
                                o1.getNodeId(), name);
                int o2Num =
                        context.getInputComponentNumOnSupervisor(
                                o2.getNodeId(), name);
                if (o1Num == o2Num)
                    return 0;
                return o1Num > o2Num ? -1 : 1;
            }
        };
    }
}

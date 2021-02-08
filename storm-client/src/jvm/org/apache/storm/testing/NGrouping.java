/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.testing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

public class NGrouping implements CustomStreamGrouping {
    int number;
    List<Integer> outTasks;

    public NGrouping(Integer n) {
        number = n;
    }

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        targetTasks = new ArrayList<Integer>(targetTasks);
        Collections.sort(targetTasks);
        outTasks = new ArrayList<Integer>();
        for (int i = 0; i < number; i++) {
            outTasks.add(targetTasks.get(i));
        }
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        return outTasks;
    }

}

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

package org.apache.storm.trident.partition;

import java.util.Arrays;
import java.util.List;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.utils.Utils;

public class IndexHashGrouping implements CustomStreamGrouping {
    int index;
    List<Integer> targets;

    public IndexHashGrouping(int index) {
        this.index = index;
    }

    public static int objectToIndex(Object val, int numPartitions) {
        if (val == null) {
            return 0;
        }
        return Utils.toPositive(val.hashCode()) % numPartitions;
    }

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        targets = targetTasks;
    }

    @Override
    public List<Integer> chooseTasks(int fromTask, List<Object> values) {
        int i = objectToIndex(values.get(index), targets.size());
        return Arrays.asList(targets.get(i));
    }

}

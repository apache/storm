/*
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

package org.apache.storm.grouping;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.task.WorkerTopologyContext;

public class LocalityAwareShuffleGrouping implements CustomStreamGrouping, Serializable {
    private ConcurrentMap<Integer, ConcurrentMap<Integer, Double>> taskNetworkDistance;
    private List<Integer> targetTasks;
    private Random rand;

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        rand = new Random();
        taskNetworkDistance = context.getTaskNetworkDistance();
        this.targetTasks = targetTasks;
    }

    /**
     * Choose the target task with the minimum distance.
     * If there are more than one task with the minimum distance, randomly pick one.
     * @param taskId The source taskId
     * @param values the values to group on
     * @return The chosen tasks
     */
    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        if (taskNetworkDistance == null || taskNetworkDistance.isEmpty() || !taskNetworkDistance.containsKey(taskId)) {
            int index = rand.nextInt(targetTasks.size());
            return new ArrayList<>(targetTasks.get(index));
        }

        ConcurrentMap<Integer, Double> taskIdToDistance = taskNetworkDistance.get(taskId);

        int sameDistanceAppearanceCount = 1;

        double minDistance = Double.MAX_VALUE;
        Integer closestTargetTask = targetTasks.get(0);
        for (int i = 0; i < targetTasks.size(); ++i) {
            Integer targetTaskId = targetTasks.get(i);
            double distance = taskIdToDistance.getOrDefault(targetTaskId, Double.MAX_VALUE);
            if (distance < minDistance) {
                closestTargetTask = targetTaskId;
                minDistance = distance;
                sameDistanceAppearanceCount = 1;
            } else if (distance == minDistance) {
                ++sameDistanceAppearanceCount;
                // accept this targetTask as the closest target task
                // with the probability of 1/sameDistanceAppearanceCount
                if (rand.nextFloat() < (1.0 / sameDistanceAppearanceCount)) {
                    closestTargetTask = targetTaskId;
                }
            }
        }

        return Arrays.asList(closestTargetTask);
    }

}

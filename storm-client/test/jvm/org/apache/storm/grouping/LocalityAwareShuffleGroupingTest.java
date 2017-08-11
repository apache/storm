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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.storm.task.WorkerTopologyContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LocalityAwareShuffleGroupingTest {

    ConcurrentMap<Integer, ConcurrentMap<Integer, Double>> taskNetworkDistance;
    WorkerTopologyContext workerTopologyContext;
    LocalityAwareShuffleGrouping localityAwareShuffleGrouping;
    List<Integer> targetTasks;

    @Before
    public void initialize() {
        taskNetworkDistance = new ConcurrentHashMap<>();
        workerTopologyContext = new WorkerTopologyContext(null, null, null, null, null, null,
                null, null, null, null, null, null, taskNetworkDistance);
        targetTasks = new ArrayList<>();
        localityAwareShuffleGrouping = new LocalityAwareShuffleGrouping();
    }

    @Test
    public void testChooseTasks() throws Exception {

        taskNetworkDistance.put(1, new ConcurrentHashMap<Integer, Double>(){{
            put(2, 10.0);
            put(3, 50.0);
            put(4, 5.0);
            put(5, 10.0);
        }});


        taskNetworkDistance.put(2, new ConcurrentHashMap<Integer, Double>(){{
            put(1, 10.0);
            put(3, 50.0);
            put(4, 50.0);
            put(5, 40.0);
        }});

        targetTasks.addAll(Arrays.asList(3, 4, 5, 6));

        localityAwareShuffleGrouping.prepare(workerTopologyContext, null, targetTasks);

        Assert.assertTrue(localityAwareShuffleGrouping.chooseTasks(1, null).get(0) == 4);
        Assert.assertTrue(localityAwareShuffleGrouping.chooseTasks(2, null).get(0) == 5);
    }

}
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
package org.apache.storm.grouping;

import com.google.common.collect.Lists;
import org.apache.storm.task.WorkerTopologyContext;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class ShuffleGroupingTest {

    /**
     * Tests that we round robbin correctly using ShuffleGrouping implementation.
     * */
    @Test
    public void testShuffleGrouping() {
        final int numTasks = 6;

        final ShuffleGrouping grouper = new ShuffleGrouping();

        // Task Id not used, so just pick a static value
        final int inputTaskId = 100;

        // Define our taskIds
        final List<Integer> availableTaskIds = Lists.newArrayList();
        for (int i = 0; i < numTasks; i++) {
            availableTaskIds.add(i);
        }
        WorkerTopologyContext context = mock(WorkerTopologyContext.class);
        grouper.prepare(context, null, availableTaskIds);

        // Keep track of how many times we see each taskId
        int[] taskCounts = new int[numTasks];
        for (int i = 1; i <= 30000; i++) {
            List<Integer> taskIds = grouper.chooseTasks(inputTaskId, Lists.newArrayList());

            // Validate a single task id return
            assertNotNull("Not null taskId list returned", taskIds);
            assertEquals("Single task Id returned", 1, taskIds.size());

            int taskId = taskIds.get(0);

            assertTrue("TaskId should exist", taskId >= 0 && taskId < numTasks);
            taskCounts[taskId]++;
        }

        for (int i = 0; i < numTasks; i++) {
            assertEquals("Distribution should be even for all nodes", 5000, taskCounts[i]);
        }
    }

    /**
     * Tests that we round robbin correctly with multiple threads using ShuffleGrouping implementation.
     */
    @Test
    public void testShuffleGroupMultiThreaded() throws InterruptedException, ExecutionException {
        final int numTasks = 6;
        final int groupingExecutionsPerThread = 30000;
        final int numThreads = 10;

        final CustomStreamGrouping grouper = new ShuffleGrouping();

        // Task Id not used, so just pick a static value
        final int inputTaskId = 100;
        // Define our taskIds - the test expects these to be incrementing by one up from zero
        final List<Integer> availableTaskIds = Lists.newArrayList();
        for (int i = 0; i < numTasks; i++) {
            availableTaskIds.add(i);
        }

        final WorkerTopologyContext context = mock(WorkerTopologyContext.class);

        // Call prepare with our available taskIds
        grouper.prepare(context, null, availableTaskIds);

        List<Callable<int[]>> threadTasks = Lists.newArrayList();
        for (int x=0; x < numThreads; x++) {
            Callable<int[]> threadTask = new Callable<int[]>() {
                @Override
                public int[] call() throws Exception {
                    int[] taskCounts = new int[availableTaskIds.size()];
                    for (int i = 1; i <= groupingExecutionsPerThread; i++) {
                        List<Integer> taskIds = grouper.chooseTasks(inputTaskId, Lists.newArrayList());

                        // Validate a single task id return
                        assertNotNull("Not null taskId list returned", taskIds);
                        assertEquals("Single task Id returned", 1, taskIds.size());

                        int taskId = taskIds.get(0);

                        assertTrue("TaskId should exist", taskId >= 0 && taskId < availableTaskIds.size());
                        taskCounts[taskId]++;
                    }
                    return taskCounts;
                }
            };

            // Add to our collection.
            threadTasks.add(threadTask);
        }

        ExecutorService executor = Executors.newFixedThreadPool(threadTasks.size());
        List<Future<int[]>> taskResults = executor.invokeAll(threadTasks);

        // Wait for all tasks to complete
        int[] taskIdTotals = new int[numTasks];
        for (Future taskResult: taskResults) {
            while (!taskResult.isDone()) {
                Thread.sleep(1000);
            }
            int[] taskDistributions = (int[]) taskResult.get();
            for (int i = 0; i < taskDistributions.length; i++) {
                taskIdTotals[i] += taskDistributions[i];
            }
        }

        for (int i = 0; i < numTasks; i++) {
            assertEquals(numThreads * groupingExecutionsPerThread / numTasks, taskIdTotals[i]);
        }
    }
}
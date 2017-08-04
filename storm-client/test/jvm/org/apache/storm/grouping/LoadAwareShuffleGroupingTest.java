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
import org.apache.storm.daemon.GrouperFactory;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.NullStruct;
import org.apache.storm.task.WorkerTopologyContext;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class LoadAwareShuffleGroupingTest {
    private static final Logger LOG = LoggerFactory.getLogger(LoadAwareShuffleGroupingTest.class);

    @Test
    public void testLoadAwareShuffleGroupingWithEvenLoad() {
        // just pick arbitrary number
        final int numTasks = 7;
        final LoadAwareShuffleGrouping grouper = new LoadAwareShuffleGrouping();

        // Define our taskIds and loads
        final List<Integer> availableTaskIds = getAvailableTaskIds(numTasks);
        final LoadMapping loadMapping = buildLocalTasksEvenLoadMapping(availableTaskIds);

        WorkerTopologyContext context = mock(WorkerTopologyContext.class);
        grouper.prepare(context, null, availableTaskIds);

        // Keep track of how many times we see each taskId
        // distribution should be even for all nodes when loads are even
        int desiredTaskCountPerTask = 5000;
        int totalEmits = numTasks * desiredTaskCountPerTask;

        int[] taskCounts = runChooseTasksWithVerification(grouper, totalEmits, numTasks, loadMapping);

        for (int i = 0; i < numTasks; i++) {
            assertEquals("Distribution should be even for all nodes", desiredTaskCountPerTask,
                taskCounts[i]);
        }
    }

    @Test
    public void testLoadAwareShuffleGroupingWithEvenLoadMultiThreaded() throws InterruptedException, ExecutionException {
        final int numTasks = 7;

        final LoadAwareShuffleGrouping grouper = new LoadAwareShuffleGrouping();

        // Task Id not used, so just pick a static value
        final int inputTaskId = 100;
        // Define our taskIds - the test expects these to be incrementing by one up from zero
        final List<Integer> availableTaskIds = getAvailableTaskIds(numTasks);
        final LoadMapping loadMapping = buildLocalTasksEvenLoadMapping(availableTaskIds);

        final WorkerTopologyContext context = mock(WorkerTopologyContext.class);

        // Call prepare with our available taskIds
        grouper.prepare(context, null, availableTaskIds);

        // triggers building ring
        grouper.chooseTasks(inputTaskId, Lists.newArrayList(), loadMapping);

        // calling chooseTasks should be finished before refreshing ring
        // adjusting groupingExecutionsPerThread might be needed with really slow machine
        // we allow race condition between refreshing ring and choosing tasks
        // so it will not make exact even distribution, though diff is expected to be small
        final int groupingExecutionsPerThread = numTasks * 5000;
        final int numThreads = 10;

        List<Callable<int[]>> threadTasks = Lists.newArrayList();
        for (int x = 0; x < numThreads; x++) {
            Callable<int[]> threadTask = new Callable<int[]>() {
                @Override
                public int[] call() throws Exception {
                    int[] taskCounts = new int[availableTaskIds.size()];
                    for (int i = 1; i <= groupingExecutionsPerThread; i++) {
                        List<Integer> taskIds = grouper.chooseTasks(inputTaskId,
                            Lists.newArrayList(), loadMapping);

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
            int expected = numThreads * groupingExecutionsPerThread / numTasks;
            assertEquals("Distribution should be even for all nodes", expected, taskIdTotals[i]);
        }
    }

    @Test
    public void testLoadAwareShuffleGroupingWithUnevenLoad() {
        // just pick arbitrary number
        final int numTasks = 7;
        final LoadAwareShuffleGrouping grouper = new LoadAwareShuffleGrouping();

        // Define our taskIds and loads
        final List<Integer> availableTaskIds = getAvailableTaskIds(numTasks);
        final LoadMapping loadMapping = buildLocalTasksUnevenLoadMapping(availableTaskIds);

        runDistributionVerificationTestWithUnevenLoad(numTasks, grouper, availableTaskIds,
            loadMapping);
    }

    @Test
    public void testLoadAwareShuffleGroupingWithRandomTasksAndRandomLoad() {
        for (int trial = 0 ; trial < 100 ; trial++) {
            // just pick arbitrary number in 5 ~ 100
            final int numTasks = new Random().nextInt(96) + 5;
            final LoadAwareShuffleGrouping grouper = new LoadAwareShuffleGrouping();

            // Define our taskIds and loads
            final List<Integer> availableTaskIds = getAvailableTaskIds(numTasks);
            final LoadMapping loadMapping = buildLocalTasksRandomLoadMapping(availableTaskIds);

            runDistributionVerificationTestWithUnevenLoad(numTasks, grouper, availableTaskIds,
                loadMapping);
        }
    }

    @Test
    public void testShuffleLoadEven() {
        // port test-shuffle-load-even
        LoadAwareCustomStreamGrouping shuffler = GrouperFactory
            .mkGrouper(null, "comp", "stream", null, Grouping.shuffle(new NullStruct()),
                Lists.newArrayList(1, 2), Collections.emptyMap());
        int numMessages = 100000;
        int minPrCount = (int) (numMessages * 0.49);
        int maxPrCount = (int) (numMessages * 0.51);
        LoadMapping load = new LoadMapping();
        Map<Integer, Double> loadInfoMap = new HashMap<>();
        loadInfoMap.put(1, 0.0);
        loadInfoMap.put(2, 0.0);
        load.setLocal(loadInfoMap);

        List<Object> data = Lists.newArrayList(1, 2);
        int[] frequencies = new int[3];
        for (int i = 0 ; i < numMessages ; i++) {
            List<Integer> tasks = shuffler.chooseTasks(1, data, load);
            for (int task : tasks) {
                frequencies[task]++;
            }
        }

        int load1 = frequencies[1];
        int load2 = frequencies[2];

        LOG.info("Frequency info: load1 = {}, load2 = {}", load1, load2);

        assertTrue(load1 >= minPrCount);
        assertTrue(load1 <= maxPrCount);
        assertTrue(load2 >= minPrCount);
        assertTrue(load2 <= maxPrCount);
    }

    @Test
    public void testShuffleLoadUneven() {
        // port test-shuffle-load-uneven
        LoadAwareCustomStreamGrouping shuffler = GrouperFactory
            .mkGrouper(null, "comp", "stream", null, Grouping.shuffle(new NullStruct()),
                Lists.newArrayList(1, 2), Collections.emptyMap());
        int numMessages = 100000;
        int min1PrCount = (int) (numMessages * 0.32);
        int max1PrCount = (int) (numMessages * 0.34);
        int min2PrCount = (int) (numMessages * 0.65);
        int max2PrCount = (int) (numMessages * 0.67);
        LoadMapping load = new LoadMapping();
        Map<Integer, Double> loadInfoMap = new HashMap<>();
        loadInfoMap.put(1, 0.5);
        loadInfoMap.put(2, 0.0);
        load.setLocal(loadInfoMap);

        List<Object> data = Lists.newArrayList(1, 2);
        int[] frequencies = new int[3]; // task id starts from 1
        for (int i = 0 ; i < numMessages ; i++) {
            List<Integer> tasks = shuffler.chooseTasks(1, data, load);
            for (int task : tasks) {
                frequencies[task]++;
            }
        }

        int load1 = frequencies[1];
        int load2 = frequencies[2];

        LOG.info("Frequency info: load1 = {}, load2 = {}", load1, load2);

        assertTrue(load1 >= min1PrCount);
        assertTrue(load1 <= max1PrCount);
        assertTrue(load2 >= min2PrCount);
        assertTrue(load2 <= max2PrCount);
    }

    private int[] runChooseTasksWithVerification(LoadAwareShuffleGrouping grouper, int totalEmits,
        int numTasks, LoadMapping loadMapping) {
        int[] taskCounts = new int[numTasks];

        // Task Id not used, so just pick a static value
        int inputTaskId = 100;

        // triggers building ring
        grouper.chooseTasks(inputTaskId, Lists.newArrayList(), loadMapping);

        for (int i = 1; i <= totalEmits; i++) {
            List<Integer> taskIds = grouper
                .chooseTasks(inputTaskId, Lists.newArrayList(), loadMapping);

            // Validate a single task id return
            assertNotNull("Not null taskId list returned", taskIds);
            assertEquals("Single task Id returned", 1, taskIds.size());

            int taskId = taskIds.get(0);

            assertTrue("TaskId should exist", taskId >= 0 && taskId < numTasks);
            taskCounts[taskId]++;
        }
        return taskCounts;
    }

    private void runDistributionVerificationTestWithUnevenLoad(int numTasks,
        LoadAwareShuffleGrouping grouper, List<Integer> availableTaskIds,
        LoadMapping loadMapping) {
        int[] loads = new int[numTasks];
        int localTotal = 0;
        List<Double> loadRate = new ArrayList<>();
        for (int i = 0; i < numTasks; i++) {
            int val = (int)(101 - (loadMapping.get(i) * 100));
            loads[i] = val;
            localTotal += val;
        }

        for (int i = 0; i < numTasks; i++) {
            loadRate.add(loads[i] * 1.0 / localTotal);
        }

        WorkerTopologyContext context = mock(WorkerTopologyContext.class);
        grouper.prepare(context, null, availableTaskIds);

        // Keep track of how many times we see each taskId
        int totalEmits = 5000 * numTasks;
        int[] taskCounts = runChooseTasksWithVerification(grouper, totalEmits, numTasks, loadMapping);

        int delta = (int) (totalEmits * 0.01);
        for (int i = 0; i < numTasks; i++) {
            int expected = (int) (totalEmits * loadRate.get(i));
            assertTrue("Distribution should respect the task load with small delta",
                taskCounts[i] >= expected - delta && taskCounts[i] <= expected + delta);
        }
    }

    @Ignore
    @Test
    public void testBenchmarkLoadAwareShuffleGroupingEvenLoad() {
        final int numTasks = 10;
        List<Integer> availableTaskIds = getAvailableTaskIds(numTasks);
        runSimpleBenchmark(new LoadAwareShuffleGrouping(), availableTaskIds,
            buildLocalTasksEvenLoadMapping(availableTaskIds));
    }

    @Ignore
    @Test
    public void testBenchmarkLoadAwareShuffleGroupingUnevenLoad() {
        final int numTasks = 10;
        List<Integer> availableTaskIds = getAvailableTaskIds(numTasks);
        runSimpleBenchmark(new LoadAwareShuffleGrouping(), availableTaskIds,
            buildLocalTasksUnevenLoadMapping(availableTaskIds));
    }

    private List<Integer> getAvailableTaskIds(int numTasks) {
        // this method should return sequential numbers starting at 0
        final List<Integer> availableTaskIds = Lists.newArrayList();
        for (int i = 0; i < numTasks; i++) {
            availableTaskIds.add(i);
        }
        return availableTaskIds;
    }

    private LoadMapping buildLocalTasksEvenLoadMapping(List<Integer> availableTasks) {
        LoadMapping loadMapping = new LoadMapping();
        Map<Integer, Double> localLoadMap = new HashMap<>(availableTasks.size());
        for (int i = 0; i < availableTasks.size(); i++) {
            localLoadMap.put(availableTasks.get(i), 0.1);
        }
        loadMapping.setLocal(localLoadMap);
        return loadMapping;
    }

    private LoadMapping buildLocalTasksUnevenLoadMapping(List<Integer> availableTasks) {
        LoadMapping loadMapping = new LoadMapping();
        Map<Integer, Double> localLoadMap = new HashMap<>(availableTasks.size());
        for (int i = 0; i < availableTasks.size(); i++) {
            localLoadMap.put(availableTasks.get(i), 0.1 * (i + 1));
        }
        loadMapping.setLocal(localLoadMap);
        return loadMapping;
    }

    private LoadMapping buildLocalTasksRandomLoadMapping(List<Integer> availableTasks) {
        LoadMapping loadMapping = new LoadMapping();
        Map<Integer, Double> localLoadMap = new HashMap<>(availableTasks.size());
        for (int i = 0; i < availableTasks.size(); i++) {
            localLoadMap.put(availableTasks.get(i), Math.random());
        }
        loadMapping.setLocal(localLoadMap);
        return loadMapping;
    }

    private void runSimpleBenchmark(LoadAwareCustomStreamGrouping grouper,
        List<Integer> availableTaskIds, LoadMapping loadMapping) {
        // Task Id not used, so just pick a static value
        final int inputTaskId = 100;

        WorkerTopologyContext context = mock(WorkerTopologyContext.class);
        grouper.prepare(context, null, availableTaskIds);

        // triggers building distribution ring
        grouper.chooseTasks(inputTaskId, Lists.newArrayList(), loadMapping);

        long current = System.currentTimeMillis();
        int idx = 0;
        while (true) {
            grouper.chooseTasks(inputTaskId, Lists.newArrayList(), loadMapping);

            idx++;
            if (idx % 100000 == 0) {
                // warm up 60 seconds
                if (System.currentTimeMillis() - current >= 60_000) {
                    break;
                }
            }
        }

        current = System.currentTimeMillis();
        for (int i = 1; i <= 2_000_000_000 ; i++) {
            grouper.chooseTasks(inputTaskId, Lists.newArrayList(), loadMapping);
        }

        System.out.println("Duration: " + (System.currentTimeMillis() - current) + " ms");
    }
}
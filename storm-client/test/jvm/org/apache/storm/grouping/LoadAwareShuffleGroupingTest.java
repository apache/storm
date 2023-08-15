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

package org.apache.storm.grouping;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.storm.Config;
import org.apache.storm.daemon.GrouperFactory;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.generated.NullStruct;
import org.apache.storm.shade.com.google.common.collect.Lists;
import org.apache.storm.shade.com.google.common.collect.Sets;
import org.apache.storm.shade.com.google.common.util.concurrent.MoreExecutors;
import org.apache.storm.task.WorkerTopologyContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LoadAwareShuffleGroupingTest {
    public static final double ACCEPTABLE_MARGIN = 0.015;
    private static final Logger LOG = LoggerFactory.getLogger(LoadAwareShuffleGroupingTest.class);

    private Map<String, Object> createConf() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_NETWORK_TOPOGRAPHY_PLUGIN, "org.apache.storm.networktopography.DefaultRackDNSToSwitchMapping");
        conf.put(Config.TOPOLOGY_LOCALITYAWARE_HIGHER_BOUND, 0.8);
        conf.put(Config.TOPOLOGY_LOCALITYAWARE_LOWER_BOUND, 0.2);
        return conf;
    }

    private WorkerTopologyContext mockContext(List<Integer> availableTaskIds) {
        WorkerTopologyContext context = mock(WorkerTopologyContext.class);
        when(context.getConf()).thenReturn(createConf());
        Map<Integer, NodeInfo> taskNodeToPort = new HashMap<>();
        NodeInfo nodeInfo = new NodeInfo("node-id", Sets.newHashSet(6700L));
        availableTaskIds.forEach(e -> taskNodeToPort.put(e, nodeInfo));
        when(context.getTaskToNodePort()).thenReturn(new AtomicReference<>(taskNodeToPort));
        when(context.getAssignmentId()).thenReturn("node-id");
        when(context.getThisWorkerPort()).thenReturn(6700);
        AtomicReference<Map<String, String>> nodeToHost = new AtomicReference<>(Collections.singletonMap("node-id", "hostname1"));
        when(context.getNodeToHost()).thenReturn(nodeToHost);
        return context;
    }

    @Test
    public void testUnevenLoadOverTime() {
        LoadAwareShuffleGrouping grouping = new LoadAwareShuffleGrouping();
        WorkerTopologyContext context = mockContext(Arrays.asList(1, 2));
        grouping.prepare(context, new GlobalStreamId("a", "default"), Arrays.asList(1, 2));
        double expectedOneWeight = 100.0;
        double expectedTwoWeight = 100.0;

        Map<Integer, Double> localLoad = new HashMap<>();
        localLoad.put(1, 1.0);
        localLoad.put(2, 0.0);
        LoadMapping lm = new LoadMapping();
        lm.setLocal(localLoad);
        //First verify that if something has a high load it's distribution will drop over time
        for (int i = 9; i >= 0; i--) {
            grouping.refreshLoad(lm);
            expectedOneWeight -= 10.0;
            Map<Integer, Double> countByType = count(grouping.choices, grouping.rets);
            LOG.info("contByType = {}", countByType);
            double expectedOnePercentage = expectedOneWeight / (expectedOneWeight + expectedTwoWeight);
            double expectedTwoPercentage = expectedTwoWeight / (expectedOneWeight + expectedTwoWeight);
            assertEquals(expectedOnePercentage,
                countByType.getOrDefault(1, 0.0) / grouping.getCapacity(),
                         0.01, "i = " + i);
            assertEquals(expectedTwoPercentage,
                countByType.getOrDefault(2, 0.0) / grouping.getCapacity(),
                0.01, "i = " + i);
        }

        //Now verify that when it is switched we can recover
        localLoad.put(1, 0.0);
        localLoad.put(2, 1.0);
        lm.setLocal(localLoad);

        while (expectedOneWeight < 100.0) {
            grouping.refreshLoad(lm);
            expectedOneWeight += 1.0;
            expectedTwoWeight = Math.max(0.0, expectedTwoWeight - 10.0);
            Map<Integer, Double> countByType = count(grouping.choices, grouping.rets);
            LOG.info("contByType = {}", countByType);
            double expectedOnePercentage = expectedOneWeight / (expectedOneWeight + expectedTwoWeight);
            double expectedTwoPercentage = expectedTwoWeight / (expectedOneWeight + expectedTwoWeight);
            assertEquals(expectedOnePercentage, countByType.getOrDefault(1, 0.0) / grouping.getCapacity(),
                         0.01);
            assertEquals(expectedTwoPercentage, countByType.getOrDefault(2, 0.0) / grouping.getCapacity(),
                         0.01);
        }
    }

    private Map<Integer, Double> count(int[] choices, List<Integer>[] rets) {
        Map<Integer, Double> ret = new HashMap<>();
        for (int i : choices) {
            int task = rets[i].get(0);
            ret.put(task, ret.getOrDefault(task, 0.0) + 1);
        }
        return ret;
    }

    @Test
    public void testLoadAwareShuffleGroupingWithEvenLoadWithManyTargets() {
        testLoadAwareShuffleGroupingWithEvenLoad(1000);
    }

    @Test
    public void testLoadAwareShuffleGroupingWithEvenLoadWithLessTargets() {
        testLoadAwareShuffleGroupingWithEvenLoad(7);
    }

    private void testLoadAwareShuffleGroupingWithEvenLoad(int numTasks) {
        final LoadAwareShuffleGrouping grouper = new LoadAwareShuffleGrouping();

        // Define our taskIds and loads
        final List<Integer> availableTaskIds = getAvailableTaskIds(numTasks);
        final LoadMapping loadMapping = buildLocalTasksEvenLoadMapping(availableTaskIds);

        final WorkerTopologyContext context = mockContext(availableTaskIds);
        grouper.prepare(context, null, availableTaskIds);

        // Keep track of how many times we see each taskId
        // distribution should be even for all nodes when loads are even
        int desiredTaskCountPerTask = 5000;
        int totalEmits = numTasks * desiredTaskCountPerTask;
        int minPrCount = (int) (totalEmits * ((1.0 / numTasks) - ACCEPTABLE_MARGIN));
        int maxPrCount = (int) (totalEmits * ((1.0 / numTasks) + ACCEPTABLE_MARGIN));

        int[] taskCounts = runChooseTasksWithVerification(grouper, totalEmits, numTasks, loadMapping);

        for (int i = 0; i < numTasks; i++) {
            assertTrue(taskCounts[i] >= minPrCount && taskCounts[i] <= maxPrCount,
                "Distribution should be even for all nodes with small delta");
        }
    }

    @Test
    public void testLoadAwareShuffleGroupingWithEvenLoadMultiThreadedWithManyTargets() throws ExecutionException, InterruptedException {
        testLoadAwareShuffleGroupingWithEvenLoadMultiThreaded(1000);
    }

    @Test
    public void testLoadAwareShuffleGroupingWithEvenLoadMultiThreadedWithLessTargets() throws ExecutionException, InterruptedException {
        testLoadAwareShuffleGroupingWithEvenLoadMultiThreaded(7);
    }

    private void testLoadAwareShuffleGroupingWithEvenLoadMultiThreaded(int numTasks) throws InterruptedException, ExecutionException {

        final LoadAwareShuffleGrouping grouper = new LoadAwareShuffleGrouping();

        // Task Id not used, so just pick a static value
        final int inputTaskId = 100;
        // Define our taskIds - the test expects these to be incrementing by one up from zero
        final List<Integer> availableTaskIds = getAvailableTaskIds(numTasks);
        final LoadMapping loadMapping = buildLocalTasksEvenLoadMapping(availableTaskIds);

        final WorkerTopologyContext context = mockContext(availableTaskIds);
        grouper.prepare(context, null, availableTaskIds);

        // force triggers building ring
        grouper.refreshLoad(loadMapping);

        // calling chooseTasks should be finished before refreshing ring
        // adjusting groupingExecutionsPerThread might be needed with really slow machine
        // we allow race condition between refreshing ring and choosing tasks
        // so it will not make exact even distribution, though diff is expected to be small
        // given that all threadTasks are finished before refreshing ring,
        // distribution should be exactly even
        final int groupingExecutionsPerThread = numTasks * 5000;
        final int numThreads = 10;
        int totalEmits = groupingExecutionsPerThread * numThreads;

        List<Callable<int[]>> threadTasks = Lists.newArrayList();
        for (int x = 0; x < numThreads; x++) {
            Callable<int[]> threadTask = () -> {
                int[] taskCounts = new int[availableTaskIds.size()];
                for (int i = 1; i <= groupingExecutionsPerThread; i++) {
                    List<Integer> taskIds = grouper.chooseTasks(inputTaskId, Lists.newArrayList());

                    // Validate a single task id return
                    assertNotNull(taskIds, "Not null taskId list returned");
                    assertEquals(1, taskIds.size(), "Single task Id not returned");

                    int taskId = taskIds.get(0);

                    assertTrue(taskId >= 0 && taskId < availableTaskIds.size(), "TaskId should exist");
                    taskCounts[taskId]++;
                }
                return taskCounts;
            };

            // Add to our collection.
            threadTasks.add(threadTask);
        }

        ExecutorService executor = Executors.newFixedThreadPool(threadTasks.size());
        List<Future<int[]>> taskResults = executor.invokeAll(threadTasks);

        // Wait for all tasks to complete
        int[] taskIdTotals = new int[numTasks];
        for (Future<int[]> taskResult : taskResults) {
            while (!taskResult.isDone()) {
                Thread.sleep(1000);
            }
            int[] taskDistributions = taskResult.get();
            for (int i = 0; i < taskDistributions.length; i++) {
                taskIdTotals[i] += taskDistributions[i];
            }
        }

        int minPrCount = (int) (totalEmits * ((1.0 / numTasks) - ACCEPTABLE_MARGIN));
        int maxPrCount = (int) (totalEmits * ((1.0 / numTasks) + ACCEPTABLE_MARGIN));

        for (int i = 0; i < numTasks; i++) {
            assertTrue(taskIdTotals[i] >= minPrCount && taskIdTotals[i] <= maxPrCount,
                "Distribution should be even for all nodes with small delta");
        }
    }

    @Test
    public void testShuffleLoadEven() {
        // port test-shuffle-load-even
        LoadAwareCustomStreamGrouping shuffler = GrouperFactory
            .mkGrouper(mockContext(Lists.newArrayList(1, 2)), "comp", "stream", null, Grouping.shuffle(new NullStruct()),
                       Lists.newArrayList(1, 2), Collections.emptyMap());
        int numMessages = 100000;
        int minPrCount = (int) (numMessages * (0.5 - ACCEPTABLE_MARGIN));
        int maxPrCount = (int) (numMessages * (0.5 + ACCEPTABLE_MARGIN));
        LoadMapping load = new LoadMapping();
        Map<Integer, Double> loadInfoMap = new HashMap<>();
        loadInfoMap.put(1, 0.0);
        loadInfoMap.put(2, 0.0);
        load.setLocal(loadInfoMap);

        // force triggers building ring
        shuffler.refreshLoad(load);

        List<Object> data = Lists.newArrayList(1, 2);
        int[] frequencies = new int[3];
        for (int i = 0; i < numMessages; i++) {
            List<Integer> tasks = shuffler.chooseTasks(1, data);
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

    @Disabled
    @Test
    public void testBenchmarkLoadAwareShuffleGroupingEvenLoad() {
        final int numTasks = 10;
        List<Integer> availableTaskIds = getAvailableTaskIds(numTasks);
        runSimpleBenchmark(new LoadAwareShuffleGrouping(), availableTaskIds,
                           buildLocalTasksEvenLoadMapping(availableTaskIds));
    }

    @Disabled
    @Test
    public void testBenchmarkLoadAwareShuffleGroupingUnevenLoad() {
        final int numTasks = 10;
        List<Integer> availableTaskIds = getAvailableTaskIds(numTasks);
        runSimpleBenchmark(new LoadAwareShuffleGrouping(), availableTaskIds,
                           buildLocalTasksUnevenLoadMapping(availableTaskIds));
    }

    @Disabled
    @Test
    public void testBenchmarkLoadAwareShuffleGroupingEvenLoadAndMultiThreaded()
        throws ExecutionException, InterruptedException {
        final int numTasks = 10;
        final int numThreads = 2;
        List<Integer> availableTaskIds = getAvailableTaskIds(numTasks);
        runMultithreadedBenchmark(new LoadAwareShuffleGrouping(), availableTaskIds,
                                  buildLocalTasksEvenLoadMapping(availableTaskIds), numThreads);
    }

    @Disabled
    @Test
    public void testBenchmarkLoadAwareShuffleGroupingUnevenLoadAndMultiThreaded()
        throws ExecutionException, InterruptedException {
        final int numTasks = 10;
        final int numThreads = 2;
        List<Integer> availableTaskIds = getAvailableTaskIds(numTasks);
        runMultithreadedBenchmark(new LoadAwareShuffleGrouping(), availableTaskIds,
                                  buildLocalTasksUnevenLoadMapping(availableTaskIds), numThreads);
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
        for (Integer availableTask : availableTasks) {
            localLoadMap.put(availableTask, 0.1);
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

    private int[] runChooseTasksWithVerification(LoadAwareShuffleGrouping grouper, int totalEmits,
                                                 int numTasks, LoadMapping loadMapping) {
        int[] taskCounts = new int[numTasks];

        // Task Id not used, so just pick a static value
        int inputTaskId = 100;

        // force triggers building ring
        grouper.refreshLoad(loadMapping);

        for (int i = 1; i <= totalEmits; i++) {
            List<Integer> taskIds = grouper
                .chooseTasks(inputTaskId, Lists.newArrayList());

            // Validate a single task id return
            assertNotNull(taskIds, "Not null taskId list returned");
            assertEquals(1, taskIds.size(), "Single task Id not returned");

            int taskId = taskIds.get(0);

            assertTrue(taskId >= 0 && taskId < numTasks, "TaskId should exist");
            taskCounts[taskId]++;
        }
        return taskCounts;
    }

    private void runSimpleBenchmark(LoadAwareCustomStreamGrouping grouper,
                                    List<Integer> availableTaskIds, LoadMapping loadMapping) {
        // Task Id not used, so just pick a static value
        final int inputTaskId = 100;

        WorkerTopologyContext context = mockContext(availableTaskIds);
        grouper.prepare(context, null, availableTaskIds);

        // periodically calls refreshLoad in 1 sec to simulate worker load update timer
        ScheduledExecutorService refreshService = MoreExecutors.getExitingScheduledExecutorService(
            new ScheduledThreadPoolExecutor(1));
        refreshService.scheduleAtFixedRate(() -> grouper.refreshLoad(loadMapping), 1, 1, TimeUnit.SECONDS);

        long current = System.currentTimeMillis();
        int idx = 0;
        while (true) {
            grouper.chooseTasks(inputTaskId, Lists.newArrayList());

            idx++;
            if (idx % 100000 == 0) {
                // warm up 60 seconds
                if (System.currentTimeMillis() - current >= 60_000) {
                    break;
                }
            }
        }

        current = System.currentTimeMillis();
        for (int i = 1; i <= 2_000_000_000; i++) {
            grouper.chooseTasks(inputTaskId, Lists.newArrayList());
        }

        LOG.info("Duration: {} ms", (System.currentTimeMillis() - current));

        refreshService.shutdownNow();
    }

    private void runMultithreadedBenchmark(LoadAwareCustomStreamGrouping grouper,
                                           List<Integer> availableTaskIds, LoadMapping loadMapping, int numThreads)
        throws InterruptedException, ExecutionException {
        // Task Id not used, so just pick a static value
        final int inputTaskId = 100;

        final WorkerTopologyContext context = mockContext(availableTaskIds);

        // Call prepare with our available taskIds
        grouper.prepare(context, null, availableTaskIds);

        // periodically calls refreshLoad in 1 sec to simulate worker load update timer
        ScheduledExecutorService refreshService = MoreExecutors.getExitingScheduledExecutorService(
            new ScheduledThreadPoolExecutor(1));
        refreshService.scheduleAtFixedRate(() -> grouper.refreshLoad(loadMapping), 1, 1, TimeUnit.SECONDS);

        long current = System.currentTimeMillis();
        int idx = 0;
        while (true) {
            grouper.chooseTasks(inputTaskId, Lists.newArrayList());

            idx++;
            if (idx % 100000 == 0) {
                // warm up 60 seconds
                if (System.currentTimeMillis() - current >= 60_000) {
                    break;
                }
            }
        }

        final int groupingExecutionsPerThread = 2_000_000_000;

        List<Callable<Long>> threadTasks = Lists.newArrayList();
        for (int x = 0; x < numThreads; x++) {
            Callable<Long> threadTask = () -> {
                long current1 = System.currentTimeMillis();
                for (int i = 1; i <= groupingExecutionsPerThread; i++) {
                    grouper.chooseTasks(inputTaskId, Lists.newArrayList());
                }
                return System.currentTimeMillis() - current1;
            };

            // Add to our collection.
            threadTasks.add(threadTask);
        }

        ExecutorService executor = Executors.newFixedThreadPool(threadTasks.size());
        List<Future<Long>> taskResults = executor.invokeAll(threadTasks);

        // Wait for all tasks to complete
        Long maxDurationMillis = 0L;
        for (Future taskResult : taskResults) {
            while (!taskResult.isDone()) {
                Thread.sleep(100);
            }
            Long durationMillis = (Long) taskResult.get();
            if (maxDurationMillis < durationMillis) {
                maxDurationMillis = durationMillis;
            }
        }

        LOG.info("Max duration among threads is : {} ms", maxDurationMillis);

        refreshService.shutdownNow();
    }

    @Test
    public void testLoadSwitching() {
        LoadAwareShuffleGrouping grouping = new LoadAwareShuffleGrouping();
        WorkerTopologyContext context = createLoadSwitchingContext();
        grouping.prepare(context, new GlobalStreamId("a", "default"), Arrays.asList(1, 2, 3));
        // startup should default to worker local
        assertEquals(LoadAwareShuffleGrouping.LocalityScope.WORKER_LOCAL, grouping.getCurrentScope());

        // with high load, switch to host local
        LoadMapping lm = createLoadMapping(1.0, 1.0, 1.0);
        grouping.refreshLoad(lm);
        assertEquals(LoadAwareShuffleGrouping.LocalityScope.HOST_LOCAL, grouping.getCurrentScope());

        // load remains high, switch to rack local
        grouping.refreshLoad(lm);
        assertEquals(LoadAwareShuffleGrouping.LocalityScope.RACK_LOCAL, grouping.getCurrentScope());

        // load remains high. switch to everything
        grouping.refreshLoad(lm);
        assertEquals(LoadAwareShuffleGrouping.LocalityScope.EVERYTHING, grouping.getCurrentScope());

        // lower load below low water threshold, but worker local load remains too high
        // should switch to rack local
        lm = createLoadMapping(0.2, 0.1, 0.1);
        grouping.refreshLoad(lm);
        assertEquals(LoadAwareShuffleGrouping.LocalityScope.RACK_LOCAL, grouping.getCurrentScope());

        // lower load continues, switch to host local
        grouping.refreshLoad(lm);
        assertEquals(LoadAwareShuffleGrouping.LocalityScope.HOST_LOCAL, grouping.getCurrentScope());

        // lower load continues, should NOT be able to switch to worker local yet
        grouping.refreshLoad(lm);
        assertEquals(LoadAwareShuffleGrouping.LocalityScope.HOST_LOCAL, grouping.getCurrentScope());

        // reduce load on local worker task, should switch to worker local
        lm = createLoadMapping(0.1, 0.1, 0.1);
        grouping.refreshLoad(lm);
        assertEquals(LoadAwareShuffleGrouping.LocalityScope.WORKER_LOCAL, grouping.getCurrentScope());
    }

    private LoadMapping createLoadMapping(double load1, double load2, double load3) {
        Map<Integer, Double> localLoad = new HashMap<>();
        localLoad.put(1, load1);
        localLoad.put(2, load2);
        localLoad.put(3, load3);
        LoadMapping lm = new LoadMapping();
        lm.setLocal(localLoad);
        return lm;
    }

    // creates a WorkerTopologyContext with 3 tasks, one worker local, one host local,
    // and one rack local
    private WorkerTopologyContext createLoadSwitchingContext() {
        WorkerTopologyContext context = mock(WorkerTopologyContext.class);
        when(context.getConf()).thenReturn(createConf());
        Map<Integer, NodeInfo> taskNodeToPort = new HashMap<>();

        // worker local task
        taskNodeToPort.put(1, new NodeInfo("node-id", Sets.newHashSet(6701L)));
        // node local task
        taskNodeToPort.put(2, new NodeInfo("node-id", Sets.newHashSet(6702L)));
        // rack local task
        taskNodeToPort.put(3, new NodeInfo("node-id2", Sets.newHashSet(6703L)));

        when(context.getTaskToNodePort()).thenReturn(new AtomicReference<>(taskNodeToPort));
        when(context.getAssignmentId()).thenReturn("node-id");
        when(context.getThisWorkerPort()).thenReturn(6701);

        Map<String, String> nodeToHost = new HashMap<>();
        nodeToHost.put("node-id", "hostname1");
        nodeToHost.put("node-id2", "hostname2");
        when(context.getNodeToHost()).thenReturn(new AtomicReference<>(nodeToHost));

        return context;
    }
}
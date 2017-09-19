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

import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.task.WorkerTopologyContext;

public class LoadAwareShuffleGrouping implements LoadAwareCustomStreamGrouping, Serializable {
    static final int CAPACITY = 1000;
    private static final int MAX_WEIGHT = 100;
    private static class IndexAndWeights {
        final int index;
        int weight;

        IndexAndWeights(int index) {
            this.index = index;
            weight = MAX_WEIGHT;
        }
    }

    private final Map<Integer, IndexAndWeights> orig = new HashMap<>();
    private Random random;
    @VisibleForTesting
    List<Integer>[] rets;
    @VisibleForTesting
    volatile int[] choices;
    private volatile int[] prepareChoices;
    private AtomicInteger current;

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        random = new Random();

        rets = (List<Integer>[]) new List<?>[targetTasks.size()];
        int i = 0;
        for (int target : targetTasks) {
            rets[i] = Arrays.asList(target);
            orig.put(target, new IndexAndWeights(i));
            i++;
        }

        // can't leave choices to be empty, so initiate it similar as ShuffleGrouping
        choices = new int[CAPACITY];

        current = new AtomicInteger(0);
        // allocate another array to be switched
        prepareChoices = new int[CAPACITY];
        updateRing(null);
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        int rightNow;
        while (true) {
            rightNow = current.incrementAndGet();
            if (rightNow < CAPACITY) {
                return rets[choices[rightNow]];
            } else if (rightNow == CAPACITY) {
                current.set(0);
                return rets[choices[0]];
            }
            //race condition with another thread, and we lost
            // try again
        }
    }

    @Override
    public void refreshLoad(LoadMapping loadMapping) {
        updateRing(loadMapping);
    }

    private synchronized void updateRing(LoadMapping load) {
        //We will adjust weights based off of the minimum load
        double min = load == null ? 0 : orig.keySet().stream().mapToDouble((key) -> load.get(key)).min().getAsDouble();
        for (Map.Entry<Integer, IndexAndWeights> target: orig.entrySet()) {
            IndexAndWeights val = target.getValue();
            double l = load == null ? 0.0 : load.get(target.getKey());
            if (l <= min + (0.05)) {
                //We assume that within 5% of the minimum congestion is still fine.
                //Not congested we grow (but slowly)
                val.weight = Math.min(MAX_WEIGHT, val.weight + 1);
            } else {
                //Congested we contract much more quickly
                val.weight = Math.max(0, val.weight - 10);
            }
        }
        //Now we need to build the array
        long weightSum = orig.values().stream().mapToLong((w) -> w.weight).sum();
        //Now we can calculate a percentage

        int currentIdx = 0;
        if (weightSum > 0) {
            for (IndexAndWeights indexAndWeights : orig.values()) {
                int count = (int) ((indexAndWeights.weight / (double) weightSum) * CAPACITY);
                for (int i = 0; i < count && currentIdx < CAPACITY; i++) {
                    prepareChoices[currentIdx] = indexAndWeights.index;
                    currentIdx++;
                }
            }

            //in case we didn't fill in enough
            for (; currentIdx < CAPACITY; currentIdx++) {
                prepareChoices[currentIdx] = prepareChoices[random.nextInt(currentIdx)];
            }
        } else {
            //This really should be impossible, because we go off of the min load, and inc anything within 5% of it.
            // But just to be sure it is never an issue, especially with float rounding etc.
            for (;currentIdx < CAPACITY; currentIdx++) {
                prepareChoices[currentIdx] = currentIdx % rets.length;
            }
        }

        shuffleArray(prepareChoices);

        // swapping two arrays
        int[] tempForSwap = choices;
        choices = prepareChoices;
        prepareChoices = tempForSwap;

        current.set(-1);
    }

    private void shuffleArray(int[] arr) {
        int size = arr.length;
        for (int i = size; i > 1; i--) {
            swap(arr, i - 1, random.nextInt(i));
        }
    }

    private void swap(int[] arr, int i, int j) {
        int tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }
}
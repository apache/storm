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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.ArrayUtils;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.task.WorkerTopologyContext;

public class LoadAwareShuffleGrouping implements LoadAwareCustomStreamGrouping, Serializable {
    private static final int CAPACITY = 1000;

    private Random random;
    private List<Integer>[] rets;
    private int[] targets;
    private int[] loads;
    private int[] unassigned;
    private int[] choices;
    private int[] prepareChoices;
    private AtomicInteger current;

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        random = new Random();

        rets = (List<Integer>[])new List<?>[targetTasks.size()];
        targets = new int[targetTasks.size()];
        for (int i = 0; i < targets.length; i++) {
            rets[i] = Arrays.asList(targetTasks.get(i));
            targets[i] = targetTasks.get(i);
        }

        // can't leave choices to be empty, so initiate it similar as ShuffleGrouping
        choices = new int[CAPACITY];

        for (int i = 0 ; i < CAPACITY ; i++) {
            choices[i] = i % rets.length;
        }

        shuffleArray(choices);
        current = new AtomicInteger(-1);

        // allocate another array to be switched
        prepareChoices = new int[CAPACITY];

        // allocating only once
        loads = new int[targets.length];
        unassigned = new int[targets.length];
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

    private void updateRing(LoadMapping load) {
        int localTotal = 0;
        for (int i = 0 ; i < targets.length; i++) {
            int val = (int)(101 - (load.get(targets[i]) * 100));
            loads[i] = val;
            localTotal += val;
        }

        int currentIdx = 0;
        int unassignedIdx = 0;
        for (int i = 0 ; i < loads.length ; i++) {
            if (currentIdx == CAPACITY) {
                break;
            }

            int loadForTask = loads[i];
            int amount = Math.round(loadForTask * 1.0f * CAPACITY / localTotal);
            // assign at least one for task
            if (amount == 0) {
                unassigned[unassignedIdx++] = i;
            }
            for (int j = 0; j < amount; j++) {
                if (currentIdx == CAPACITY) {
                    break;
                }

                prepareChoices[currentIdx++] = i;
            }
        }

        if (currentIdx < CAPACITY) {
            // if there're some rooms, give unassigned tasks a chance to be included
            // this should be really small amount, so just add them sequentially
            if (unassignedIdx > 0) {
                for (int i = currentIdx ; i < CAPACITY ; i++) {
                    prepareChoices[i] = unassigned[(i - currentIdx) % unassignedIdx];
                }
            } else {
                // just pick random
                for (int i = currentIdx ; i < CAPACITY ; i++) {
                    prepareChoices[i] = random.nextInt(loads.length);
                }
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

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
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.task.WorkerTopologyContext;

public class LoadAwareShuffleGrouping implements LoadAwareCustomStreamGrouping, Serializable {
    private static final int CAPACITY_TASK_MULTIPLICATION = 100;

    private Random random;
    private List<Integer>[] rets;
    private int[] targets;
    private ArrayList<List<Integer>> choices;
    private AtomicInteger current;
    private int actualCapacity = 0;

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        random = new Random();

        rets = (List<Integer>[])new List<?>[targetTasks.size()];
        targets = new int[targetTasks.size()];
        for (int i = 0; i < targets.length; i++) {
            rets[i] = Arrays.asList(targetTasks.get(i));
            targets[i] = targetTasks.get(i);
        }

        actualCapacity = targets.length * CAPACITY_TASK_MULTIPLICATION;

        // can't leave choices to be empty, so initiate it similar as ShuffleGrouping
        choices = new ArrayList<>(actualCapacity);
        int index = 0;
        while (index < actualCapacity) {
            choices.add(rets[index % targets.length]);
            index++;
        }

        Collections.shuffle(choices, random);
        current = new AtomicInteger(0);
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        throw new RuntimeException("NOT IMPLEMENTED");
    }

    @Override
    public void refreshLoad(LoadMapping loadMapping) {
        updateRing(loadMapping);
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values, LoadMapping load) {
        int rightNow;
        int size = choices.size();
        while (true) {
            rightNow = current.incrementAndGet();
            if (rightNow < size) {
                return choices.get(rightNow);
            } else if (rightNow == size) {
                current.set(0);
                return choices.get(0);
            }
            //race condition with another thread, and we lost
            // try again
        }
    }

    private void updateRing(LoadMapping load) {
        int localTotal = 0;
        int[] loads = new int[targets.length];
        for (int i = 0 ; i < targets.length; i++) {
            int val = (int)(101 - (load.get(targets[i]) * 100));
            loads[i] = val;
            localTotal += val;
        }

        // allocating enough memory doesn't hurt much, so assign aggressively
        // we will cut out if actual size becomes bigger than actualCapacity
        ArrayList<List<Integer>> newChoices = new ArrayList<>(actualCapacity + targets.length);
        for (int i = 0 ; i < loads.length ; i++) {
            int loadForTask = loads[i];
            int amount = loadForTask * actualCapacity / localTotal;
            // assign at least one for task
            if (amount == 0) {
                amount = 1;
            }
            for (int j = 0; j < amount; j++) {
                newChoices.add(rets[i]);
            }
        }

        Collections.shuffle(newChoices, random);

        // make sure length of newChoices is same as actualCapacity, like current choices
        // this ensures safety when requests and update occurs concurrently
        if (newChoices.size() > actualCapacity) {
            newChoices.subList(actualCapacity, newChoices.size()).clear();
        } else if (newChoices.size() < actualCapacity) {
            int remaining = actualCapacity - newChoices.size();
            while (remaining > 0) {
                newChoices.add(newChoices.get(remaining % newChoices.size()));
                remaining--;
            }
        }

        assert newChoices.size() == actualCapacity;

        choices = newChoices;
        current.set(0);
    }
}

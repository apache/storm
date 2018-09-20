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

package org.apache.storm.grouping.partialKeyGrouping;

import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.storm.grouping.PartialKeyGrouping;
import org.apache.storm.utils.Utils;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class BalancedTargetSelectorTest {


    private static final int[] TASK_LIST = { 9, 8, 7, 6 };

    private final PartialKeyGrouping.TargetSelector targetSelector = new PartialKeyGrouping.BalancedTargetSelector();

    @Test
    public void classIsSerializable() throws Exception {
        Utils.javaSerialize(targetSelector);
    }

    @Test
    public void selectorReturnsTasksInAssignment() {
        // select tasks once more than the number of tasks available
        for (int i = 0; i < TASK_LIST.length + 1; i++) {
            int selectedTask = targetSelector.chooseTask(TASK_LIST);
            assertThat(selectedTask, Matchers.in(Arrays.stream(TASK_LIST).boxed().collect(Collectors.toList())));
        }
    }

    @Test
    public void selectsTaskThatHasBeenUsedTheLeast() {
        // ensure that the first three tasks have been selected before
        targetSelector.chooseTask(new int[]{ TASK_LIST[0] });
        targetSelector.chooseTask(new int[]{ TASK_LIST[1] });
        targetSelector.chooseTask(new int[]{ TASK_LIST[2] });

        // now, selecting from the full set should cause the fourth task to be chosen.
        int selectedTask = targetSelector.chooseTask(TASK_LIST);
        assertThat(selectedTask, equalTo(TASK_LIST[3]));
    }

}

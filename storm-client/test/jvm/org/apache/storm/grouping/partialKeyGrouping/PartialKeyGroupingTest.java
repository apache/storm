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

import java.util.List;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.PartialKeyGrouping;
import org.apache.storm.shade.com.google.common.collect.Lists;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PartialKeyGroupingTest {

    @Test
    public void testGroupingIsSerializable() throws Exception {
        PartialKeyGrouping grouping = new PartialKeyGrouping(new Fields("some_field"));
        Utils.javaSerialize(grouping);
    }

    @Test
    public void testChooseTasks() {
        PartialKeyGrouping pkg = new PartialKeyGrouping();
        pkg.prepare(null, null, Lists.newArrayList(0, 1, 2, 3, 4, 5));
        Values message = new Values("key1");
        List<Integer> choice1 = pkg.chooseTasks(0, message);
        assertThat(choice1.size(), is(1));
        List<Integer> choice2 = pkg.chooseTasks(0, message);
        assertThat(choice2, is(not(choice1)));
        List<Integer> choice3 = pkg.chooseTasks(0, message);
        assertThat(choice3, is(not(choice2)));
        assertThat(choice3, is(choice1));
    }

    @Test
    public void testChooseTasksWithoutConsecutiveTaskIds() {
        PartialKeyGrouping pkg = new PartialKeyGrouping();
        pkg.prepare(null, null, Lists.newArrayList(9, 8, 7, 1, 2, 3));
        Values message = new Values("key1");
        List<Integer> choice1 = pkg.chooseTasks(0, message);
        assertThat(choice1.size(), is(1));
        List<Integer> choice2 = pkg.chooseTasks(0, message);
        assertThat(choice2, is(not(choice1)));
        List<Integer> choice3 = pkg.chooseTasks(0, message);
        assertThat(choice3, is(not(choice2)));
        assertThat(choice3, is(choice1));
    }

    @Test
    public void testChooseTasksFields() {
        PartialKeyGrouping pkg = new PartialKeyGrouping(new Fields("test"));
        WorkerTopologyContext context = mock(WorkerTopologyContext.class);
        when(context.getComponentOutputFields(any(GlobalStreamId.class))).thenReturn(new Fields("test"));
        pkg.prepare(context, mock(GlobalStreamId.class), Lists.newArrayList(0, 1, 2, 3, 4, 5));
        Values message = new Values("key1");
        List<Integer> choice1 = pkg.chooseTasks(0, message);
        assertThat(choice1.size(), is(1));
        List<Integer> choice2 = pkg.chooseTasks(0, message);
        assertThat(choice2, is(not(choice1)));
        List<Integer> choice3 = pkg.chooseTasks(0, message);
        assertThat(choice3, is(not(choice2)));
        assertThat(choice3, is(choice1));
    }
}

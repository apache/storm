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

import org.apache.storm.grouping.PartialKeyGrouping;
import org.apache.storm.shade.com.google.common.collect.Lists;
import org.apache.storm.utils.Utils;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

public class RandomTwoTaskAssignmentCreatorTest {

    private static final byte[] GROUPING_KEY_ONE = "some_key_one".getBytes();
    private static final byte[] GROUPING_KEY_TWO = "some_key_two".getBytes();

    @Test
    public void classIsSerializable() throws Exception {
        PartialKeyGrouping.AssignmentCreator assignmentCreator = new PartialKeyGrouping.RandomTwoTaskAssignmentCreator();
        Utils.javaSerialize(assignmentCreator);
    }

    @Test
    public void returnsAssignmentOfExpectedSize() {
        PartialKeyGrouping.AssignmentCreator assignmentCreator = new PartialKeyGrouping.RandomTwoTaskAssignmentCreator();
        int[] assignedTasks = assignmentCreator.createAssignment(Lists.newArrayList(9, 8, 7, 6), GROUPING_KEY_ONE);
        assertThat(assignedTasks.length, equalTo(2));
    }

    @Test
    public void returnsDifferentAssignmentForDifferentKeys() {
        PartialKeyGrouping.AssignmentCreator assignmentCreator = new PartialKeyGrouping.RandomTwoTaskAssignmentCreator();
        int[] assignmentOne = assignmentCreator.createAssignment(Lists.newArrayList(9, 8, 7, 6), GROUPING_KEY_ONE);
        int[] assignmentTwo = assignmentCreator.createAssignment(Lists.newArrayList(9, 8, 7, 6), GROUPING_KEY_TWO);
        assertThat(assignmentOne, not(equalTo(assignmentTwo)));
    }

    @Test
    public void returnsSameAssignmentForSameKey() {
        PartialKeyGrouping.AssignmentCreator assignmentCreator = new PartialKeyGrouping.RandomTwoTaskAssignmentCreator();
        int[] assignmentOne = assignmentCreator.createAssignment(Lists.newArrayList(9, 8, 7, 6), GROUPING_KEY_ONE);
        int[] assignmentOneAgain = assignmentCreator.createAssignment(Lists.newArrayList(9, 8, 7, 6), GROUPING_KEY_ONE);
        assertThat(assignmentOne, equalTo(assignmentOneAgain));
    }
}

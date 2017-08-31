package org.apache.storm.grouping.partialKeyGrouping;

import com.google.common.collect.Lists;
import org.apache.storm.grouping.PartialKeyGrouping;
import org.apache.storm.utils.Utils;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

public class HashingTwoTaskAssignmentCreatorTest {

    private static final byte[] GROUPING_KEY_ONE = "some_key_one".getBytes();
    private static final byte[] GROUPING_KEY_TWO = "some_key_two".getBytes();

    @Test
    public void classIsSerializable() throws Exception {
        PartialKeyGrouping.AssignmentCreator assignmentCreator = new PartialKeyGrouping.HashingTwoTaskAssignmentCreator();
        Utils.javaSerialize(assignmentCreator);
    }

    @Test
    public void returnsAssignmentOfExpectedSize() {
        PartialKeyGrouping.AssignmentCreator assignmentCreator = new PartialKeyGrouping.HashingTwoTaskAssignmentCreator();
        List<Integer> assignedTasks = assignmentCreator.createAssignment(Lists.newArrayList(9, 8, 7, 6), GROUPING_KEY_ONE);
        assertThat(assignedTasks.size(), equalTo(2));
    }

    @Test
    public void returnsDifferentAssignmentForDifferentKeys() {
        PartialKeyGrouping.AssignmentCreator assignmentCreator = new PartialKeyGrouping.HashingTwoTaskAssignmentCreator();
        List<Integer> assignmentOne = assignmentCreator.createAssignment(Lists.newArrayList(9, 8, 7, 6), GROUPING_KEY_ONE);
        List<Integer> assignmentTwo = assignmentCreator.createAssignment(Lists.newArrayList(9, 8, 7, 6), GROUPING_KEY_TWO);
        assertThat(assignmentOne, not(equalTo(assignmentTwo)));
    }

    @Test
    public void returnsSameAssignmentForSameKey() {
        PartialKeyGrouping.AssignmentCreator assignmentCreator = new PartialKeyGrouping.HashingTwoTaskAssignmentCreator();
        List<Integer> assignmentOne = assignmentCreator.createAssignment(Lists.newArrayList(9, 8, 7, 6), GROUPING_KEY_ONE);
        List<Integer> assignmentOneAgain = assignmentCreator.createAssignment(Lists.newArrayList(9, 8, 7, 6), GROUPING_KEY_ONE);
        assertThat(assignmentOne, equalTo(assignmentOneAgain));
    }
}

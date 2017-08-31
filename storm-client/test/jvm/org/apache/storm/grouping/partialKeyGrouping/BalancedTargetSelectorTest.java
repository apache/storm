package org.apache.storm.grouping.partialKeyGrouping;

import com.google.common.collect.Lists;
import org.apache.storm.grouping.PartialKeyGrouping;
import org.apache.storm.utils.Utils;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class BalancedTargetSelectorTest {


    private static final List<Integer> TASK_LIST = Lists.newArrayList(9, 8, 7, 6);

    private final PartialKeyGrouping.TargetSelector targetSelector = new PartialKeyGrouping.BalancedTargetSelector();

    @Test
    public void classIsSerializable() throws Exception {
        Utils.javaSerialize(targetSelector);
    }

    @Test
    public void selectorReturnsTasksInAssignment() {
        // select tasks once more than the number of tasks available
        for (int i = 0; i < TASK_LIST.size() + 1; i++) {
            int selectedTask = targetSelector.chooseTask(TASK_LIST);
            assertThat(selectedTask, Matchers.isIn(TASK_LIST));
        }
    }

    @Test
    public void selectsTaskThatHasBeenUsedTheLeast() {
        // ensure that the first three tasks have been selected before
        targetSelector.chooseTask(Lists.newArrayList(TASK_LIST.get(0)));
        targetSelector.chooseTask(Lists.newArrayList(TASK_LIST.get(1)));
        targetSelector.chooseTask(Lists.newArrayList(TASK_LIST.get(2)));

        // now, selecting from the full set should cause the fourth task to be chosen.
        int selectedTask = targetSelector.chooseTask(Lists.newArrayList(TASK_LIST));
        assertThat(selectedTask, equalTo(TASK_LIST.get(3)));
    }

}

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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.shade.com.google.common.collect.Maps;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tuple.Fields;

/**
 * A variation on FieldGrouping. This grouping operates on a partitioning of the incoming tuples (like a FieldGrouping),
 * but it can send Tuples from a given partition to multiple downstream tasks.
 *
 * <p>Given a total pool of target tasks, this grouping will always send Tuples with a given key to one member of a
 * subset of those tasks. Each key is assigned a subset of tasks. Each tuple is then sent to one task from that subset.
 *
 * <p>Notes: - the default TaskSelector ensures each task gets as close to a balanced number of Tuples as possible - the
 * default AssignmentCreator hashes the key and produces an assignment of two tasks
 */
public class PartialKeyGrouping implements CustomStreamGrouping, Serializable {
    private static final long serialVersionUID = -1672360572274911808L;
    private List<Integer> targetTasks;
    private Fields fields = null;
    private Fields outFields = null;

    private AssignmentCreator assignmentCreator;
    private TargetSelector targetSelector;

    public PartialKeyGrouping() {
        this(null);
    }

    public PartialKeyGrouping(Fields fields) {
        this(fields, new RandomTwoTaskAssignmentCreator(), new BalancedTargetSelector());
    }

    public PartialKeyGrouping(Fields fields, AssignmentCreator assignmentCreator) {
        this(fields, assignmentCreator, new BalancedTargetSelector());
    }

    public PartialKeyGrouping(Fields fields, AssignmentCreator assignmentCreator, TargetSelector targetSelector) {
        this.fields = fields;
        this.assignmentCreator = assignmentCreator;
        this.targetSelector = targetSelector;
    }

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        this.targetTasks = targetTasks;
        if (this.fields != null) {
            this.outFields = context.getComponentOutputFields(stream);
        }
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        List<Integer> boltIds = new ArrayList<>(1);
        if (values.size() > 0) {
            final byte[] rawKeyBytes = getKeyBytes(values);

            final int[] taskAssignmentForKey = assignmentCreator.createAssignment(this.targetTasks, rawKeyBytes);
            final int selectedTask = targetSelector.chooseTask(taskAssignmentForKey);

            boltIds.add(selectedTask);
        }
        return boltIds;
    }


    /**
     * Extract the key from the input Tuple.
     */
    private byte[] getKeyBytes(List<Object> values) {
        byte[] raw;
        if (fields != null) {
            List<Object> selectedFields = outFields.select(fields, values);
            ByteBuffer out = ByteBuffer.allocate(selectedFields.size() * 4);
            for (Object o : selectedFields) {
                if (o instanceof List) {
                    out.putInt(Arrays.deepHashCode(((List) o).toArray()));
                } else if (o instanceof Object[]) {
                    out.putInt(Arrays.deepHashCode((Object[]) o));
                } else if (o instanceof byte[]) {
                    out.putInt(Arrays.hashCode((byte[]) o));
                } else if (o instanceof short[]) {
                    out.putInt(Arrays.hashCode((short[]) o));
                } else if (o instanceof int[]) {
                    out.putInt(Arrays.hashCode((int[]) o));
                } else if (o instanceof long[]) {
                    out.putInt(Arrays.hashCode((long[]) o));
                } else if (o instanceof char[]) {
                    out.putInt(Arrays.hashCode((char[]) o));
                } else if (o instanceof float[]) {
                    out.putInt(Arrays.hashCode((float[]) o));
                } else if (o instanceof double[]) {
                    out.putInt(Arrays.hashCode((double[]) o));
                } else if (o instanceof boolean[]) {
                    out.putInt(Arrays.hashCode((boolean[]) o));
                } else if (o != null) {
                    out.putInt(o.hashCode());
                } else {
                    out.putInt(0);
                }
            }
            raw = out.array();
        } else {
            raw = values.get(0).toString().getBytes(); // assume key is the first field
        }
        return raw;
    }

    /*==================================================
     * Helper Classes
     *==================================================*/

    /**
     * This interface is responsible for choosing a subset of the target tasks to use for a given key.
     *
     * <p>NOTE: whatever scheme you use to create the assignment should be deterministic. This may be executed on
     * multiple Storm Workers, thus each of them needs to come up with the same assignment for a given key.
     */
    public interface AssignmentCreator extends Serializable {
        int[] createAssignment(List<Integer> targetTasks, byte[] key);
    }

    /**
     * This interface chooses one element from a task assignment to send a specific Tuple to.
     */
    public interface TargetSelector extends Serializable {
        Integer chooseTask(int[] assignedTasks);
    }

    /*========== Implementations ==========*/

    /**
     * This implementation of AssignmentCreator chooses two arbitrary tasks.
     */
    public static class RandomTwoTaskAssignmentCreator implements AssignmentCreator {
        /**
         * Creates a two task assignment by selecting random tasks.
         */
        @Override
        public int[] createAssignment(List<Integer> tasks, byte[] key) {
            // It is necessary that this produce a deterministic assignment based on the key, so seed the Random from the key
            final long seedForRandom = Arrays.hashCode(key);
            final Random random = new Random(seedForRandom);
            final int choice1 = random.nextInt(tasks.size());
            int choice2 = random.nextInt(tasks.size());
            // ensure that choice1 and choice2 are not the same task
            choice2 = choice1 == choice2 ? (choice2 + 1) % tasks.size() : choice2;
            return new int[]{ tasks.get(choice1), tasks.get(choice2) };
        }
    }

    /**
     * A basic implementation of target selection. This strategy chooses the task within the assignment that has received the fewest Tuples
     * overall from this instance of the grouping.
     */
    public static class BalancedTargetSelector implements TargetSelector {
        private Map<Integer, Long> targetTaskStats = Maps.newHashMap();

        /**
         * Chooses one of the incoming tasks and selects the one that has been selected the fewest times so far.
         */
        @Override
        public Integer chooseTask(int[] assignedTasks) {
            Integer taskIdWithMinLoad = null;
            Long minTaskLoad = Long.MAX_VALUE;

            for (Integer currentTaskId : assignedTasks) {
                final Long currentTaskLoad = targetTaskStats.getOrDefault(currentTaskId, 0L);
                if (currentTaskLoad < minTaskLoad) {
                    minTaskLoad = currentTaskLoad;
                    taskIdWithMinLoad = currentTaskId;
                }
            }

            targetTaskStats.put(taskIdWithMinLoad, targetTaskStats.getOrDefault(taskIdWithMinLoad, 0L) + 1);
            return taskIdWithMinLoad;
        }
    }
}
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
package com.alibaba.jstorm.schedule.default_assign;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.schedule.TopologyAssignContext;
import com.alibaba.jstorm.schedule.default_assign.Selector.ComponentNumSelector;
import com.alibaba.jstorm.schedule.default_assign.Selector.InputComponentNumSelector;
import com.alibaba.jstorm.schedule.default_assign.Selector.Selector;
import com.alibaba.jstorm.schedule.default_assign.Selector.TotalTaskNumSelector;
import com.alibaba.jstorm.utils.FailedAssignTopologyException;

public class TaskScheduler {

    public static Logger LOG = LoggerFactory.getLogger(TaskScheduler.class);

    public static final String ACKER_NAME = "__acker";

    private final TaskAssignContext taskContext;

    private List<ResourceWorkerSlot> assignments =
            new ArrayList<ResourceWorkerSlot>();

    private int workerNum;

    /**
     * For balance purpose, default scheduler is trying to assign the same
     * number of tasks to a worker. e.g. There are 4 tasks and 3 available
     * workers. Each worker will be assigned one task first. And then one worker
     * is chosen for the last one.
     */
    private int avgTaskNum;
    private int leftTaskNum;

    private Set<Integer> tasks;

    private DefaultTopologyAssignContext context;

    private Selector componentSelector;

    private Selector inputComponentSelector;

    private Selector totalTaskNumSelector;

    public TaskScheduler(DefaultTopologyAssignContext context,
            Set<Integer> tasks, List<ResourceWorkerSlot> workers) {
        this.tasks = tasks;
        LOG.info("Tasks " + tasks + " is going to be assigned in workers "
                + workers);
        this.context = context;
        this.taskContext =
                new TaskAssignContext(this.buildSupervisorToWorker(workers),
                        Common.buildSpoutOutoputAndBoltInputMap(context));
        this.componentSelector = new ComponentNumSelector(taskContext);
        this.inputComponentSelector =
                new InputComponentNumSelector(taskContext);
        this.totalTaskNumSelector = new TotalTaskNumSelector(taskContext);
        if (tasks.size() == 0)
            return;
        setTaskNum(tasks.size(), workerNum);

        // For Scale-out case, the old assignment should be kept.
        if (context.getAssignType() == TopologyAssignContext.ASSIGN_TYPE_REBALANCE
                && context.isReassign() == false) {
            keepAssignment(context.getOldAssignment().getWorkers());
        }
    }

    private void keepAssignment(Set<ResourceWorkerSlot> keepAssignments) {
        Set<Integer> keepTasks = new HashSet<Integer>();
        for (ResourceWorkerSlot worker : keepAssignments) {
            for (Integer taskId : worker.getTasks()) {
                if (tasks.contains(taskId)) {
                    ResourceWorkerSlot contextWorker =
                            taskContext.getWorker(worker);
                    if (contextWorker != null) {
                        String componentName =
                                context.getTaskToComponent().get(taskId);
                        updateAssignedTasksOfWorker(taskId, contextWorker);
                        updateComponentsNumOfWorker(componentName,
                                contextWorker);
                        keepTasks.add(taskId);
                    }
                }
            }
        }

        // Try to find the workers which have been assigned too much tasks
        // If found, remove the workers from worker resource pool and update
        // the avgNum and leftNum
        int doneAssignedTaskNum = 0;
        while (true) {
            boolean found = false;
            Set<ResourceWorkerSlot> doneAssignedWorkers =
                    new HashSet<ResourceWorkerSlot>();
            for (ResourceWorkerSlot worker : keepAssignments) {
                ResourceWorkerSlot contextWorker =
                        taskContext.getWorker(worker);
                if (contextWorker != null && isTaskFullForWorker(contextWorker)) {
                    found = true;
                    workerNum--;
                    taskContext.getWorkerToTaskNum().remove(contextWorker);
                    assignments.add(contextWorker);

                    doneAssignedWorkers.add(worker);
                    doneAssignedTaskNum += contextWorker.getTasks().size();
                }
            }

            if (found) {
                setTaskNum((tasks.size() - doneAssignedTaskNum), workerNum);
                keepAssignments.removeAll(doneAssignedWorkers);
            } else {
                break;
            }
        }
        tasks.removeAll(keepTasks);
        LOG.info("keep following assignment, " + assignments);
    }

    private boolean isTaskFullForWorker(ResourceWorkerSlot worker) {
        boolean ret = false;
        Set<Integer> tasks = worker.getTasks();

        if (tasks != null) {
            if ((leftTaskNum == 0 && tasks.size() >= avgTaskNum)
                    || (leftTaskNum > 0 && tasks.size() >= (avgTaskNum + 1))) {
                ret = true;
            }
        }
        return ret;
    }

    public List<ResourceWorkerSlot> assign() {
        if (tasks.size() == 0)
            return assignments;

        // Firstly, assign workers to the components which are configured
        // by "task.on.differ.node"
        Set<Integer> assignedTasks = assignForDifferNodeTask();

        // Assign for the tasks except acker
        tasks.removeAll(assignedTasks);
        Set<Integer> ackers = new HashSet<Integer>();
        for (Integer task : tasks) {
            String name = context.getTaskToComponent().get(task);
            if (name.equals(TaskScheduler.ACKER_NAME)) {
                ackers.add(task);
                continue;
            }
            assignForTask(name, task);
        }

        // At last, make the assignment for acker
        for (Integer task : ackers) {
            assignForTask(TaskScheduler.ACKER_NAME, task);
        }
        return assignments;
    }

    private void assignForTask(String name, Integer task) {
        ResourceWorkerSlot worker =
                chooseWorker(name, new ArrayList<ResourceWorkerSlot>(
                        taskContext.getWorkerToTaskNum().keySet()));
        pushTaskToWorker(task, name, worker);
    }

    private Set<Integer> assignForDifferNodeTask() {
        Set<Integer> ret = new HashSet<Integer>();
        for (Integer task : tasks) {
            Map conf = Common.getComponentMap(context, task);
            if (ConfigExtension.isTaskOnDifferentNode(conf))
                ret.add(task);
        }
        for (Integer task : ret) {
            String name = context.getTaskToComponent().get(task);
            ResourceWorkerSlot worker =
                    chooseWorker(name, getDifferNodeTaskWokers(name));
            pushTaskToWorker(task, name, worker);
        }
        return ret;
    }

    private Map<String, List<ResourceWorkerSlot>> buildSupervisorToWorker(
            List<ResourceWorkerSlot> workers) {
        Map<String, List<ResourceWorkerSlot>> supervisorToWorker =
                new HashMap<String, List<ResourceWorkerSlot>>();
        for (ResourceWorkerSlot worker : workers) {
            if (worker.getTasks() == null || worker.getTasks().size() == 0) {
                List<ResourceWorkerSlot> supervisor =
                        supervisorToWorker.get(worker.getNodeId());
                if (supervisor == null) {
                    supervisor = new ArrayList<ResourceWorkerSlot>();
                    supervisorToWorker.put(worker.getNodeId(), supervisor);
                }
                supervisor.add(worker);
            } else {
                assignments.add(worker);
            }
        }
        this.workerNum = workers.size() - assignments.size();
        return supervisorToWorker;
    }

    private ResourceWorkerSlot chooseWorker(String name,
            List<ResourceWorkerSlot> workers) {
        List<ResourceWorkerSlot> result =
                componentSelector.select(workers, name);
        result = totalTaskNumSelector.select(result, name);
        if (name.equals(TaskScheduler.ACKER_NAME))
            return result.iterator().next();
        result = inputComponentSelector.select(result, name);
        return result.iterator().next();
    }

    private void pushTaskToWorker(Integer task, String name,
            ResourceWorkerSlot worker) {
        LOG.debug("Push task-" + task + " to worker-" + worker.getPort());
        int taskNum = updateAssignedTasksOfWorker(task, worker);

        if (leftTaskNum <= 0) {
            if (taskNum == avgTaskNum) {
                taskContext.getWorkerToTaskNum().remove(worker);
                assignments.add(worker);
            }
        } else {
            if (taskNum == (avgTaskNum + 1)) {
                taskContext.getWorkerToTaskNum().remove(worker);
                leftTaskNum--;
                assignments.add(worker);
            }
            if (leftTaskNum <= 0) {
                List<ResourceWorkerSlot> needDelete =
                        new ArrayList<ResourceWorkerSlot>();
                for (Entry<ResourceWorkerSlot, Integer> entry : taskContext
                        .getWorkerToTaskNum().entrySet()) {
                    if (entry.getValue() == avgTaskNum)
                        needDelete.add(entry.getKey());
                }
                for (ResourceWorkerSlot workerToDelete : needDelete) {
                    taskContext.getWorkerToTaskNum().remove(workerToDelete);
                    assignments.add(workerToDelete);
                }
            }
        }

        updateComponentsNumOfWorker(name, worker);
    }

    private int updateAssignedTasksOfWorker(Integer task,
            ResourceWorkerSlot worker) {
        int ret = 0;
        Set<Integer> tasks = worker.getTasks();
        if (tasks == null) {
            tasks = new HashSet<Integer>();
            worker.setTasks(tasks);
        }
        tasks.add(task);

        ret = taskContext.getWorkerToTaskNum().get(worker);
        taskContext.getWorkerToTaskNum().put(worker, ++ret);
        return ret;
    }

    private void updateComponentsNumOfWorker(String name,
            ResourceWorkerSlot worker) {
        Map<String, Integer> components =
                taskContext.getWorkerToComponentNum().get(worker);
        if (components == null) {
            components = new HashMap<String, Integer>();
            taskContext.getWorkerToComponentNum().put(worker, components);
        }
        Integer componentNum = components.get(name);
        if (componentNum == null) {
            componentNum = 0;
        }
        components.put(name, ++componentNum);
    }

    private void setTaskNum(int taskNum, int workerNum) {
        if (taskNum >= 0 && workerNum > 0) {
            this.avgTaskNum = taskNum / workerNum;
            this.leftTaskNum = taskNum % workerNum;
            LOG.debug("avgTaskNum=" + avgTaskNum + ", leftTaskNum="
                    + leftTaskNum);
        } else {
            LOG.debug("Illegal parameters, taskNum=" + taskNum + ", workerNum="
                    + workerNum);
        }
    }

    private List<ResourceWorkerSlot> getDifferNodeTaskWokers(String name) {
        List<ResourceWorkerSlot> workers = new ArrayList<ResourceWorkerSlot>();
        workers.addAll(taskContext.getWorkerToTaskNum().keySet());

        for (Entry<String, List<ResourceWorkerSlot>> entry : taskContext
                .getSupervisorToWorker().entrySet()) {
            if (taskContext.getComponentNumOnSupervisor(entry.getKey(), name) != 0)
                workers.removeAll(entry.getValue());
        }
        if (workers.size() == 0)
            throw new FailedAssignTopologyException(
                    "there's no enough supervisor for making component: "
                            + name + " 's tasks on different node");
        return workers;
    }
}

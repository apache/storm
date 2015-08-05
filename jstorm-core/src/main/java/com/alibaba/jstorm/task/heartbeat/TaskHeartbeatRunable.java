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
package com.alibaba.jstorm.task.heartbeat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.schedule.Assignment.AssignmentType;
import com.alibaba.jstorm.task.Task;
import com.alibaba.jstorm.task.UptimeComputer;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeUtils;

/**
 * Task hearbeat
 * 
 * @author yannian
 * 
 */
public class TaskHeartbeatRunable extends RunnableCallback {
    private static final Logger LOG = LoggerFactory
            .getLogger(TaskHeartbeatRunable.class);

    private StormClusterState zkCluster;
    private String topology_id;
    private UptimeComputer uptime;
    private Map storm_conf;
    private Integer frequence;
    private Map<Integer, Long> taskAssignTsMap = new HashMap<Integer, Long>();

    private static Map<Integer, TaskStats> taskStatsMap =
            new HashMap<Integer, TaskStats>();
    private static LinkedBlockingDeque<Event> eventQueue =
            new LinkedBlockingDeque<TaskHeartbeatRunable.Event>();

    public static void registerTaskStats(int taskId, TaskStats taskStats) {
        Event event = new Event(Event.REGISTER_TYPE, taskId, taskStats);
        eventQueue.offer(event);
    }

    public static void unregisterTaskStats(int taskId) {
        Event event = new Event(Event.UNREGISTER_TYPE, taskId, null);
        eventQueue.offer(event);
    }

    public static void updateTaskHbStats(int taskId, Task taskData) {
        Event event = new Event(Event.TASK_HEARTBEAT_TYPE, taskId, taskData);
        eventQueue.offer(event);
    }

    public TaskHeartbeatRunable(WorkerData workerData) {

        this.zkCluster = workerData.getZkCluster();
        this.topology_id = workerData.getTopologyId();
        this.uptime = new UptimeComputer();
        this.storm_conf = workerData.getStormConf();

        String key = Config.TASK_HEARTBEAT_FREQUENCY_SECS;
        Object time = storm_conf.get(key);
        frequence = JStormUtils.parseInt(time, 10);

    }

    public void handle() throws InterruptedException {
        Event event = eventQueue.take();
        while (event != null) {
            switch (event.getType()) {
            case Event.TASK_HEARTBEAT_TYPE: {
                updateTaskHbStats(event);
                break;
            }
            case Event.REGISTER_TYPE: {
                Event<TaskStats> regEvent = event;
                taskStatsMap.put(event.getTaskId(), regEvent.getEventValue());
                taskAssignTsMap.put(event.getTaskId(),
                        System.currentTimeMillis());
                break;
            }
            case Event.UNREGISTER_TYPE: {
                taskStatsMap.remove(event.getTaskId());
                taskAssignTsMap.remove(event.getTaskId());
                break;
            }
            default: {
                LOG.warn("Unknown event type received:" + event.getType());
                break;
            }
            }

            event = eventQueue.take();
        }
    }

    @Override
    public void run() {
        try {
            handle();
        } catch (InterruptedException e) {
            LOG.info(e.getMessage());
        }
    }

    @Override
    public Object getResult() {
        return frequence;
    }

    public void updateTaskHbStats(Event event) {
        Integer currtime = TimeUtils.current_time_secs();
        Event<Task> taskHbEvent = event;
        int taskId = taskHbEvent.getTaskId();
        String idStr = " " + topology_id + ":" + taskId + " ";

        try {

            TaskHeartbeat hb = new TaskHeartbeat(currtime, uptime.uptime());
            zkCluster.task_heartbeat(topology_id, taskId, hb);

            LOG.info("update task hearbeat ts " + currtime + " for" + idStr);

            // Check if assignment is changed. e.g scale-out
            Task task = taskHbEvent.getEventValue();
            Long timeStamp = taskAssignTsMap.get(taskId);
            if (timeStamp != null) {
                if (timeStamp < task.getWorkerAssignmentTs() && 
                        task.getWorkerAssignmentType().equals(AssignmentType.Assign)) {
                    LOG.info("Start to update the task data for task-" + taskId);
                    task.updateTaskData();
                    taskAssignTsMap.put(taskId, task.getWorkerAssignmentTs());
                }
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            String errMsg = "Failed to update heartbeat to ZK " + idStr + "\n";
            LOG.error(errMsg, e);
        }
    }

    private static class Event<T> {
        public static final int REGISTER_TYPE = 0;
        public static final int UNREGISTER_TYPE = 1;
        public static final int TASK_HEARTBEAT_TYPE = 2;
        private final int type;
        private final int taskId;
        private final T value;

        public Event(int type, int taskId, T value) {
            this.type = type;
            this.taskId = taskId;
            this.value = value;
        }

        public int getType() {
            return type;
        }

        public int getTaskId() {
            return taskId;
        }

        public T getEventValue() {
            return value;
        }

    }

}

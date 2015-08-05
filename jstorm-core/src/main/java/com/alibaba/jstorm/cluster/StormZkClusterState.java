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
package com.alibaba.jstorm.cluster;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.Utils;

import com.alibaba.jstorm.cache.JStormCache;
import com.alibaba.jstorm.callback.ClusterStateCallback;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.common.metric.QueueGauge;
import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.schedule.AssignmentBak;
import com.alibaba.jstorm.task.TaskInfo;
import com.alibaba.jstorm.task.error.TaskError;
import com.alibaba.jstorm.task.heartbeat.TaskHeartbeat;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.PathUtils;
import com.alibaba.jstorm.utils.TimeUtils;

public class StormZkClusterState implements StormClusterState {
    private static Logger LOG = LoggerFactory
            .getLogger(StormZkClusterState.class);

    private ClusterState cluster_state;

    private ConcurrentHashMap<String, RunnableCallback> assignment_info_callback;
    private AtomicReference<RunnableCallback> supervisors_callback;
    private AtomicReference<RunnableCallback> assignments_callback;
    private ConcurrentHashMap<String, RunnableCallback> storm_base_callback;
    private AtomicReference<RunnableCallback> master_callback;

    private UUID state_id;

    private boolean solo;

    public StormZkClusterState(Object cluster_state_spec) throws Exception {

        if (cluster_state_spec instanceof ClusterState) {
            solo = false;
            cluster_state = (ClusterState) cluster_state_spec;
        } else {

            solo = true;
            cluster_state =
                    new DistributedClusterState((Map) cluster_state_spec);
        }

        assignment_info_callback =
                new ConcurrentHashMap<String, RunnableCallback>();
        supervisors_callback = new AtomicReference<RunnableCallback>(null);
        assignments_callback = new AtomicReference<RunnableCallback>(null);
        storm_base_callback = new ConcurrentHashMap<String, RunnableCallback>();
        master_callback = new AtomicReference<RunnableCallback>(null);

        state_id = cluster_state.register(new ClusterStateCallback() {

            public <T> Object execute(T... args) {
                if (args == null) {
                    LOG.warn("Input args is null");
                    return null;
                } else if (args.length < 2) {
                    LOG.warn("Input args is invalid, args length:"
                            + args.length);
                    return null;
                }

                EventType zkEventTypes = (EventType) args[0];
                String path = (String) args[1];

                List<String> toks = PathUtils.tokenize_path(path);
                int size = toks.size();
                if (size >= 1) {
                    String params = null;
                    String root = toks.get(0);
                    RunnableCallback fn = null;
                    if (root.equals(Cluster.ASSIGNMENTS_ROOT)) {
                        if (size == 1) {
                            // set null and get the old value
                            fn = assignments_callback.getAndSet(null);
                        } else {
                            params = toks.get(1);
                            fn = assignment_info_callback.remove(params);
                        }

                    } else if (root.equals(Cluster.SUPERVISORS_ROOT)) {
                        fn = supervisors_callback.getAndSet(null);
                    } else if (root.equals(Cluster.STORMS_ROOT) && size > 1) {
                        params = toks.get(1);
                        fn = storm_base_callback.remove(params);
                    } else if (root.equals(Cluster.MASTER_ROOT)) {
                        fn = master_callback.getAndSet(null);
                    } else {
                        LOG.error("Unknown callback for subtree " + path);
                    }

                    if (fn != null) {
                        // FIXME How to set the args
                        // fn.setArgs(params, zkEventTypes, path);
                        fn.run();
                    }
                }

                return null;
            }

        });

        String[] pathlist =
                JStormUtils.mk_arr(Cluster.SUPERVISORS_SUBTREE,
                        Cluster.STORMS_SUBTREE, Cluster.ASSIGNMENTS_SUBTREE,
                        Cluster.ASSIGNMENTS_BAK_SUBTREE, Cluster.TASKS_SUBTREE,
                        Cluster.TASKBEATS_SUBTREE, Cluster.TASKERRORS_SUBTREE,
                        Cluster.METRIC_SUBTREE);
        for (String path : pathlist) {
            cluster_state.mkdirs(path);
        }

    }

    /**
     * @@@ TODO
     * 
     *     Just add cache in lower ZK level In fact, for some Object
     *     Assignment/TaskInfo/StormBase These object can be cache for long time
     * 
     * @param simpleCache
     */
    public void setCache(JStormCache simpleCache) {
        if (cluster_state instanceof DistributedClusterState) {
            ((DistributedClusterState) cluster_state).setZkCache(simpleCache);
        }
    }

    public Object getObject(String path, boolean callback) throws Exception {
        byte[] data = cluster_state.get_data(path, callback);

        return Utils.maybe_deserialize(data);
    }

    public Object getObjectSync(String path, boolean callback) throws Exception {
        byte[] data = cluster_state.get_data_sync(path, callback);

        return Utils.maybe_deserialize(data);
    }

    public String getString(String path, boolean callback) throws Exception {
        byte[] data = cluster_state.get_data(path, callback);

        return new String(data);
    }

    public void deleteObject(String path) {
        try {
            cluster_state.delete_node(path);
        } catch (Exception e) {
            LOG.warn("Failed to delete node " + path);
        }
    }

    public void setObject(String path, Object obj) throws Exception {
        if (obj instanceof byte[]) {
            cluster_state.set_data(path, (byte[]) obj);
        } else if (obj instanceof String) {
            cluster_state.set_data(path, ((String) obj).getBytes());
        } else {
            cluster_state.set_data(path, Utils.serialize(obj));
        }
    }

    public void setTempObject(String path, Object obj) throws Exception {
        if (obj instanceof byte[]) {
            cluster_state.set_ephemeral_node(path, (byte[]) obj);
        } else if (obj instanceof String) {
            cluster_state.set_ephemeral_node(path, ((String) obj).getBytes());
        } else {
            cluster_state.set_ephemeral_node(path, Utils.serialize(obj));
        }
    }

    @Override
    public void disconnect() {
        cluster_state.unregister(state_id);
        if (solo == true) {
            cluster_state.close();
        }
    }

    public void remove_storm(String topologyId, boolean needSleep) {
        deleteObject(Cluster.assignment_path(topologyId));
        // wait 10 seconds, so supervisor will kill worker smoothly
        if (needSleep) {
            JStormUtils.sleepMs(10000);
        }
        try {
            deleteObject(Cluster.storm_task_root(topologyId));
            teardown_heartbeats(topologyId);
            teardown_task_errors(topologyId);
            deleteObject(Cluster.metric_path(topologyId));
        } catch (Exception e) {
            LOG.warn("Failed to delete task root and monitor root for" 
                    + topologyId);
        }
        remove_storm_base(topologyId);
    }

    @Override
    public void remove_storm(String topologyId) throws Exception {
        remove_storm(topologyId, true);
    }

    @Override
    public void try_remove_storm(String topologyId) {
        remove_storm(topologyId, false);
    }

    @Override
    public Assignment assignment_info(String topologyId,
            RunnableCallback callback) throws Exception {
        if (callback != null) {
            assignment_info_callback.put(topologyId, callback);
        }

        String assgnmentPath = Cluster.assignment_path(topologyId);

        return (Assignment) getObject(assgnmentPath, callback != null);

    }

    @Override
    public List<String> assignments(RunnableCallback callback) throws Exception {
        if (callback != null) {
            assignments_callback.set(callback);
        }
        return cluster_state.get_children(Cluster.ASSIGNMENTS_SUBTREE,
                callback != null);
    }

    @Override
    public void set_assignment(String topologyId, Assignment info)
            throws Exception {
        setObject(Cluster.assignment_path(topologyId), info);
    }

    @Override
    public AssignmentBak assignment_bak(String topologyName) throws Exception {
        String assgnmentBakPath = Cluster.assignment_bak_path(topologyName);

        return (AssignmentBak) getObject(assgnmentBakPath, false);

    }

    @Override
    public void backup_assignment(String topologyName, AssignmentBak info)
            throws Exception {
        setObject(Cluster.assignment_bak_path(topologyName), info);
    }

    @Override
    public StormBase storm_base(String topologyId, RunnableCallback callback)
            throws Exception {
        if (callback != null) {
            storm_base_callback.put(topologyId, callback);
        }

        return (StormBase) getObject(Cluster.storm_path(topologyId),
                callback != null);

    }

    @Override
    public void activate_storm(String topologyId, StormBase stormBase)
            throws Exception {
        String stormPath = Cluster.storm_path(topologyId);

        setObject(stormPath, stormBase);
    }

    @Override
    public void remove_storm_base(String topologyId) {
        deleteObject(Cluster.storm_path(topologyId));
    }

    @Override
    public void update_storm(String topologyId, StormStatus newElems)
            throws Exception {
        /**
         * FIXME, maybe overwrite old callback
         */

        StormBase base = this.storm_base(topologyId, null);

        if (base != null) {
            base.setStatus(newElems);
            setObject(Cluster.storm_path(topologyId), base);
        }

    }

    @Override
    public void set_storm_monitor(String topologyId, boolean isEnable)
            throws Exception {
        // TODO Auto-generated method stub
        StormBase base = this.storm_base(topologyId, null);

        if (base != null) {
            base.setEnableMonitor(isEnable);
            setObject(Cluster.storm_path(topologyId), base);
        }
    }

    @Override
    public List<String> active_storms() throws Exception {
        return cluster_state.get_children(Cluster.STORMS_SUBTREE, false);
    }

    @Override
    public void setup_heartbeats(String topologyId) throws Exception {
        String taskbeatPath = Cluster.taskbeat_storm_root(topologyId);

        cluster_state.mkdirs(taskbeatPath);
    }

    @Override
    public List<String> heartbeat_storms() throws Exception {
        return cluster_state.get_children(Cluster.TASKBEATS_SUBTREE, false);
    }

    @Override
    public List<String> heartbeat_tasks(String topologyId) throws Exception {
        String taskbeatPath = Cluster.taskbeat_storm_root(topologyId);

        return cluster_state.get_children(taskbeatPath, false);
    }

    @Override
    public void remove_task_heartbeat(String topologyId, int taskId)
            throws Exception {
        String taskbeatPath = Cluster.taskbeat_path(topologyId, taskId);

        deleteObject(taskbeatPath);
    }

    @Override
    public void teardown_heartbeats(String topologyId) {
        try {
            String taskbeatPath = Cluster.taskbeat_storm_root(topologyId);

            deleteObject(taskbeatPath);
        } catch (Exception e) {
            LOG.warn("Could not teardown heartbeats for " + topologyId, e);
        }

    }

    @Override
    public void report_task_error(String topologyId, int taskId, Throwable error)
            throws Exception {
        report_task_error(topologyId, taskId,
                new String(JStormUtils.getErrorInfo(error)));
    }

    public void report_task_error(String topologyId, int taskId, String error)
            throws Exception {
        boolean found = false;
        String path = Cluster.taskerror_path(topologyId, taskId);
        cluster_state.mkdirs(path);

        List<Integer> children = new ArrayList<Integer>();

        String timeStamp = String.valueOf(TimeUtils.current_time_secs());
        String timestampPath = path + Cluster.ZK_SEPERATOR + timeStamp;

        for (String str : cluster_state.get_children(path, false)) {
            String errorPath = path + "/" + str;
            String errorInfo = getString(errorPath, false);
            if (StringUtils.isBlank(errorInfo)) {
                deleteObject(errorPath);
                continue;
            }
            if (errorInfo.equals(error)) {
                deleteObject(errorPath);
                setObject(timestampPath, error);
                found = true;
                break;
            }

            children.add(Integer.parseInt(str));
        }

        if (found == false) {
            Collections.sort(children);

            while (children.size() >= 3) {
                deleteObject(path + Cluster.ZK_SEPERATOR + children.remove(0));
            }

            setObject(timestampPath, error);
        }

        setLastErrInfo(topologyId, error, timeStamp);
    }

    private static final String TASK_IS_DEAD = "is dead on"; // Full string is
                                                             // "task-id is dead on hostname:port"

    private void setLastErrInfo(String topologyId, String error,
            String timeStamp) throws Exception {
        // Set error information in task error topology patch
        // Last Error information format in ZK: map<report_duration, timestamp>
        // report_duration means only the errors will presented in web ui if the
        // error happens within this duration.
        // Currently, the duration for "queue full" error is 180sec(3min) while
        // the duration for other errors is 1800sec(30min).
        String lastErrTopoPath = Cluster.lasterror_path(topologyId);
        Map<Integer, String> lastErrInfo = null;
        try {
            lastErrInfo =
                    (Map<Integer, String>) getObject(lastErrTopoPath, false);

        } catch (Exception e) {
            LOG.error(
                    "Failed to get last error time. Remove the corrupt node for "
                            + topologyId, e);
            remove_lastErr_time(topologyId);
            lastErrInfo = null;
        }
        if (lastErrInfo == null)
            lastErrInfo = new HashMap<Integer, String>();

        // The error time is used to indicate how long the error info is present
        // in UI
        if (error.indexOf(QueueGauge.QUEUE_IS_FULL) != -1)
            lastErrInfo.put(JStormUtils.MIN_1 * 3, timeStamp);
        else if (error.indexOf(TASK_IS_DEAD) != -1)
            lastErrInfo.put(JStormUtils.DAY_1 * 3, timeStamp);
        else
            lastErrInfo.put(JStormUtils.MIN_30, timeStamp);

        setObject(lastErrTopoPath, lastErrInfo);
    }

    @Override
    public void remove_task_error(String topologyId, int taskId)
            throws Exception {
        String path = Cluster.taskerror_path(topologyId, taskId);
        cluster_state.delete_node(path);
    }

    @Override
    public Map<Integer, String> topo_lastErr_time(String topologyId)
            throws Exception {
        String path = Cluster.lasterror_path(topologyId);

        return (Map<Integer, String>) getObject(path, false);
    }

    @Override
    public void remove_lastErr_time(String topologyId) throws Exception {
        String path = Cluster.lasterror_path(topologyId);
        deleteObject(path);
    }

    @Override
    public List<String> task_error_storms() throws Exception {
        return cluster_state.get_children(Cluster.TASKERRORS_SUBTREE, false);
    }
    
    @Override
    public List<String> task_error_ids(String topologyId) throws Exception {
    	return cluster_state.get_children(Cluster.taskerror_storm_root(topologyId), false);
    }

    @Override
    public List<String> task_error_time(String topologyId, int taskId)
            throws Exception {
        String path = Cluster.taskerror_path(topologyId, taskId);
        cluster_state.mkdirs(path);
        return cluster_state.get_children(path, false);
    }

    @Override
    public void remove_task(String topologyId, Set<Integer> taskIds) throws Exception {
        String tasksPath = Cluster.storm_task_root(topologyId);
        Object data = getObject(tasksPath, false);
        if (data != null) {
            Map<Integer, TaskInfo> taskInfoMap = ((Map<Integer, TaskInfo>)data);
            for (Integer taskId : taskIds){
                taskInfoMap.remove(taskId);
            }
            //update zk node of tasks
            setObject(tasksPath, taskInfoMap);
        }
    }

    @Override
    public String task_error_info(String topologyId, int taskId, long timeStamp)
            throws Exception {
        String path = Cluster.taskerror_path(topologyId, taskId);
        cluster_state.mkdirs(path);
        path = path + "/" + timeStamp;
        return getString(path, false);
    }

    @Override
    public List<TaskError> task_errors(String topologyId, int taskId)
            throws Exception {
        String path = Cluster.taskerror_path(topologyId, taskId);
        cluster_state.mkdirs(path);

        List<String> children = cluster_state.get_children(path, false);
        List<TaskError> errors = new ArrayList<TaskError>();

        for (String str : children) {
            byte[] v = cluster_state.get_data(path + "/" + str, false);
            if (v != null) {
                TaskError error =
                        new TaskError(new String(v), Integer.parseInt(str));
                errors.add(error);
            }
        }

        Collections.sort(errors, new Comparator<TaskError>() {

            @Override
            public int compare(TaskError o1, TaskError o2) {
                if (o1.getTimSecs() > o2.getTimSecs()) {
                    return 1;
                }
                if (o1.getTimSecs() < o2.getTimSecs()) {
                    return -1;
                }
                return 0;
            }
        });

        return errors;

    }

    @Override
    public void teardown_task_errors(String topologyId) {
        try {
            String taskerrPath = Cluster.taskerror_storm_root(topologyId);
            deleteObject(taskerrPath);
        } catch (Exception e) {
            LOG.error("Could not teardown errors for " + topologyId, e);
        }
    }
    @Override
    public void set_task(String topologyId, Map<Integer, TaskInfo>  taskInfoMap)
            throws Exception {
        String stormTaskPath = Cluster.storm_task_root(topologyId);
        if (taskInfoMap != null){
            //reupdate zk node of tasks
            setObject(stormTaskPath, taskInfoMap);
        }
    }
    @Override
    public void add_task(String topologyId, Map<Integer, TaskInfo> taskInfoMap)
            throws Exception {
        String stormTaskPath = Cluster.storm_task_root(topologyId);
        Object data = getObject(stormTaskPath, false);
        if (data != null){
            ((Map<Integer, TaskInfo>)data).putAll(taskInfoMap);
            //reupdate zk node of tasks
            setObject(stormTaskPath, data);
        }
    }

    @Override
    public TaskHeartbeat task_heartbeat(String topologyId, int taskId)
            throws Exception {
        String taskbeatPath = Cluster.taskbeat_path(topologyId, taskId);

        return (TaskHeartbeat) getObjectSync(taskbeatPath, false);

    }

    @Override
    public void task_heartbeat(String topologyId, int taskId, TaskHeartbeat info)
            throws Exception {
        String taskPath = Cluster.taskbeat_path(topologyId, taskId);

        setObject(taskPath, info);
    }

    @Override
    public List<String> task_storms() throws Exception {
        return cluster_state.get_children(Cluster.TASKS_SUBTREE, false);
    }

    @Override
    public Set<Integer> task_ids(String stromId) throws Exception {

        String stormTaskPath = Cluster.storm_task_root(stromId);
        Object data = getObject(stormTaskPath, false);
        if (data == null) {
            return null;
        }
        return ((Map<Integer, TaskInfo>)data).keySet();
    }

    @Override
    public Set<Integer> task_ids_by_componentId(String topologyId,
            String componentId) throws Exception {
        String stormTaskPath = Cluster.storm_task_root(topologyId);
        Object data = getObject(stormTaskPath, false);
        if (data == null) {
            return null;
        }
        Map<Integer, TaskInfo> taskInfoMap = (Map<Integer, TaskInfo>)data;
        Set<Integer> rtn = new HashSet<Integer>();
        Set<Integer> taskIds = taskInfoMap.keySet();
        for(Integer taskId : taskIds){
            TaskInfo taskInfo = taskInfoMap.get(taskId);
            if (taskInfo != null){
                if (taskInfo.getComponentId().equalsIgnoreCase(componentId))
                    rtn.add(taskId);
            }
        }
        return rtn;
    }

    @Override
    public Map<Integer, TaskInfo> task_all_info(String topologyId) throws Exception {

        String taskPath = Cluster.storm_task_root(topologyId);

        Object data = getObject(taskPath, false);
        if (data == null) {
            return null;
        }
        return (Map<Integer, TaskInfo>) data;
    }

    @Override
    public SupervisorInfo supervisor_info(String supervisorId) throws Exception {
        String supervisorPath = Cluster.supervisor_path(supervisorId);

        return (SupervisorInfo) getObject(supervisorPath, false);

    }

    @Override
    public List<String> supervisors(RunnableCallback callback) throws Exception {
        if (callback != null) {
            supervisors_callback.set(callback);
        }
        return cluster_state.get_children(Cluster.SUPERVISORS_SUBTREE,
                callback != null);
    }

    @Override
    public void supervisor_heartbeat(String supervisorId, SupervisorInfo info)
            throws Exception {

        String supervisorPath = Cluster.supervisor_path(supervisorId);

        setTempObject(supervisorPath, info);
    }

    @Override
    public String get_leader_host() throws Exception {
        // TODO Auto-generated method stub
        return new String(cluster_state.get_data(Cluster.MASTER_SUBTREE, false));
    }

    @Override
    public boolean leader_existed() throws Exception {
        // TODO Auto-generated method stub
        return cluster_state.node_existed(Cluster.MASTER_SUBTREE, false);
    }

    @Override
    public List<String> get_nimbus_slaves() throws Exception {
        return cluster_state.get_children(Cluster.NIMBUS_SLAVE_SUBTREE, false);
    }

    public String get_nimbus_slave_time(String host) throws Exception {
        String path =
                Cluster.NIMBUS_SLAVE_SUBTREE + Cluster.ZK_SEPERATOR + host;
        return (String) getObject(path, false);
    }

    @Override
    public void update_nimbus_slave(String host, int time) throws Exception {
        setTempObject(Cluster.NIMBUS_SLAVE_SUBTREE + Cluster.ZK_SEPERATOR
                + host, String.valueOf(time));
    }

    @Override
    public void unregister_nimbus_host(String host) throws Exception {
        deleteObject(Cluster.NIMBUS_SLAVE_SUBTREE + Cluster.ZK_SEPERATOR + host);
    }

    @Override
    public boolean try_to_be_leader(String path, String host,
            RunnableCallback callback) throws Exception {
        // TODO Auto-generated method stub
        if (callback != null)
            this.master_callback.set(callback);
        try {
            cluster_state.tryToBeLeader(path, host.getBytes());
        } catch (NodeExistsException e) {
            cluster_state.node_existed(path, true);
            LOG.info("leader is alive");
            return false;
        }
        return true;
    }

    @Override
    public void set_topology_metric(String topologyId, Object metric)
            throws Exception {
        // TODO Auto-generated method stub
        String path = Cluster.metric_path(topologyId);

        setObject(path, metric);
    }

    @Override
    public Object get_topology_metric(String topologyId) throws Exception {
        // TODO Auto-generated method stub
        return getObject(Cluster.metric_path(topologyId), false);
    }

	@Override
	public List<String> get_metrics() throws Exception {
		// TODO Auto-generated method stub
		return cluster_state.get_children(Cluster.METRIC_SUBTREE, false);
	}

}
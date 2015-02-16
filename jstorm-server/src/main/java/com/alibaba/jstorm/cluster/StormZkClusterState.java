package com.alibaba.jstorm.cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Watcher.Event.EventType;

import backtype.storm.utils.Utils;

import com.alibaba.jstorm.callback.ClusterStateCallback;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.task.Assignment;
import com.alibaba.jstorm.task.AssignmentBak;
import com.alibaba.jstorm.task.TaskInfo;
import com.alibaba.jstorm.task.TaskMetricInfo;
import com.alibaba.jstorm.task.error.TaskError;
import com.alibaba.jstorm.task.heartbeat.TaskHeartbeat;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.PathUtils;
import com.alibaba.jstorm.utils.TimeUtils;
import com.alibaba.jstorm.zk.ZkConstant;
import com.alibaba.jstorm.zk.ZkTool;
import com.alibaba.jstorm.daemon.worker.WorkerMetricInfo;
import com.alibaba.jstorm.metric.UserDefMetric;
import com.alibaba.jstorm.metric.UserDefMetricData;

public class StormZkClusterState implements StormClusterState {
	private static Logger LOG = Logger.getLogger(StormZkClusterState.class);

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
			cluster_state = new DistributedClusterState(
					(Map) cluster_state_spec);
		}

		assignment_info_callback = new ConcurrentHashMap<String, RunnableCallback>();
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

		String[] pathlist = JStormUtils.mk_arr(Cluster.ASSIGNMENTS_SUBTREE,
				Cluster.TASKS_SUBTREE, Cluster.STORMS_SUBTREE,
				Cluster.SUPERVISORS_SUBTREE, Cluster.TASKBEATS_SUBTREE,
				Cluster.TASKERRORS_SUBTREE, Cluster.MONITOR_SUBTREE);
		for (String path : pathlist) {
			cluster_state.mkdirs(path);
		}

	}

	@Override
	public Assignment assignment_info(String topologyId,
			RunnableCallback callback) throws Exception {
		if (callback != null) {
			assignment_info_callback.put(topologyId, callback);
		}

		String assgnmentPath = Cluster.assignment_path(topologyId);

		byte[] znodeData = cluster_state.get_data(assgnmentPath,
				callback != null);

		Object data = Cluster.maybe_deserialize(znodeData);

		if (data == null) {
			return null;
		}
		return (Assignment) data;
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
		cluster_state.set_data(Cluster.assignment_path(topologyId),
				Utils.serialize(info));
	}

	@Override
	public AssignmentBak assignment_bak(String topologyName) throws Exception {
		String assgnmentBakPath = ZkTool.assignment_bak_path(topologyName);

		byte[] znodeData = cluster_state.get_data(assgnmentBakPath, false);

		Object data = Cluster.maybe_deserialize(znodeData);

		if (data == null) {
			return null;
		}
		return (AssignmentBak) data;
	}

	@Override
	public void backup_assignment(String topologyName, AssignmentBak info)
			throws Exception {
		cluster_state.set_data(ZkTool.assignment_bak_path(topologyName),
				Utils.serialize(info));
	}

	@Override
	public void activate_storm(String topologyId, StormBase stormBase)
			throws Exception {
		String stormPath = Cluster.storm_path(topologyId);

		byte[] stormBaseData = Utils.serialize(stormBase);

		cluster_state.set_data(stormPath, stormBaseData);
	}

	@Override
	public List<String> active_storms() throws Exception {
		return cluster_state.get_children(Cluster.STORMS_SUBTREE, false);
	}
	
	@Override
	public List<String> monitor_user_workers(String topologyId) throws Exception {
		return cluster_state.get_children(Cluster.monitor_userdir_path(topologyId), false);
	}
	
	@Override
	public List<String> monitors() throws Exception {
		return cluster_state.get_children(Cluster.MONITOR_SUBTREE, false);
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
	public void remove_storm(String topologyId) throws Exception {
		cluster_state.delete_node(Cluster.assignment_path(topologyId));
		// wait 10 seconds, so supervisor will kill worker smoothly
		JStormUtils.sleepMs(10000);
		cluster_state.delete_node(Cluster.storm_task_root(topologyId));
		cluster_state.delete_node(Cluster.monitor_path(topologyId));
		this.remove_storm_base(topologyId);
	}
	
	@Override
	public void try_remove_storm(String topologyId) {
		teardown_heartbeats(topologyId);
		teardown_task_errors(topologyId);
		
		try {
			cluster_state.delete_node(Cluster.assignment_path(topologyId));
		}catch(Exception e) {
			LOG.warn("Failed to delete zk Assignment " + topologyId);
		}
		
		try {
			cluster_state.delete_node(Cluster.storm_task_root(topologyId));
		}catch(Exception e) {
			LOG.warn("Failed to delete zk taskInfo " + topologyId);
		}
		
		try {
			cluster_state.delete_node(Cluster.monitor_path(topologyId));
		}catch(Exception e) {
			LOG.warn("Failed to delete zk monitor " + topologyId);
		}
	}

	@Override
	public void remove_storm_base(String topologyId) throws Exception {
		cluster_state.delete_node(Cluster.storm_path(topologyId));
	}

	@Override
	public void remove_task_heartbeat(String topologyId, int taskId)
			throws Exception {
		String taskbeatPath = Cluster.taskbeat_path(topologyId, taskId);

		cluster_state.delete_node(taskbeatPath);
	}

	@Override
	public void report_task_error(String topologyId, int taskId, Throwable error)
			throws Exception {
		report_task_error(topologyId, taskId, new String(JStormUtils.getErrorInfo(error)));
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
			children.add(Integer.parseInt(str));
			
			String errorPath = path + "/" + str;
			byte[] data = cluster_state.get_data(errorPath, false);
			if (data == null) continue;
			String errorInfo = new String(data);
			if (errorInfo.equals(error)) {
				cluster_state.delete_node(errorPath);
				cluster_state.set_data(timestampPath, error.getBytes());
				found = true;
				break;
			}	
		}

		if (found == false) {
		    Collections.sort(children);

		    while (children.size() >= 10) {
			    cluster_state.delete_node(path + Cluster.ZK_SEPERATOR
					    + children.remove(0));
		    }

		    cluster_state.set_data(timestampPath, error.getBytes());
		}

		setLastErrInfo(topologyId, error, timeStamp);
	}
	
	private static final String TASK_IS_DEAD = "is dead on"; // Full string is "task-id is dead on hostname:port"
	
	private void setLastErrInfo(String topologyId, String error, String timeStamp) throws Exception {
		// Set error information in task error topology patch
		// Last Error information format in ZK: map<report_duration, timestamp>
		// report_duration means only the errors will presented in web ui if the
		// error happens within this duration.
		// Currently, the duration for "queue full" error is 180sec(3min) while
		// the duration for other errors is 1800sec(30min). 
		String lastErrTopoPath = Cluster.lasterror_path(topologyId);
		Map<Integer, String> lastErrInfo = null;
		try {
		    lastErrInfo = (Map<Integer, String>) 
				    (Cluster.maybe_deserialize(cluster_state.get_data(lastErrTopoPath, false)));
		} catch (Exception e) {
			LOG.error("Failed to get last error time. Remove the corrupt node for " + topologyId, e);
			remove_lastErr_time(topologyId);
			lastErrInfo = null;
		}
		if (lastErrInfo == null)
			lastErrInfo = new HashMap<Integer, String>();

		// The error time is used to indicate how long the error info is present in UI
		if (error.indexOf(TaskMetricInfo.QEUEU_IS_FULL) != -1)
			lastErrInfo.put(JStormUtils.MIN_1*3, timeStamp);
		else if (error.indexOf(TASK_IS_DEAD) != -1)
			lastErrInfo.put(JStormUtils.DAY_1*3, timeStamp);
		else
			lastErrInfo.put(JStormUtils.MIN_30, timeStamp);

		cluster_state.set_data(lastErrTopoPath, Utils.serialize(lastErrInfo));
	}
	
	@Override
	public void set_task(String topologyId, int taskId, TaskInfo info)
			throws Exception {
		String taskPath = Cluster.task_path(topologyId, taskId);

		byte[] taskData = Utils.serialize(info);

		cluster_state.set_data(taskPath, taskData);
	}

	@Override
	public void setup_heartbeats(String topologyId) throws Exception {
		String taskbeatPath = Cluster.taskbeat_storm_root(topologyId);

		cluster_state.mkdirs(taskbeatPath);
	}

	@Override
	public StormBase storm_base(String topologyId, RunnableCallback callback)
			throws Exception {
		if (callback != null) {
			storm_base_callback.put(topologyId, callback);
		}
		Object data = Cluster.maybe_deserialize(cluster_state.get_data(
				Cluster.storm_path(topologyId), callback != null));
		if (data == null) {
			return null;
		}
		return (StormBase) data;
	}

	@Override
	public void supervisor_heartbeat(String supervisorId, SupervisorInfo info)
			throws Exception {

		String supervisorPath = Cluster.supervisor_path(supervisorId);

		byte[] infoData = Utils.serialize(info);

		cluster_state.set_ephemeral_node(supervisorPath, infoData);
	}

	@Override
	public SupervisorInfo supervisor_info(String supervisorId) throws Exception {
		String supervisorPath = Cluster.supervisor_path(supervisorId);

		byte[] znodeData = cluster_state.get_data(supervisorPath, false);

		Object data = Cluster.maybe_deserialize(znodeData);
		if (data == null) {
			return null;
		}
		return (SupervisorInfo) data;

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
	public Map<Integer, String> topo_lastErr_time(String topologyId) throws Exception {
		String path = Cluster.lasterror_path(topologyId);
		Map<Integer, String> lastErrTime;
		lastErrTime = (Map<Integer, String>) (Cluster.maybe_deserialize(
				cluster_state.get_data(path, false)));
		return lastErrTime;
	}
	
	@Override
	public void remove_lastErr_time(String topologyId) throws Exception {
		String path = Cluster.lasterror_path(topologyId);
		cluster_state.delete_node(path);
	}
	
	@Override
	public List<String> task_error_storms() throws Exception {
		return cluster_state.get_children(Cluster.TASKERRORS_SUBTREE, false);
	}
	
	@Override
	public List<String> task_error_time(String topologyId, int taskId) throws Exception {	
		String path = Cluster.taskerror_path(topologyId, taskId);
		cluster_state.mkdirs(path);
		return cluster_state.get_children(path, false);
	}
	
	@Override
	public String task_error_info(String topologyId, int taskId, long timeStamp) throws Exception {
		String path = Cluster.taskerror_path(topologyId, taskId);
		cluster_state.mkdirs(path);
		path = path + "/" + timeStamp;
		return new String(cluster_state.get_data(path, false));
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
				TaskError error = new TaskError(new String(v),
						Integer.parseInt(str));
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
	public TaskHeartbeat task_heartbeat(String topologyId, int taskId)
			throws Exception {
		String taskbeatPath = Cluster.taskbeat_path(topologyId, taskId);

		byte[] znodeData = cluster_state.get_data(taskbeatPath, false);

		Object data = Cluster.maybe_deserialize(znodeData);
		if (data == null) {
			return null;
		}
		return (TaskHeartbeat) data;
	}
	
	@Override
	public Map<String, TaskHeartbeat> task_heartbeat(String topologyId)
			throws Exception {
		Map<String, TaskHeartbeat> ret = new HashMap<String, TaskHeartbeat>();
		
		String topoTbPath = Cluster.taskbeat_storm_root(topologyId);
		List<String> taskList = cluster_state.get_children(topoTbPath, false);
		
		for (String taskId : taskList) {
		    String taskbeatPath = Cluster.taskbeat_path(topologyId, Integer.parseInt(taskId));

		    byte[] znodeData = cluster_state.get_data(taskbeatPath, false);

		    Object data = Cluster.maybe_deserialize(znodeData);
		    if (data == null) {
			    continue;
		    }
		    ret.put(taskId, (TaskHeartbeat)data);
		}
		
		return ret;
	}

	@Override
	public void task_heartbeat(String topologyId, int taskId, TaskHeartbeat info)
			throws Exception {
		String taskPath = Cluster.taskbeat_path(topologyId, taskId);

		byte[] taskData = Utils.serialize(info);

		cluster_state.set_data(taskPath, taskData);
	}

	@Override
	public List<Integer> task_ids(String stromId) throws Exception {

		String stormTaskPath = Cluster.storm_task_root(stromId);

		List<String> list = cluster_state.get_children(stormTaskPath, false);

		List<Integer> rtn = new ArrayList<Integer>();
		for (String str : list) {
			rtn.add(Integer.parseInt(str));
		}
		return rtn;
	}

	@Override
	public TaskInfo task_info(String topologyId, int taskId) throws Exception {

		String taskPath = Cluster.task_path(topologyId, taskId);

		byte[] znodeData = cluster_state.get_data(taskPath, false);

		Object data = Cluster.maybe_deserialize(znodeData);
		if (data == null) {
			return null;
		}
		return (TaskInfo) data;
	}
	
	@Override
	public Map<Integer, TaskInfo> task_info_list(String topologyId) throws Exception {
		Map<Integer, TaskInfo> taskInfoList = new HashMap<Integer, TaskInfo>();
		
		List<Integer> taskIds = task_ids(topologyId);
		
		for (Integer taskId : taskIds) {
			TaskInfo taskInfo = task_info(topologyId, taskId);
			taskInfoList.put(taskId, taskInfo);
		}
		
		return taskInfoList;
	}

	@Override
	public List<String> task_storms() throws Exception {
		return cluster_state.get_children(Cluster.TASKS_SUBTREE, false);
	}

	@Override
	public void teardown_heartbeats(String topologyId) {
		try {
			String taskbeatPath = Cluster.taskbeat_storm_root(topologyId);

			cluster_state.delete_node(taskbeatPath);
		} catch (Exception e) {
			LOG.error("Could not teardown heartbeats for " + topologyId, e);
		}

	}

	@Override
	public void teardown_task_errors(String topologyId) {
		try {
			String taskerrPath = Cluster.taskerror_storm_root(topologyId);
			cluster_state.delete_node(taskerrPath);
		} catch (Exception e) {
			LOG.error("Could not teardown errors for " + topologyId, e);
		}
	}

	@Override
	public void update_storm(String topologyId, StormStatus newElems)
			throws Exception {
		/**
		 * FIXME, not sure where the old exist error or not The raw code
		 * (set-data cluster-state (storm-path storm-id) (-> (storm-base this
		 * storm-id nil) (merge new-elems) Utils/serialize)))
		 */

		StormBase base = this.storm_base(topologyId, null);

		if (base != null) {
			base.setStatus(newElems);
			cluster_state.set_data(Cluster.storm_path(topologyId),
					Utils.serialize(base));
		}

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
	public void disconnect() {
		cluster_state.unregister(state_id);
		if (solo == true) {
			cluster_state.close();
		}
	}

	@Override
	public void register_nimbus_host(String host) throws Exception {
		// TODO Auto-generated method stub
		cluster_state.set_ephemeral_node(ZkConstant.NIMBUS_SLAVE_SUBTREE
				+ Cluster.ZK_SEPERATOR + host, null);
	}

	@Override
	public void unregister_nimbus_host(String host) throws Exception {
		cluster_state.delete_node(ZkConstant.NIMBUS_SLAVE_SUBTREE
				+ Cluster.ZK_SEPERATOR + host);
	}
	
	@Override
	public void update_follower_hb(String host, int time) throws Exception {
		cluster_state.set_data(ZkConstant.NIMBUS_SLAVE_SUBTREE
				+ Cluster.ZK_SEPERATOR + host,
				String.valueOf(time).getBytes("UTF-8"));
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
	public void set_storm_monitor(String topologyId, StormMonitor metricsMonitor) throws Exception {
	    String monitorStatusPath = Cluster.monitor_status_path(topologyId);
	    cluster_state.set_data(monitorStatusPath, Utils.serialize(metricsMonitor));
	    cluster_state.mkdirs(Cluster.monitor_taskdir_path(topologyId));
	    cluster_state.mkdirs(Cluster.monitor_workerdir_path(topologyId));
	    cluster_state.mkdirs(Cluster.monitor_userdir_path(topologyId));
	}

	@Override 
	public StormMonitor get_storm_monitor(String topologyId) throws Exception {
		String monitorPath = Cluster.monitor_status_path(topologyId);
		
		byte[] metricsMonitorData = cluster_state.get_data(monitorPath, false);
		Object metricsMonitor = Cluster.maybe_deserialize(metricsMonitorData);
		
		return (StormMonitor)metricsMonitor;
	}
	public UserDefMetricData get_userDef_metric(String topologyId,String workerId) throws Exception{
		String workerMetricPath = Cluster.monitor_user_path(topologyId, workerId);
		byte[] userMetricsData=cluster_state.get_data(workerMetricPath, false);
		Object userMetrics = Cluster.maybe_deserialize(userMetricsData);
		return (UserDefMetricData)userMetrics;	
	}
	
	@Override
	public void update_task_metric(String topologyId, String taskId, TaskMetricInfo metricInfo) throws Exception {
		String taskMetricPath = Cluster.monitor_task_path(topologyId, taskId);
		cluster_state.set_data(taskMetricPath, Utils.serialize(metricInfo));
	}
	
	@Override
	public void update_worker_metric(String topologyId, String workerId, WorkerMetricInfo metricInfo) throws Exception {
		String workerMetricPath = Cluster.monitor_worker_path(topologyId, workerId);
		cluster_state.set_data(workerMetricPath, Utils.serialize(metricInfo));
	}
	
	@Override
	public void update_userDef_metric(String topologyId, String workerId, UserDefMetricData metricInfo) throws Exception {
		String userMetricPath = Cluster.monitor_user_path(topologyId, workerId);
		cluster_state.set_data(userMetricPath, Utils.serialize(metricInfo));
	}
	
	@Override
	public List<TaskMetricInfo> get_task_metric_list(String topologyId) throws Exception {
		List<TaskMetricInfo> taskMetricList = new ArrayList<TaskMetricInfo>();
		
		String monitorTaskDirPath = Cluster.monitor_taskdir_path(topologyId);	
		List<String> taskList = cluster_state.get_children(monitorTaskDirPath, false);
		
		for(String taskId : taskList) {
			Object taskMetric = Cluster.maybe_deserialize(
					cluster_state.get_data(Cluster.monitor_task_path(topologyId, taskId), false));
			if(taskMetric != null) {
				taskMetricList.add((TaskMetricInfo)taskMetric);
			} else {
				LOG.warn("get_task_metric_list failed, topoId: " + topologyId + " taskId:" + taskId);
			}
		}
		
		return taskMetricList;
	}
	
	@Override
	public List<String> get_metric_taskIds(String topologyId) throws Exception {
		String monitorTaskDirPath = Cluster.monitor_taskdir_path(topologyId);	
		return cluster_state.get_children(monitorTaskDirPath, false);
	}
	
	@Override
	public void remove_metric_task(String topologyId, String taskId) throws Exception {
		String monitorTaskPath = Cluster.monitor_task_path(topologyId, taskId);	
		cluster_state.delete_node(monitorTaskPath);
	}
	
	@Override
	public List<WorkerMetricInfo> get_worker_metric_list(String topologyId) throws Exception {
		List<WorkerMetricInfo> workerMetricList = new ArrayList<WorkerMetricInfo>();
		
		String monitorWorkerDirPath = Cluster.monitor_workerdir_path(topologyId);
		List<String> workerList = cluster_state.get_children(monitorWorkerDirPath, false);
		
		for(String workerId : workerList) {
			byte[] byteArray = cluster_state.get_data(Cluster.monitor_worker_path(topologyId, workerId), false);
			if(byteArray != null) {
				WorkerMetricInfo workerMetric = (WorkerMetricInfo)Cluster.maybe_deserialize(byteArray);
			    if(workerMetric != null) {
				    workerMetricList.add(workerMetric);
			    } 
			} else {
				LOG.warn("get_worker_metric_list failed, workerMetric is null, topoId: " + topologyId + " workerId:" + workerId);
			}
		}
		
		return workerMetricList;
	}
	
	@Override
	public List<String> get_metric_workerIds(String topologyId) throws Exception {
		String monitorWorkerDirPath = Cluster.monitor_workerdir_path(topologyId);
		return cluster_state.get_children(monitorWorkerDirPath, false);
	}
	
	@Override
	public void remove_metric_worker(String topologyId, String workerId) throws Exception {
		String monitorWorkerPath = Cluster.monitor_worker_path(topologyId, workerId);	
		cluster_state.delete_node(monitorWorkerPath);
	}
	
	@Override
	public List<String> get_metric_users(String topologyId) throws Exception {
		String monitorUserDirPath = Cluster.monitor_userdir_path(topologyId);
		return cluster_state.get_children(monitorUserDirPath, false);
	}
	
	@Override
	public void remove_metric_user(String topologyId, String workerId) throws Exception {
		String monitorUserPath = Cluster.monitor_user_path(topologyId, workerId);	
		cluster_state.delete_node(monitorUserPath);
	}
	
	@Override
	public TaskMetricInfo get_task_metric(String topologyId, int taskId) throws Exception {
		TaskMetricInfo taskMetric = null;
		
		String monitorTaskPath = Cluster.monitor_task_path(topologyId, String.valueOf(taskId));	
		taskMetric = (TaskMetricInfo)(Cluster.maybe_deserialize(
				cluster_state.get_data(monitorTaskPath, false)));
		
		return taskMetric;
	}
	
	@Override
	public WorkerMetricInfo get_worker_metric(String topologyId, String workerId) throws Exception {
		WorkerMetricInfo workerMetric = null;
		
		String monitorWorkerPath = Cluster.monitor_worker_path(topologyId, workerId);	
		workerMetric = (WorkerMetricInfo)(Cluster.maybe_deserialize(
				cluster_state.get_data(monitorWorkerPath, false)));
		
		return workerMetric;
	}
}
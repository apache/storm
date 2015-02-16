package com.alibaba.jstorm.cluster;

import java.util.List;
import java.util.Map;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.daemon.worker.WorkerMetricInfo;
import com.alibaba.jstorm.metric.UserDefMetricData;
import com.alibaba.jstorm.task.Assignment;
import com.alibaba.jstorm.task.AssignmentBak;
import com.alibaba.jstorm.task.TaskInfo;
import com.alibaba.jstorm.task.TaskMetricInfo;
import com.alibaba.jstorm.task.error.TaskError;
import com.alibaba.jstorm.task.heartbeat.TaskHeartbeat;

/**
 * all storm in zk operation interface
 */
public interface StormClusterState {
	public List<String> assignments(RunnableCallback callback) throws Exception;

	public Assignment assignment_info(String topology_id,
			RunnableCallback callback) throws Exception;

	public void set_assignment(String topology_id, Assignment info)
			throws Exception;

	public AssignmentBak assignment_bak(String topologyName) throws Exception;

	public void backup_assignment(String topology_id, AssignmentBak info)
			throws Exception;

	public List<String> active_storms() throws Exception;

	public StormBase storm_base(String topology_id, RunnableCallback callback)
			throws Exception;

	public void activate_storm(String topology_id, StormBase storm_base)
			throws Exception;

	public void update_storm(String topology_id, StormStatus new_elems)
			throws Exception;

	public void remove_storm_base(String topology_id) throws Exception;

	public void remove_storm(String topology_id) throws Exception;
	
	public void try_remove_storm(String topology_id);

	public List<Integer> task_ids(String topology_id) throws Exception;

	public void set_task(String topology_id, int task_id, TaskInfo info)
			throws Exception;

	public TaskInfo task_info(String topology_id, int task_id) throws Exception;

	public List<String> task_storms() throws Exception;

	public void setup_heartbeats(String topology_id) throws Exception;

	public void teardown_heartbeats(String topology_id) throws Exception;

	public List<String> heartbeat_storms() throws Exception;

	public List<String> heartbeat_tasks(String topology_id) throws Exception;

	public TaskHeartbeat task_heartbeat(String topology_id, int task_id)
			throws Exception;
	
	public Map<String, TaskHeartbeat> task_heartbeat(String topologyId)
			throws Exception;

	public void task_heartbeat(String topology_id, int task_id,
			TaskHeartbeat info) throws Exception;

	public void remove_task_heartbeat(String topology_id, int task_id)
			throws Exception;

	public List<String> supervisors(RunnableCallback callback) throws Exception;

	public SupervisorInfo supervisor_info(String supervisor_id)
			throws Exception;

	public void supervisor_heartbeat(String supervisor_id, SupervisorInfo info)
			throws Exception;

	public void teardown_task_errors(String topology_id) throws Exception;

	public List<String> task_error_storms() throws Exception;

	public void report_task_error(String topology_id, int task_id,
			Throwable error) throws Exception;
	
	public void report_task_error(String topology_id, int task_id,
			String error) throws Exception;

	public Map<Integer, String> topo_lastErr_time(String topologyId) throws Exception;
	
	public void remove_lastErr_time(String topologyId) throws Exception;
	
	public List<TaskError> task_errors(String topology_id, int task_id)
			throws Exception;

	public boolean try_to_be_leader(String path, String host, RunnableCallback callback) throws Exception;

	public String get_leader_host() throws Exception;
	
	public void update_follower_hb(String host, int time) throws Exception;

	public boolean leader_existed() throws Exception;

	public void register_nimbus_host(String host) throws Exception;
	
	public void unregister_nimbus_host(String host) throws Exception;
	
	public void disconnect() throws Exception;
	
	public void set_storm_monitor(String topologyId, StormMonitor metricsMonitor) throws Exception;
	
	public StormMonitor get_storm_monitor(String topologyId) throws Exception;
	
	public UserDefMetricData get_userDef_metric(String topologyId,String workerId) throws Exception;
	
	public Map<Integer, TaskInfo> task_info_list(String topologyId) throws Exception;
	
	public void update_task_metric(String topologyId, String taskId, TaskMetricInfo metricInfo) throws Exception;

	public void update_worker_metric(String topologyId, String workerId, WorkerMetricInfo metricInfo) throws Exception;
	
	public List<TaskMetricInfo> get_task_metric_list(String topologyId) throws Exception;
	
	public List<String> get_metric_taskIds(String topologyId) throws Exception;
	
	public void remove_metric_task(String topologyId, String taskId) throws Exception;
	
	public List<WorkerMetricInfo> get_worker_metric_list(String topologyId) throws Exception;
	
	public List<String> get_metric_workerIds(String topologyId) throws Exception;
	
	public void remove_metric_worker(String topologyId, String workerId) throws Exception;
	
	public List<String> get_metric_users(String topologyId) throws Exception;
	
	public void remove_metric_user(String topologyId, String workerId) throws Exception;
	
	public void update_userDef_metric(String topologyId, String workerId, UserDefMetricData metricInfo) throws Exception;
	
	public List<String> monitor_user_workers(String topologyId) throws Exception;
	
	public List<String> monitors() throws Exception;
	
	public TaskMetricInfo get_task_metric(String topologyId, int taskId) throws Exception;
	
	public WorkerMetricInfo get_worker_metric(String topologyId, String workerId) throws Exception;
	
	public List<String> task_error_time(String topologyId, int taskId) throws Exception;
	
	public String task_error_info(String topologyId, int taskId, long timeStamp) throws Exception;
}

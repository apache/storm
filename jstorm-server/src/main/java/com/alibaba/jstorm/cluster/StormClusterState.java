package com.alibaba.jstorm.cluster;

import java.util.List;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.task.Assignment;
import com.alibaba.jstorm.task.AssignmentBak;
import com.alibaba.jstorm.task.TaskInfo;
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

	public List<TaskError> task_errors(String topology_id, int task_id)
			throws Exception;

	public boolean try_to_be_leader(String path, String host, RunnableCallback callback) throws Exception;

	public String get_leader_host() throws Exception;
	
	public void update_follower_hb(String host, int time) throws Exception;

	public boolean leader_existed() throws Exception;

	public void register_nimbus_host(String host) throws Exception;
	
	public void unregister_nimbus_host(String host) throws Exception;
	
	public void disconnect() throws Exception;
}

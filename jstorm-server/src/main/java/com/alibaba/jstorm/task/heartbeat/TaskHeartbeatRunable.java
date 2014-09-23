package com.alibaba.jstorm.task.heartbeat;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import backtype.storm.Config;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.stats.CommonStatsRolling;
import com.alibaba.jstorm.task.TaskStatus;
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
	private static final Logger LOG = Logger
			.getLogger(TaskHeartbeatRunable.class);

	private StormClusterState zkCluster;
	private String topology_id;
	private UptimeComputer uptime;
	private Map storm_conf;
	private Integer frequence;
	
	private AtomicBoolean active;
	
	private static Map<Integer, TaskStats> taskStatsMap = 
			new HashMap<Integer, TaskStats>();
	
	public static void registerTaskStats(int taskId, TaskStats taskStats) {
		taskStatsMap.put(taskId, taskStats);
	}
	
	public static void unregisterTaskStats(int taskId) {
		taskStatsMap.remove(taskId);
	}

	public TaskHeartbeatRunable(WorkerData workerData) {
//		StormClusterState zkCluster, String _topology_id,
//		int _task_id, UptimeComputer _uptime,
//		CommonStatsRolling _task_stats, TaskStatus _taskStatus,
//		Map _storm_conf;
		
		
		this.zkCluster = workerData.getZkCluster();
		this.topology_id = workerData.getTopologyId();
		this.uptime = new UptimeComputer();;
		this.storm_conf = workerData.getStormConf();
		this.active = workerData.getActive();

		String key = Config.TASK_HEARTBEAT_FREQUENCY_SECS;
		Object time = storm_conf.get(key);
		frequence = JStormUtils.parseInt(time, 10);

	}

	@Override
	public void run() {
		Integer currtime = TimeUtils.current_time_secs();

		for (Entry<Integer, TaskStats> entry : taskStatsMap.entrySet()) {
			Integer taskId = entry.getKey();
			CommonStatsRolling taskStats = entry.getValue().getTaskStat();

			String idStr = " " + topology_id + ":" + taskId + " ";

			try {
				TaskHeartbeat hb = new TaskHeartbeat(currtime, uptime.uptime(),
						taskStats.render_stats(), entry.getValue().getComponentType());
				zkCluster.task_heartbeat(topology_id, taskId, hb);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				String errMsg = "Failed to update heartbeat to ZK " + idStr
						+ "\n";
				LOG.error(errMsg, e);
				continue;
			}
		}

		LOG.info("update all task hearbeat ts " + currtime + "," + taskStatsMap.keySet());
	}

	@Override
	public Object getResult() {
		if (active.get() == true) {
			return frequence;

		} else {
			LOG.info("Successfully shutdown Task's headbeat thread");
			return -1;
		}
	}

}

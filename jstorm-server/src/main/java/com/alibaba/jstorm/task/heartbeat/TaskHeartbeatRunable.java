package com.alibaba.jstorm.task.heartbeat;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.cluster.StormClusterState;
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
	private int task_id;
	private String idStr;
	private UptimeComputer uptime;
	private CommonStatsRolling task_stats;
	private TaskStatus taskStatus;
	private Map storm_conf;
	private Integer frequence;

	public TaskHeartbeatRunable(StormClusterState zkCluster, String _topology_id,
			int _task_id, UptimeComputer _uptime,
			CommonStatsRolling _task_stats, TaskStatus _taskStatus,
			Map _storm_conf) {
		this.zkCluster = zkCluster;
		this.topology_id = _topology_id;
		this.task_id = _task_id;
		this.uptime = _uptime;
		this.task_stats = _task_stats;
		this.taskStatus = _taskStatus;
		this.storm_conf = _storm_conf;

		String key = Config.TASK_HEARTBEAT_FREQUENCY_SECS;
		Object time = storm_conf.get(key);
		frequence = JStormUtils.parseInt(time, 10);

		idStr = " " + topology_id + ":" + task_id + " ";
	}

	@Override
	public void run() {
		Integer currtime = TimeUtils.current_time_secs();
		TaskHeartbeat hb = new TaskHeartbeat(currtime, uptime.uptime(),
				task_stats.render_stats());

		try {
			zkCluster.task_heartbeat(topology_id, task_id, hb);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			String errMsg = "Failed to update heartbeat to ZK " + idStr + "\n";
			LOG.error(errMsg, e);
			return;
		}

		LOG.debug("task hearbeat task_id=" + idStr + "=>" + hb.toString());
	}

	@Override
	public Object getResult() {
		if (taskStatus.isShutdown() == false) {
			return frequence;

		} else {
			LOG.info("Successfully shutdown Task's headbeat thread" + idStr);
			return -1;
		}
	}

}

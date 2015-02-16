package com.alibaba.jstorm.task.heartbeat;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingDeque;
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
import com.alibaba.jstorm.task.TaskInfo;

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
	private static LinkedBlockingDeque<Event> eventQueue = new 
			LinkedBlockingDeque<TaskHeartbeatRunable.Event>();
	
	public static void registerTaskStats(int taskId, TaskStats taskStats) {
		Event event = new Event(Event.REGISTER_TYPE, taskId, taskStats);
		eventQueue.offer(event);
	}
	
	public static void unregisterTaskStats(int taskId) {
		Event event = new Event(Event.UNREGISTER_TYPE, taskId, null);
		eventQueue.offer(event);
	}

	public TaskHeartbeatRunable(WorkerData workerData) {
		
		
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
		Event event = eventQueue.poll();
		while(event != null) {
			if (event.getType() == Event.REGISTER_TYPE) {
				taskStatsMap.put(event.getTaskId(), event.getTaskStats());
			}else {
				taskStatsMap.remove(event.getTaskId());
			}
			
			event = eventQueue.poll();
		}
		
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
	
	private static class Event {
		public static final int REGISTER_TYPE = 0;
		public static final int UNREGISTER_TYPE = 1;
		private final int type;
		private final int taskId;
		private final TaskStats taskStats;
		
		public Event(int type, int taskId, TaskStats taskStats) {
			this.type = type;
			this.taskId = taskId;
			this.taskStats = taskStats;
		}

		public int getType() {
			return type;
		}

		public int getTaskId() {
			return taskId;
		}

		public TaskStats getTaskStats() {
			return taskStats;
		}
		
		
	}

}

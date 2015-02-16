package com.alibaba.jstorm.schedule;

import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.daemon.nimbus.NimbusUtils;
import com.alibaba.jstorm.daemon.nimbus.StatusType;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.task.Assignment;
import com.alibaba.jstorm.utils.TimeFormat;
import com.alibaba.jstorm.task.TaskInfo;

/**
 * 
 * Scan all task's heartbeat, if task isn't alive, DO
 * NimbusUtils.transition(monitor)
 * 
 * @author Longda
 * 
 */
public class MonitorRunnable implements Runnable {
	private static Logger LOG = Logger.getLogger(MonitorRunnable.class);

	private NimbusData data;

	public MonitorRunnable(NimbusData data) {
		this.data = data;
	}

	/**
	 * @@@ Todo when one topology is being reassigned, the topology should be
	 *     skip check
	 * @param data
	 */
	@Override
	public void run() {
		StormClusterState clusterState = data.getStormClusterState();

		try {
			// Attetion, need first check Assignments
			List<String> active_topologys = clusterState.assignments(null);

			if (active_topologys == null) {
				LOG.info("Failed to get active topologies");
				return;
			}

			for (String topologyid : active_topologys) {

				LOG.debug("Check tasks " + topologyid);

				// Attention, here don't check /ZK-dir/taskbeats/topologyid to
				// get task ids
				List<Integer> taskIds = clusterState.task_ids(topologyid);
				if (taskIds == null) {
					LOG.info("Failed to get task ids of " + topologyid);
					continue;
				}
				Assignment assignment = clusterState.assignment_info(
						topologyid, null);

				boolean needReassign = false;
				for (Integer task : taskIds) {
					boolean isTaskDead = NimbusUtils.isTaskDead(data,
							topologyid, task);
					if (isTaskDead == true) {
						LOG.info("Found " + topologyid + ",taskid:" + task
								+ " is dead");
						
						ResourceWorkerSlot resource = null;
						if (assignment != null)
							resource = assignment.getWorkerByTaskId(task);
						if (resource != null) {
							Date now = new Date();
							String nowStr = TimeFormat.getSecond(now);
							String errorInfo = "Task-" + task + " is dead on "
									+ resource.getHostname() + ":"
									+ resource.getPort() + ", " + nowStr;
							LOG.info(errorInfo);
						    clusterState.report_task_error(topologyid, task, errorInfo);
						}
						needReassign = true;
					}
				}
				if (needReassign == true) {
					NimbusUtils.transition(data, topologyid, false,
							StatusType.monitor);
				}
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.error(e.getCause(), e);
		}
	}

}

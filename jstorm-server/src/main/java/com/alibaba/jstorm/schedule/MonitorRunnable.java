package com.alibaba.jstorm.schedule;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.daemon.nimbus.NimbusUtils;
import com.alibaba.jstorm.daemon.nimbus.StatusType;
import com.alibaba.jstorm.resource.ResourceAssignment;
import com.alibaba.jstorm.task.Assignment;

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
						
						ResourceAssignment resource = null;
						if (assignment != null)
							resource = assignment.getTaskToResource().get(task);
						if (resource != null) {
							String host = resource.getHostname();
							if (host == null) {
								host = assignment.getNodeHost().
										get(resource.getSupervisorId());
							}
							LOG.info("taskid: " + task + " is on "
									+ host + ":"
									+ resource.getPort());
						}
							
						needReassign = true;
						break;
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

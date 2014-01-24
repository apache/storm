package com.alibaba.jstorm.task.error;

import org.apache.log4j.Logger;

import com.alibaba.jstorm.cluster.StormClusterState;

/**
 * Task error report callback
 * 
 * @author yannian
 * 
 */
public class TaskReportError implements ITaskReportErr {
	private static Logger LOG = Logger.getLogger(TaskReportError.class);
	private StormClusterState zkCluster;
	private String topology_id;
	private int task_id;

	public TaskReportError(StormClusterState _storm_cluster_state,
			String _topology_id, int _task_id) {
		this.zkCluster = _storm_cluster_state;
		this.topology_id = _topology_id;
		this.task_id = _task_id;
	}

	@Override
	public void report(Throwable error) {

		LOG.error("Report error to /ZK/taskerrors/" + topology_id + "/" + task_id
				+ "\n", error);
		try {
			zkCluster.report_task_error(topology_id, task_id, error);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.error("Failed update error to /ZK/taskerrors/" + topology_id + "/"
					+ task_id + "\n", e);
		}

	}

}

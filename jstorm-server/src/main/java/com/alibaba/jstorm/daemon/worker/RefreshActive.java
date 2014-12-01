package com.alibaba.jstorm.daemon.worker;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import backtype.storm.Config;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.cluster.StormBase;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormMonitor;
import com.alibaba.jstorm.daemon.nimbus.StatusType;
import com.alibaba.jstorm.daemon.worker.metrics.MetricReporter;
import com.alibaba.jstorm.task.TaskShutdownDameon;
import com.alibaba.jstorm.utils.JStormUtils;

/**
 * Timely check whether topology is active or not and whether
 * the metrics monitor is enable or disable from ZK
 * 
 * @author yannian/Longda
 * 
 */
public class RefreshActive extends RunnableCallback {
	private static Logger LOG = Logger.getLogger(RefreshActive.class);

	private WorkerData workerData;

	private AtomicBoolean active;
	private Map<Object, Object> conf;
	private StormClusterState zkCluster;
	private String topologyId;
	private Integer frequence;

	// private Object lock = new Object();

	@SuppressWarnings("rawtypes")
	public RefreshActive(WorkerData workerData) {
		this.workerData = workerData;

		this.active = workerData.getActive();
		this.conf = workerData.getConf();
		this.zkCluster = workerData.getZkCluster();
		this.topologyId = workerData.getTopologyId();
		this.frequence = JStormUtils.parseInt(
				conf.get(Config.TASK_REFRESH_POLL_SECS), 10);
	}

	@Override
	public void run() {

		if (active.get() == false) {
			return;
		}
		
		

		try {
			StatusType newTopologyStatus = StatusType.activate;
			// /ZK-DIR/topology
			StormBase base = zkCluster.storm_base(topologyId, this);
			if (base == null) {
				// @@@ normally the topology has been removed
				LOG.warn("Failed to get StromBase from ZK of " + topologyId);
				newTopologyStatus = StatusType.killed;
			} else {

				newTopologyStatus = base.getStatus().getStatusType();
			}

			// Start metrics report if metrics monitor is enabled.
			// Stop metrics report if metrics monitor is disabled. 
			try {
			    StormMonitor monitor = zkCluster.get_storm_monitor(topologyId);
			    if (null != monitor) {
			        boolean newMetricsMonitor = monitor.getMetrics();
			        MetricReporter metricReporter = workerData.getMetricsReporter();
			        boolean oldMetricsMonitor = metricReporter.isEnable();
			    
			        if (oldMetricsMonitor != newMetricsMonitor) {
			            metricReporter.setEnable(newMetricsMonitor);
			            if (true == newMetricsMonitor) {
			                LOG.info("Start metrics reporter");
			            } else {
			    	        LOG.info("Stop metrics reporter");
			            }
			        }
			    }
			} catch (Exception e) {
				LOG.warn("Failed to get monitor status of topology " + topologyId);
				LOG.debug(e);
			}
			
			// Process the topology status change
			StatusType oldTopologyStatus = workerData.getTopologyStatus();

			if (newTopologyStatus.equals(oldTopologyStatus)) {
				return;
			}

			LOG.info("Old TopologyStatus:" + oldTopologyStatus
					+ ", new TopologyStatus:" + newTopologyStatus);

			List<TaskShutdownDameon> tasks = workerData.getShutdownTasks();
			if(tasks == null) {
				LOG.info("Tasks aren't ready or begin to shutdown");
				return ;
			}

			if (newTopologyStatus.equals(StatusType.active)) {
				for (TaskShutdownDameon task : tasks) {
					task.active();
				}
			} else {
				for (TaskShutdownDameon task : tasks) {
					task.deactive();
				}
			}
			workerData.setTopologyStatus(newTopologyStatus);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.error("Failed to get topology from ZK ", e);
			return;
		}

	}

	@Override
	public Object getResult() {
		if (active.get()) {
			return frequence;
		}
		return -1;
	}
}

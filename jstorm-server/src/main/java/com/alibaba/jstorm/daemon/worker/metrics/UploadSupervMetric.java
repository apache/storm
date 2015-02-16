package  com.alibaba.jstorm.daemon.worker.metrics;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

import org.apache.log4j.Logger;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.StormBase;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormMonitor;
import com.alibaba.jstorm.daemon.supervisor.Supervisor;
import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.daemon.worker.WorkerMetricInfo;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.task.TaskMetricInfo;
import com.alibaba.jstorm.task.Assignment;
import com.alibaba.jstorm.utils.JStormUtils;

public class UploadSupervMetric extends RunnableCallback {
	private static Logger LOG = Logger.getLogger(UploadSupervMetric.class);
	
    private AtomicBoolean active;
	private Integer result;
	private int frequence;
    
	private Map conf;
	private String supervisorId;
	private String hostName;
	private StormClusterState cluster;
	private MetricSendClient client;
	
	List<Map<String, Object>> jsonMsgTasks = new ArrayList<Map<String, Object>>();
	List<Map<String, Object>> jsonMsgWorkers = new ArrayList<Map<String, Object>>();
	
	public UploadSupervMetric(Map conf, StormClusterState cluster, String supervisorId, 
			AtomicBoolean active, int frequence, MetricSendClient client) {
		this.active = active;
		this.frequence = frequence;
		this.result = null;
		this.conf = conf;
		this.cluster = cluster;
		this.supervisorId = supervisorId;
		this.client = client;
		try {
		    SupervisorInfo supervisorInfo = cluster.supervisor_info(supervisorId);
		    this.hostName = supervisorInfo.getHostName();
		} catch (Exception e) {
			LOG.error("Failed to get hostname for supervisorID=" + supervisorId);
		}
	}
	
	@Override
	public Object getResult() {
		return result;
	}

	@Override
	public void run() {
		sendMetricsData();;
		if (active.get()) {
			this.result = frequence;
		} else {
			this.result = -1;

		}
	}
	
	
	public void sendMetricsData() {
		
		try {
		    List<String> topologys = cluster.active_storms();
		    
		    for (String topologyId : topologys) {
		    	StormMonitor monitor = null;
		    	boolean metricPerf = true;
		    	Assignment assignment = null;
		    	
		    	try {
		    	    monitor = cluster.get_storm_monitor(topologyId);
		    	    if (monitor != null) metricPerf = monitor.getMetrics();
		    	
		    	    assignment = cluster.assignment_info(topologyId, null);
		    	} catch (Exception e) {
		    		LOG.error("Error when retrieving monitor status and assignment info "
		    				+ "for " + topologyId, e);
		    		continue;
		    	}
		    	
		    	if (assignment != null) {
		    		Set<Integer> taskSet = new HashSet<Integer>();
		    		Set<Integer> workerSet = new HashSet<Integer>();
		    		//Retrieve task set
		    		Set<Integer> tempTaskSet = assignment.getCurrentSuperviosrTasks(supervisorId);
		    	    taskSet.addAll(tempTaskSet);
		    		
		    		//Retrieve worker set
		    		Set<Integer> tempWorkerSet = assignment.getCurrentSuperviosrWorkers(supervisorId);
		    		workerSet.addAll(tempWorkerSet);
		    		
		    		//Build KV Map for AliMonitor
		    		buildTaskJsonMsg(topologyId, taskSet, metricPerf);
		    		buildWorkerJsonMsg(topologyId, workerSet, metricPerf);
		    	}
		    }
		    
		    if (jsonMsgTasks.size() != 0) {
		    	if (client instanceof AlimonitorClient) {
		    	    ((AlimonitorClient) client).setMonitorName(
		    	    		ConfigExtension.getAlmonTaskMetricName(conf));
		    	    ((AlimonitorClient) client).setCollectionFlag(0);
				    ((AlimonitorClient) client).setErrorInfo("");
		    	}
		    	client.send(jsonMsgTasks);
		    }
		    
		    if (jsonMsgWorkers.size() != 0) {
		    	if (client instanceof AlimonitorClient) {
		    	    ((AlimonitorClient) client).setMonitorName(
		    	    		ConfigExtension.getAlmonWorkerMetricName(conf));
		    	    ((AlimonitorClient) client).setCollectionFlag(0);
				    ((AlimonitorClient) client).setErrorInfo("");
		    	}
		    	client.send(jsonMsgWorkers);
		    }
		    
		    jsonMsgTasks.clear();
		    jsonMsgWorkers.clear();
		    
		} catch (Exception e) {
			LOG.error("Failed to upload worker&task metrics data", e);
			jsonMsgTasks.clear();
		    jsonMsgWorkers.clear();
		}
	}

	public void buildTaskJsonMsg(String topologyId, Set<Integer> taskSet, boolean metricPerf) {
		for (Integer taskId : taskSet) {
			try {
			    TaskMetricInfo taskMetric = cluster.get_task_metric(topologyId, taskId);
			    if (taskMetric == null) continue;
			    
			    // Task KV structure
			    Map<String, Object> taskKV = new HashMap<String, Object>();
			    taskKV.put("Topology_Name", topologyId);
			    taskKV.put("Task_Id", String.valueOf(taskId));
			    taskKV.put("Component", taskMetric.getComponent());
			    taskKV.putAll(taskMetric.getGaugeData());
			    taskKV.putAll(taskMetric.getCounterData());
			    taskKV.putAll(taskMetric.getMeterData());
			    if (metricPerf == true) {
			        taskKV.putAll(taskMetric.getTimerData());
			        taskKV.putAll(taskMetric.getHistogramData());
			    }
			    
			    taskKV.put("Task_Error_Info", getTaskErrInfo(topologyId, taskId));
			    
			    jsonMsgTasks.add(taskKV);
			} catch (Exception e) {
				LOG.error("Failed to buildTaskJsonMsg, taskID=" + taskId + ", e=" + e);
			}
		}
	}
	
	public void buildWorkerJsonMsg(String topologyId, Set<Integer> workerSet, boolean metricPerf) {
		String workerId = null;
		for (Integer port: workerSet) {
			try {
				workerId = hostName + ":" + port;
				WorkerMetricInfo workerMetric = cluster.get_worker_metric(topologyId, workerId);
				if (workerMetric == null) continue;
				
				Map<String, Object> workerKV = new HashMap<String, Object>();
                
				workerKV.put("Topology_Name", topologyId);
				workerKV.put("Port", String.valueOf(port));
				workerKV.put(MetricDef.MEMORY_USED, workerMetric.getUsedMem());
				workerKV.put(MetricDef.CPU_USED_RATIO, workerMetric.getUsedCpu());
				
				workerKV.putAll(workerMetric.getGaugeData());
				workerKV.putAll(workerMetric.getCounterData());
				workerKV.putAll(workerMetric.getMeterData());
				
				if (metricPerf == true)
				{
                    workerKV.putAll(workerMetric.getTimerData());
                    workerKV.putAll(workerMetric.getHistogramData());
				}
				
				jsonMsgWorkers.add(workerKV);
			} catch (Exception e) {
				LOG.error("Failed to buildWorkerJsonMsg, workerId=" + workerId + ", e=" + e);
			}
		}
	}
	
	public String getTaskErrInfo(String topologyId, int taskId) {
		String ret = null;
		long currTime = System.currentTimeMillis()/1000;
		
		try {
		    List<String> errorTimeStamps = cluster.task_error_time(topologyId, taskId);
		    
		    // Only send the errors which ocurr during last min
		    for (String time : errorTimeStamps) {
		    	long errTime = JStormUtils.parseLong(time);
		    	if (currTime - errTime < ConfigExtension.getTaskErrorReportInterval(conf)) {
		    		String errInfo = cluster.task_error_info(topologyId, taskId, errTime);
		    		if (errInfo.indexOf("queue is full") == -1) {
		    		    if (ret == null)
		    		        ret = errInfo;
		    		    else
                            ret = ret + "; " + errInfo;
		    		}
		    	}
		    }
		} catch (Exception e) {
			LOG.error("Failed to read task error info for topo=" + topologyId + ", taskId=" + taskId 
					+ ". Cause is \"" + e.getMessage() + "\"");
		}
		
		return ret;
	}

	public void clean() {
	}
}
package com.alibaba.jstorm.daemon.nimbus;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.ErrorInfo;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StateSpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TaskMetricData;
import backtype.storm.generated.TaskSummary;
import backtype.storm.generated.TopologySummary;
import backtype.storm.generated.WorkerMetricData;
import backtype.storm.generated.WorkerSummary;
import backtype.storm.utils.ThriftTopologyUtils;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.cluster.StormMonitor;
import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.daemon.worker.WorkerMetricInfo;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.task.Assignment;
import com.alibaba.jstorm.task.TaskMetricInfo;
import com.alibaba.jstorm.task.TkHbCacheTime;
import com.alibaba.jstorm.task.error.TaskError;
import com.alibaba.jstorm.task.heartbeat.TaskHeartbeat;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.PathUtils;
import com.alibaba.jstorm.utils.TimeUtils;

public class NimbusUtils {

	private static Logger LOG = Logger.getLogger(NimbusUtils.class);

	/**
	 * add coustom KRYO serialization
	 * 
	 */
	private static Map mapifySerializations(List sers) {
		Map rtn = new HashMap();
		if (sers != null) {
			int size = sers.size();
			for (int i = 0; i < size; i++) {
				if (sers.get(i) instanceof Map) {
					rtn.putAll((Map) sers.get(i));
				} else {
					rtn.put(sers.get(i), null);
				}
			}

		}
		return rtn;
	}

	/**
	 * Normalize stormConf
	 * 
	 * 
	 * 
	 * @param conf
	 * @param stormConf
	 * @param topology
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	public static Map normalizeConf(Map conf, Map stormConf,
			StormTopology topology) throws Exception {

		List kryoRegisterList = new ArrayList();
		List kryoDecoratorList = new ArrayList();

		Map totalConf = new HashMap();
		totalConf.putAll(conf);
		totalConf.putAll(stormConf);

		Object totalRegister = totalConf.get(Config.TOPOLOGY_KRYO_REGISTER);
		if (totalRegister != null) {
			LOG.info("topology:" + stormConf.get(Config.TOPOLOGY_NAME)
					+ ", TOPOLOGY_KRYO_REGISTER"
					+ totalRegister.getClass().getName());

			JStormUtils.mergeList(kryoRegisterList, totalRegister);
		}

		Object totalDecorator = totalConf.get(Config.TOPOLOGY_KRYO_DECORATORS);
		if (totalDecorator != null) {
			LOG.info("topology:" + stormConf.get(Config.TOPOLOGY_NAME)
					+ ", TOPOLOGY_KRYO_DECORATOR"
					+ totalDecorator.getClass().getName());
			JStormUtils.mergeList(kryoDecoratorList, totalDecorator);
		}

		Set<String> cids = ThriftTopologyUtils.getComponentIds(topology);
		for (Iterator it = cids.iterator(); it.hasNext();) {
			String componentId = (String) it.next();

			ComponentCommon common = ThriftTopologyUtils.getComponentCommon(
					topology, componentId);
			String json = common.get_json_conf();
			if (json == null) {
				continue;
			}
			Map mtmp = (Map) JStormUtils.from_json(json);
			if (mtmp == null) {
				StringBuilder sb = new StringBuilder();

				sb.append("Failed to deserilaize " + componentId);
				sb.append(" json configuration: ");
				sb.append(json);
				LOG.info(sb.toString());
				throw new Exception(sb.toString());
			}

			Object componentKryoRegister = mtmp
					.get(Config.TOPOLOGY_KRYO_REGISTER);

			if (componentKryoRegister != null) {
				LOG.info("topology:" + stormConf.get(Config.TOPOLOGY_NAME)
						+ ", componentId:" + componentId
						+ ", TOPOLOGY_KRYO_REGISTER"
						+ componentKryoRegister.getClass().getName());

				JStormUtils.mergeList(kryoRegisterList, componentKryoRegister);
			}

			Object componentDecorator = mtmp
					.get(Config.TOPOLOGY_KRYO_DECORATORS);
			if (componentDecorator != null) {
				LOG.info("topology:" + stormConf.get(Config.TOPOLOGY_NAME)
						+ ", componentId:" + componentId
						+ ", TOPOLOGY_KRYO_DECORATOR"
						+ componentDecorator.getClass().getName());
				JStormUtils.mergeList(kryoDecoratorList, componentDecorator);
			}

		}

		Map kryoRegisterMap = mapifySerializations(kryoRegisterList);
		List decoratorList = JStormUtils.distinctList(kryoDecoratorList);

		Integer ackerNum = JStormUtils.parseInt(totalConf
				.get(Config.TOPOLOGY_ACKER_EXECUTORS));
		if (ackerNum == null) {
			ackerNum = Integer.valueOf(1);
		}

		Map rtn = new HashMap();
		rtn.putAll(stormConf);
		rtn.put(Config.TOPOLOGY_KRYO_DECORATORS, decoratorList);
		rtn.put(Config.TOPOLOGY_KRYO_REGISTER, kryoRegisterMap);
		rtn.put(Config.TOPOLOGY_ACKER_EXECUTORS, ackerNum);
		rtn.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM,
				totalConf.get(Config.TOPOLOGY_MAX_TASK_PARALLELISM));
		return rtn;
	}

	public static Integer componentParalism(Map stormConf,
			ComponentCommon common) {
		Map mergeMap = new HashMap();
		mergeMap.putAll(stormConf);

		String jsonConfString = common.get_json_conf();
		if (jsonConfString != null) {
			Map componentMap = (Map) JStormUtils.from_json(jsonConfString);
			mergeMap.putAll(componentMap);
		}

		Integer taskNum = common.get_parallelism_hint();
		if (taskNum == null) {
			taskNum = Integer.valueOf(1);
		}

		// don't get taskNum from component configuraiton
		// skip .setTaskNum
		// Integer taskNum = null;
		// Object taskNumObject = mergeMap.get(Config.TOPOLOGY_TASKS);
		// if (taskNumObject != null) {
		// taskNum = JStormUtils.parseInt(taskNumObject);
		// } else {
		// taskNum = common.get_parallelism_hint();
		// if (taskNum == null) {
		// taskNum = Integer.valueOf(1);
		// }
		// }

		Object maxTaskParalismObject = mergeMap
				.get(Config.TOPOLOGY_MAX_TASK_PARALLELISM);
		if (maxTaskParalismObject == null) {
			return taskNum;
		} else {
			int maxTaskParalism = JStormUtils.parseInt(maxTaskParalismObject);

			return Math.min(maxTaskParalism, taskNum);
		}

	}
	

	/**
	 * finalize component's task paralism
	 * 
	 * @param topology
	 * @param fromConf means if the paralism is read from conf file
	 *        instead of reading from topology code
	 * @return
	 */
	public static StormTopology normalizeTopology(Map stormConf,
			StormTopology topology, boolean fromConf){
		StormTopology ret = topology.deepCopy();
		
		Map<String, Object> rawComponents = ThriftTopologyUtils.getComponents(topology);

		Map<String, Object> components = ThriftTopologyUtils.getComponents(ret);
		
		if (rawComponents.keySet().equals(components.keySet()) == false) {
			String errMsg = "Failed to normalize topology binary, maybe due to wrong dependency";
			LOG.info(errMsg + " raw components:" + rawComponents.keySet() + 
					", normalized " + components.keySet());
			
			throw new InvalidParameterException(errMsg);
		}
		
		for (Entry<String, Object> entry : components.entrySet()) {
			Object component = entry.getValue();
			String componentName = entry.getKey();

			ComponentCommon common = null;
			if (component instanceof Bolt) {
				common = ((Bolt) component).get_common();
				if (fromConf) {
					Integer paraNum = ConfigExtension.getBoltParallelism(stormConf, componentName);
					if (paraNum != null) {
						LOG.info("Set " + componentName + " as " + paraNum);
						common.set_parallelism_hint(paraNum);
					}
				}
			}
			if (component instanceof SpoutSpec) {
				common = ((SpoutSpec) component).get_common();
				if (fromConf) {
					Integer paraNum = ConfigExtension.getSpoutParallelism(stormConf, componentName);
					if (paraNum != null) {
						LOG.info("Set " + componentName + " as " + paraNum);
						common.set_parallelism_hint(paraNum);
					}
				}
			}
			if (component instanceof StateSpoutSpec) {
				common = ((StateSpoutSpec) component).get_common();
				if (fromConf) {
					Integer paraNum = ConfigExtension.getSpoutParallelism(stormConf, componentName);
					if (paraNum != null) {
						LOG.info("Set " + componentName + " as " + paraNum);
						common.set_parallelism_hint(paraNum);
					}
				}
			}

			Map componentMap = new HashMap();

			String jsonConfString = common.get_json_conf();
			if (jsonConfString != null) {
				componentMap
						.putAll((Map) JStormUtils.from_json(jsonConfString));
			}

			Integer taskNum = componentParalism(stormConf, common);

			componentMap.put(Config.TOPOLOGY_TASKS, taskNum);
			// change the executor's task number
			common.set_parallelism_hint(taskNum);
			LOG.info("Set " + componentName + " parallelism " + taskNum);

			common.set_json_conf(JStormUtils.to_json(componentMap));
		}

		return ret;
	}

	/**
	 * clean the topology which is in ZK but not in local dir
	 * 
	 * @throws Exception
	 * 
	 */
	public static void cleanupCorruptTopologies(NimbusData data)
			throws Exception {

		StormClusterState stormClusterState = data.getStormClusterState();

		// get /local-storm-dir/nimbus/stormdist path
		String master_stormdist_root = StormConfig.masterStormdistRoot(data
				.getConf());

		// listdir /local-storm-dir/nimbus/stormdist
		List<String> code_ids = PathUtils
				.read_dir_contents(master_stormdist_root);

		// get topology in ZK /storms
		List<String> active_ids = data.getStormClusterState().active_storms();
		if (active_ids != null && active_ids.size() > 0) {
			if (code_ids != null) {
				// clean the topology which is in ZK but not in local dir
				active_ids.removeAll(code_ids);
			}

			for (String corrupt : active_ids) {
				LOG.info("Corrupt topology "
						+ corrupt
						+ " has state on zookeeper but doesn't have a local dir on Nimbus. Cleaning up...");

				/**
				 * Just removing the /STORMS is enough
				 * 
				 */
				stormClusterState.remove_storm(corrupt);
			}
		}

		LOG.info("Successfully cleanup all old toplogies");

	}

	
	public static boolean isTaskDead(NimbusData data, String topologyId,
			Integer taskId) {
		String idStr = " topology:" + topologyId + ",taskid:" + taskId;

		Integer zkReportTime = null;

		StormClusterState stormClusterState = data.getStormClusterState();
		TaskHeartbeat zkTaskHeartbeat = null;
		try {
			zkTaskHeartbeat = stormClusterState.task_heartbeat(topologyId,
					taskId);
			if (zkTaskHeartbeat != null) {
				zkReportTime = zkTaskHeartbeat.getTimeSecs();
			}
		} catch (Exception e) {
			LOG.error("Failed to get ZK task hearbeat " + idStr, e);
			return true;
		}

		Map<Integer, TkHbCacheTime> taskHBs = data.getTaskHeartbeatsCache()
				.get(topologyId);
		if (taskHBs == null) {
			LOG.info("No task heartbeat cache " + topologyId);

			// update task hearbeat cache
			taskHBs = new HashMap<Integer, TkHbCacheTime>();

			data.getTaskHeartbeatsCache().put(topologyId, taskHBs);
		}

		TkHbCacheTime taskHB = taskHBs.get(taskId);
		if (taskHB == null) {
			LOG.info("No task heartbeat cache " + idStr);

			if (zkTaskHeartbeat == null) {
				LOG.info("No ZK task hearbeat " + idStr);
				return true;
			}

			taskHB = new TkHbCacheTime();
			taskHB.update(zkTaskHeartbeat);

			taskHBs.put(taskId, taskHB);

			return false;
		}

		if (zkReportTime == null) {
			LOG.debug("No ZK task heartbeat " + idStr);
			// Task hasn't finish init
			int nowSecs = TimeUtils.current_time_secs();
			int assignSecs = taskHB.getTaskAssignedTime();

			int waitInitTimeout = JStormUtils.parseInt(data.getConf().get(
					Config.NIMBUS_TASK_LAUNCH_SECS));

			if (nowSecs - assignSecs > waitInitTimeout) {
				LOG.info(idStr + " failed to init ");
				return true;
			} else {
				return false;
			}

		}

		// the left is zkReportTime isn't null
		// task has finished initialization
		int nimbusTime = taskHB.getNimbusTime();
		int reportTime = taskHB.getTaskReportedTime();

		int nowSecs = TimeUtils.current_time_secs();
		if (nimbusTime == 0) {
			// taskHB no entry, first time
			// update taskHB
			taskHB.setNimbusTime(nowSecs);
			taskHB.setTaskReportedTime(zkReportTime);

			LOG.info("Update taskheartbeat to nimbus cache " + idStr);
			return false;
		}

		if (reportTime != zkReportTime.intValue()) {
			// zk has been updated the report time
			taskHB.setNimbusTime(nowSecs);
			taskHB.setTaskReportedTime(zkReportTime);

			LOG.debug(idStr + ",nimbusTime " + nowSecs + ",zkReport:"
					+ zkReportTime + ",report:" + reportTime);
			return false;
		}

		// the following is (zkReportTime == reportTime)
		int taskHBTimeout = JStormUtils.parseInt(data.getConf().get(
				Config.NIMBUS_TASK_TIMEOUT_SECS));

		if (nowSecs - nimbusTime > taskHBTimeout) {
			// task is dead
			long ts = ((long) nimbusTime) * 1000;
			Date lastTaskHBDate = new Date(ts);
			StringBuilder sb = new StringBuilder();

			sb.append(idStr);
			sb.append(" last tasktime is ");
			sb.append(nimbusTime);
			sb.append(":").append(lastTaskHBDate);
			sb.append(",current ");
			sb.append(nowSecs);
			sb.append(":").append(new Date(((long) nowSecs) * 1000));
			LOG.info(sb.toString());
			return true;
		}

		return false;

	}

	public static void updateTaskHbStartTime(NimbusData data,
			Assignment assignment, String topologyId) {
		Map<Integer, TkHbCacheTime> taskHBs = data.getTaskHeartbeatsCache()
				.get(topologyId);

		if (taskHBs == null) {
			taskHBs = new HashMap<Integer, TkHbCacheTime>();
			data.getTaskHeartbeatsCache().put(topologyId, taskHBs);
		}

		Map<Integer, Integer> taskStartTimes = assignment
				.getTaskStartTimeSecs();
		for (Entry<Integer, Integer> entry : taskStartTimes.entrySet()) {
			Integer taskId = entry.getKey();
			Integer taskStartTime = entry.getValue();

			TkHbCacheTime taskHB = taskHBs.get(taskId);
			if (taskHB == null) {
				taskHB = new TkHbCacheTime();
				taskHBs.put(taskId, taskHB);
			}

			taskHB.setTaskAssignedTime(taskStartTime);
		}

		return;
	}

	public static <T> void transitionName(NimbusData data, String topologyName,
			boolean errorOnNoTransition, StatusType transition_status,
			T... args) throws Exception {
		StormClusterState stormClusterState = data.getStormClusterState();
		String topologyId = Cluster.get_topology_id(stormClusterState,
				topologyName);
		if (topologyId == null) {
			throw new NotAliveException(topologyName);
		}
		transition(data, topologyId, errorOnNoTransition, transition_status,
				args);
	}

	public static <T> void transition(NimbusData data, String topologyid,
			boolean errorOnNoTransition, StatusType transition_status,
			T... args) {
		try {
			data.getStatusTransition().transition(topologyid,
					errorOnNoTransition, transition_status, args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.error("Failed to do status transition,", e);
		}
	}

	public static TopologySummary mkTopologySummary(Assignment assignment,
			String topologyId, String topologyName, String status,
			int uptime_secs, Map<Integer, String> lastErrTimeStamp) {

		int num_workers = assignment.getWorkers().size();
		int num_tasks = 0;

		for (ResourceWorkerSlot worker : assignment.getWorkers()) {
			num_tasks = num_tasks + worker.getTasks().size();
		}
		
		long currentTimeSecs = System.currentTimeMillis() / 1000;
		String errorInfo = "";
		if (lastErrTimeStamp != null)
		{
			for (Entry<Integer, String> entry : lastErrTimeStamp.entrySet()) {
		        if ((currentTimeSecs - Long.valueOf(entry.getValue())) < entry.getKey()) {
			        errorInfo = "Y";
			        break;
		        }
			}
		}

		TopologySummary ret = new TopologySummary(topologyId, topologyName,
				status, uptime_secs, num_tasks, num_workers, errorInfo);

		return ret;
	}

	public static SupervisorSummary mkSupervisorSummary(
			SupervisorInfo supervisorInfo, String supervisorId,
			Map<String, Integer> supervisorToUsedSlotNum) {
		Integer usedNum = supervisorToUsedSlotNum.get(supervisorId);

		SupervisorSummary summary = new SupervisorSummary(
				supervisorInfo.getHostName(), supervisorId,
				supervisorInfo.getUptimeSecs(), supervisorInfo.getWorkerPorts()
						.size(), usedNum == null ? 0 : usedNum);

		return summary;
	}

	public static List<SupervisorSummary> mkSupervisorSummaries(
			Map<String, SupervisorInfo> supervisorInfos,
			Map<String, Assignment> assignments) {

		Map<String, Integer> supervisorToLeftSlotNum = new HashMap<String, Integer>();
		for (Entry<String, Assignment> entry : assignments.entrySet()) {
			Set<ResourceWorkerSlot> workers = entry.getValue().getWorkers();

			for (ResourceWorkerSlot worker : workers) {

				String supervisorId = worker.getNodeId();
				SupervisorInfo supervisorInfo = supervisorInfos
						.get(supervisorId);
				if (supervisorInfo == null) {
					continue;
				}
				Integer slots = supervisorToLeftSlotNum.get(supervisorId);
				if (slots == null) {
					slots = 0;
					supervisorToLeftSlotNum.put(supervisorId, slots);
				}
				supervisorToLeftSlotNum.put(supervisorId, ++slots);
			}
		}

		List<SupervisorSummary> ret = new ArrayList<SupervisorSummary>();
		for (Entry<String, SupervisorInfo> entry : supervisorInfos.entrySet()) {
			String supervisorId = entry.getKey();
			SupervisorInfo supervisorInfo = entry.getValue();

			SupervisorSummary summary = mkSupervisorSummary(supervisorInfo,
					supervisorId, supervisorToLeftSlotNum);

			ret.add(summary);
		}

		Collections.sort(ret, new Comparator<SupervisorSummary>() {

			@Override
			public int compare(SupervisorSummary o1, SupervisorSummary o2) {

				return o1.get_host().compareTo(o2.get_host());
			}

		});
		return ret;
	}

	public static TaskSummary mkSimpleTaskSummary(ResourceWorkerSlot resource,
			int taskId, String component, String componentType, String host, int uptime) {
		TaskSummary ret = new TaskSummary();

		ret.set_task_id(taskId);
		ret.set_component_id(component);
		ret.set_component_type(componentType);
		ret.set_host(host);
		ret.set_port(resource.getPort());
		ret.set_uptime_secs(uptime);
		ret.set_errors(new ArrayList<ErrorInfo>());

		return ret;
	}

	public static Map<Integer, TaskSummary> mkTaskSummary(
			StormClusterState zkClusterState, Assignment assignment,
			Map<Integer, String> taskToComponent, String topologyId)
			throws Exception {

		Map<Integer, TaskSummary> taskSummaries = new TreeMap<Integer, TaskSummary>();

		Set<ResourceWorkerSlot> workers = assignment.getWorkers();

		for (ResourceWorkerSlot worker : workers) {
			for (Integer taskId : worker.getTasks()) {
				TaskSummary taskSummary = new TaskSummary();
				
				taskSummary.set_task_id(taskId);
				taskSummary.set_component_id(taskToComponent.get(taskId));
				taskSummary.set_host(worker.getHostname());
				taskSummary.set_port(worker.getPort());
				
				List<TaskError> errors = zkClusterState.task_errors(topologyId,
				  	    taskId);
			    List<ErrorInfo> newErrors = new ArrayList<ErrorInfo>();
			    if (errors != null) {
				    int size = errors.size();
				    for (int i = 0; i < size; i++) {
					    TaskError e = (TaskError) errors.get(i);
					    newErrors.add(new ErrorInfo(e.getError(), e
							    .getTimSecs()));
				    }
			    }
			    taskSummary.set_errors(newErrors);
				
				TaskHeartbeat heartbeat = zkClusterState.task_heartbeat(
						topologyId, taskId);
				if (heartbeat == null) {
					LOG.warn("Topology " + topologyId + " task " + taskId
							+ " hasn't been started");
					taskSummary.set_status(ConfigExtension.TASK_STATUS_STARTING);
				} else {
				    taskSummary.set_uptime_secs(heartbeat.getUptimeSecs());
				    taskSummary.set_stats(heartbeat.getStats().getTaskStats());
				    taskSummary.set_status(ConfigExtension.TASK_STATUS_ACTIVE);
				}

				taskSummaries.put(taskId, taskSummary);
			}
		}

		return taskSummaries;
	}

	public static List<WorkerSummary> mkWorkerSummary(String topology,
			Assignment assignment, Map<Integer, TaskSummary> taskSumm) {
		Set<ResourceWorkerSlot> workers = assignment.getWorkers();
		List<WorkerSummary> result = new ArrayList<WorkerSummary>();
		for (ResourceWorkerSlot worker : workers) {
			WorkerSummary workerSumm = new WorkerSummary();
			workerSumm.set_topology(topology);
			workerSumm.set_port(worker.getPort());
			List<TaskSummary> tasks = new ArrayList<TaskSummary>();
			workerSumm.set_tasks(tasks);
			for (Integer taskId : worker.getTasks()) {
				TaskSummary task = taskSumm.get(taskId);
				if (task == null)
					continue;
				tasks.add(task);
			}
			result.add(workerSumm);
		}
		return result;
	}
	
	public static void updateMetricMonitorStatus(StormClusterState clusterState, 
			String topologyId, boolean isEnable) throws Exception {
		StormMonitor stormMonitor = new StormMonitor(isEnable);
		clusterState.set_storm_monitor(topologyId, stormMonitor);
	}
	
	public static void updateMetricsInfo(NimbusData data, String topologyId, 
			Assignment assignment) {
		List<Integer> taskList = new ArrayList<Integer>();
		List<String> workerList = new ArrayList<String>();
		
		StormClusterState clusterState = data.getStormClusterState();
		
		Set<ResourceWorkerSlot> workerSlotSet = assignment.getWorkers();
		
		for (ResourceWorkerSlot workerSlot : workerSlotSet) {
			String workerId = workerSlot.getHostname() + ":" + workerSlot.getPort();
			workerList.add(workerId);
			
			taskList.addAll(workerSlot.getTasks());
		}
		
		try {
		    //Remove the obsolete tasks of metrics monitor in ZK
		    List<String> metricTaskList = clusterState.get_metric_taskIds(topologyId);
		    for (String task : metricTaskList) {
		    	Integer taskId = Integer.valueOf(task);
			    if(taskList.contains(taskId) == false)
			    	clusterState.remove_metric_task(topologyId, String.valueOf(taskId));
		    }
		    
		    //Remove the obsolete workers of metrics monitor in ZK
		    List<String> metricWorkerList = clusterState.get_metric_workerIds(topologyId);
		    for (String workerId : metricWorkerList) {
		    	if (workerList.contains(workerId) == false)
		    		clusterState.remove_metric_worker(topologyId, workerId);
		    }
		    
		    //Remove the obsolete user workers of metrics monitor in ZK
		    List<String> metricUserList = clusterState.get_metric_users(topologyId);
		    for (String workerId : metricUserList) {
		    	if (workerList.contains(workerId) == false)
		    		clusterState.remove_metric_user(topologyId, workerId);
		    }
		} catch (Exception e) {
			LOG.error("Failed to update metrics info when rebalance or reassignment, topologyId=" + 
		              topologyId, e);
		}
	}
	
	public static void updateTaskMetricData(TaskMetricData metricData, TaskMetricInfo metricInfo) {
		metricData.set_task_id(Integer.valueOf(metricInfo.getTaskId()));
		metricData.set_component_id(metricInfo.getComponent());
		metricData.set_gauge(metricInfo.getGaugeData());
		metricData.set_counter(metricInfo.getCounterData());
		metricData.set_meter(metricInfo.getMeterData());
		metricData.set_timer(metricInfo.getTimerData());
		metricData.set_histogram(metricInfo.getHistogramData());
	}
	
	public static void updateWorkerMetricData(WorkerMetricData metricData, WorkerMetricInfo metricInfo) {
		metricData.set_hostname(metricInfo.getHostName());
		metricData.set_port(metricInfo.getPort());
		metricData.set_gauge(metricInfo.getGaugeData());
		metricData.set_counter(metricInfo.getCounterData());
		metricData.set_meter(metricInfo.getMeterData());
		metricData.set_timer(metricInfo.getTimerData());
		metricData.set_histogram(metricInfo.getHistogramData());
		
		//Add cpu and Mem into gauge map
		Map<String, Double> gaugeMap = metricData.get_gauge();
		gaugeMap.put(MetricDef.CPU_USED_RATIO, metricInfo.getUsedCpu());
		gaugeMap.put(MetricDef.MEMORY_USED,((Long) metricInfo.getUsedMem()).doubleValue());
	}
	
	public static String getNimbusVersion() {
		String ret = null;
		
		String path = System.getProperty("jstorm.home") + "/RELEASE";
		File file = new File(path);
		FileReader reader = null;
        Closeable resource = reader;
		
		try{
		    reader = new FileReader(file);
		    BufferedReader bufferedReader = new BufferedReader(reader);
            resource = bufferedReader;
		    ret = bufferedReader.readLine();
		} catch (Exception e) {
			LOG.warn("Failed to get nimbus version", e);
		} finally {
			if (resource != null) {
				try {
					resource.close();
				} catch (Exception e) {
					LOG.error("Failed to close the reader of RELEASE", e);
				}
			}
		}
		
		return ret;
	}
}

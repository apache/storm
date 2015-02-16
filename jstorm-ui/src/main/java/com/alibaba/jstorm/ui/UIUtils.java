package com.alibaba.jstorm.ui;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import javax.faces.context.FacesContext;

import org.apache.commons.lang.StringUtils;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.transport.TFramedTransport;
import org.apache.thrift7.transport.TSocket;
import org.apache.thrift7.transport.TTransportException;
import org.yaml.snakeyaml.Yaml;

import backtype.storm.Config;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.ErrorInfo;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TaskStats;
import backtype.storm.generated.TaskSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;
import backtype.storm.generated.WorkerSummary;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.common.stats.StatBuckets;
import com.alibaba.jstorm.common.stats.StaticsType;
import com.alibaba.jstorm.ui.model.ClusterSumm;
import com.alibaba.jstorm.ui.model.ComponentTask;
import com.alibaba.jstorm.ui.model.Components;
import com.alibaba.jstorm.ui.model.SupervisorSumm;
import com.alibaba.jstorm.ui.model.TopologySumm;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;

public class UIUtils {

	private static int maxErrortime = 1800;
	private static int maxErrornum = 200;

	public static String getWindow(FacesContext ctx) {
		String window = null;
		if (ctx.getExternalContext().getRequestParameterMap().get("window") != null) {
			window = (String) ctx.getExternalContext().getRequestParameterMap()
					.get("window");
		}

		return StatBuckets.getTimeKey(window);
	}

	public static List nimbusClientandConn(String host, Integer port)
			throws TTransportException {
		TSocket ts = new TSocket(host, port);
		TFramedTransport tt = new TFramedTransport(ts);
		TBinaryProtocol prot = new TBinaryProtocol(tt);
		Client nc = new Client(prot);
		ts.open();
		List l = new ArrayList();
		l.add(nc);
		l.add(tt);
		return l;
	}

	public static final String SPOUT_STR = "spout";
	public static final String BOLT_STR = "bolt";

	public static String componentType(StormTopology topology, String id) {
		Map<String, Bolt> bolts = topology.get_bolts();
		Map<String, SpoutSpec> spouts = topology.get_spouts();
		String type = "";
		if (bolts.containsKey(id)) {
			type = BOLT_STR;
		} else if (spouts.containsKey(id)) {
			type = SPOUT_STR;
		}
		return type;
	}

	private List<Double> addPairs(ArrayList<Double> p1, ArrayList<Double> p2) {
		if (p1 == null || p2 == null) {
			return null;
		} else {
			int p1Size = p1.size();
			int p2Size = p2.size();
			if (p1Size != p2Size) {
				return null;
			} else {
				List<Double> rtn = new ArrayList<Double>();
				for (int i = 0; i < p1Size; i++) {
					rtn.set(i, p1.get(i) + p2.get(i));
				}
				return rtn;
			}
		}

	}

	public static void addTask(Map<String, List<TaskSummary>> taskMap,
			TaskSummary t, String componentId) {

		List<TaskSummary> taskList = taskMap.get(componentId);

		if (taskList == null) {
			taskList = new ArrayList<TaskSummary>();
			taskMap.put(componentId, taskList);
		}

		taskList.add(t);
	}

	/**
	 * Merge stream value
	 * 
	 * @param rawMap
	 *            Map<String, Map<K, V>> rawMap
	 * @param keySample
	 * @param zeroValue
	 *            0 Value
	 * @return Map<String, V>
	 */
	public static <K, V> Map<String, V> mergeStream(
			Map<String, Map<K, V>> rawMap, V zeroValue) {
		Map<String, V> ret = new HashMap<String, V>();

		for (Entry<String, Map<K, V>> rawEntry : rawMap.entrySet()) {
			String window = rawEntry.getKey();
			Map<K, V> streamMap = rawEntry.getValue();

			V retValue = zeroValue;

			if (streamMap != null) {
				for (Entry<K, V> streamEntry : streamMap.entrySet()) {
					K entry = streamEntry.getKey();
					V counter = streamEntry.getValue();

					retValue = (V) JStormUtils.add(retValue, counter);
				}
			}

			ret.put(window, retValue);
		}

		return ret;
	}

	public static Map<StaticsType, List<Object>> mergeStream(
			List<TaskSummary> taskSummaries, String window) {

		Map<StaticsType, List<Object>> ret = new HashMap<StaticsType, List<Object>>();

		List<Object> emitted = new ArrayList<Object>();
		List<Object> sendTps = new ArrayList<Object>();
		List<Object> recvTps = new ArrayList<Object>();
		List<Object> acked = new ArrayList<Object>();
		List<Object> failed = new ArrayList<Object>();
		List<Object> process = new ArrayList<Object>();

		ret.put(StaticsType.emitted, emitted);
		ret.put(StaticsType.send_tps, sendTps);
		ret.put(StaticsType.recv_tps, recvTps);
		ret.put(StaticsType.acked, acked);
		ret.put(StaticsType.failed, failed);
		ret.put(StaticsType.process_latencies, process);

		for (TaskSummary taskSummary : taskSummaries) {
			TaskStats taskStats = taskSummary.get_stats();

			if (taskStats == null) {
				continue;
			}

			Map<String, Long> emittedMap = mergeStream(taskStats.get_emitted(),
					Long.valueOf(0));
			emitted.add(emittedMap.get(window));

			Map<String, Double> rendTpsMap = mergeStream(
					taskStats.get_send_tps(), Double.valueOf(0));
			sendTps.add(rendTpsMap.get(window));

			Map<String, Double> recvTpsMap = mergeStream(
					taskStats.get_recv_tps(), Double.valueOf(0));
			recvTps.add(recvTpsMap.get(window));

			Map<String, Long> ackedMap = mergeStream(taskStats.get_acked(),
					Long.valueOf(0));
			acked.add(ackedMap.get(window));

			Map<String, Long> failedMap = mergeStream(taskStats.get_failed(),
					Long.valueOf(0));
			failed.add(failedMap.get(window));

			Map<String, Double> processMap = mergeStream(
					taskStats.get_process_ms_avg(), Double.valueOf(0));
			process.add(processMap.get(window));
		}

		return ret;

	}

	public static Map<StaticsType, Object> mergeTasks(
			List<TaskSummary> taskSummaries, String window) {
		Map<StaticsType, Object> ret = new HashMap<StaticsType, Object>();

		Map<StaticsType, List<Object>> mergedStreamTasks = mergeStream(
				taskSummaries, window);
		for (Entry<StaticsType, List<Object>> entry : mergedStreamTasks
				.entrySet()) {
			StaticsType type = entry.getKey();
			List<Object> valueList = entry.getValue();

			Object valueSum = JStormUtils.mergeList(valueList);

			ret.put(type, valueSum);
		}

		// special handle process
		Object value = ret.get(StaticsType.process_latencies);
		if (value != null && taskSummaries.size() > 0) {
			Double process = (Double) value;
			process = process / taskSummaries.size();
			ret.put(StaticsType.process_latencies, process);
		}

		return ret;
	}

	public static Components getComponent(List<TaskSummary> taskSummaries,
			String componentId, String type, String window) {

		Map<StaticsType, Object> staticsType = UIUtils.mergeTasks(
				taskSummaries, window);

		Components component = new Components();
		component.setType(type);
		component.setComponetId(componentId);
		component.setParallelism(String.valueOf(taskSummaries.size()));
		component.setValues(staticsType);

		return component;
	}

	/**
	 * Convert thrift TaskSummary to UI bean ComponentTask
	 * 
	 * @param summ
	 * @return
	 */
	public static ComponentTask getComponentTask(TaskSummary task,
			String topologyid) {

		ComponentTask componentTask = new ComponentTask();

		componentTask.setComponentid(task.get_component_id());
		componentTask.setTaskid(String.valueOf(task.get_task_id()));
		componentTask.setHost(task.get_host());
		componentTask.setPort(String.valueOf(task.get_port()));
		
		
		componentTask.setStatus(task.get_status());
		if (componentTask.getStatus() == null) {
			// This is for old jstorm version
			componentTask.setStatus(ConfigExtension.TASK_STATUS_ACTIVE);
		}
		
		if (componentTask.getStatus().equals(ConfigExtension.TASK_STATUS_ACTIVE)) {
		    componentTask.setUptime(StatBuckets.prettyUptimeStr(task
				    .get_uptime_secs()));
		    componentTask.setLastErr(UIUtils.getTaskError(task.get_errors()));
		}

		componentTask.setIp(NetWorkUtils.host2Ip(task.get_host()));

		componentTask.setTopologyid(topologyid);

		return componentTask;
	}

	public static List<TaskSummary> getTaskList(
			List<TaskSummary> taskSummaries, String componentId) {
		List<TaskSummary> ret = new ArrayList<TaskSummary>();

		for (TaskSummary task : taskSummaries) {
			if (componentId.equals(task.get_component_id())) {
				ret.add(task);
			}
		}

		return ret;
	}

	public static String mostRecentError(List<TaskSummary> summarys) {
		String rtn = "";
		int summarysSzie = 0;
		if (summarys != null) {
			summarysSzie = summarys.size();
		}
		for (int i = 0; i < summarysSzie; i++) {
			List<ErrorInfo> einfos = summarys.get(i).get_errors();
			rtn += getTaskError(einfos);
		}

		return rtn;
	}

	public static String getTaskError(List<ErrorInfo> errList) {
		if (errList == null) {
			return "";
		}

		List<String> errors = new ArrayList<String>();

		for (ErrorInfo einfo : errList) {
			long current = System.currentTimeMillis() / 1000;
			
			//shorten the most recent time for "queue is full" error
			int maxTime = JStormUtils.MIN_30;
			if (einfo.get_error().indexOf("queue is full") != -1)
				maxTime = JStormUtils.MIN_1*3;
			else if (einfo.get_error().indexOf("is dead on") != -1)
				maxTime = JStormUtils.DAY_1*3;
			
			if (current - einfo.get_error_time_secs() < maxTime) {
				errors.add(einfo.get_error());
			}
		}

		String rtn = "";
		int size = 0;
		for (String e : errors) {
			if (size >= maxErrornum) {
				break;
			}
			rtn += e + ";";
			size++;
		}
		return rtn;
	}

	/**
	 * Convert thrift TopologySummary to UI bean TopologySumm
	 * 
	 * @param ts
	 * @return
	 */
	public static List<TopologySumm> topologySummary(List<TopologySummary> ts) {

		List<TopologySumm> tsumm = new ArrayList<TopologySumm>();
		if (ts != null) {
			for (TopologySummary t : ts) {

				TopologySumm topologySumm = new TopologySumm();
				topologySumm.setTopologyId(t.get_id());
				topologySumm.setTopologyName(t.get_name());

				topologySumm.setStatus(t.get_status());
				topologySumm.setUptime(StatBuckets.prettyUptimeStr(t
						.get_uptime_secs()));

				topologySumm.setNumWorkers(String.valueOf(t.get_num_workers()));
				topologySumm.setNumTasks(String.valueOf(t.get_num_tasks()));
				
				topologySumm.setErrorInfo(t.get_error_info());
				tsumm.add(topologySumm);
			}
		}
		return tsumm;
	}

	/**
	 * Convert thrift TopologyInfo to UI bean TopologySumm
	 * 
	 * @param summ
	 * @return
	 */
	public static List<TopologySumm> topologySummary(TopologyInfo t) {

		List<WorkerSummary> workers = t.get_workers();
		int taskNum = 0;
		int memNum = 0;
		for (WorkerSummary worker : workers) {
			taskNum += worker.get_tasks_size();
		}

		List<TopologySumm> tsumm = new ArrayList<TopologySumm>();

		// TopologySumm ts = new TopologySumm(summ.get_name(), summ.get_id(),
		// summ.get_status(), StatBuckets.prettyUptimeStr(summ
		// .get_uptime_secs()), String.valueOf(workers.size()),
		// String.valueOf(taskSize), summ.get_uptime_secs());

		TopologySumm topologySumm = new TopologySumm();
		topologySumm.setTopologyId(t.get_id());
		topologySumm.setTopologyName(t.get_name());
		topologySumm.setStatus(t.get_status());
		topologySumm
				.setUptime(StatBuckets.prettyUptimeStr(t.get_uptime_secs()));

		topologySumm.setNumWorkers(String.valueOf(workers.size()));
		topologySumm.setNumTasks(String.valueOf(taskNum));

		tsumm.add(topologySumm);
		return tsumm;
	}

	/**
	 * Connvert thrift ClusterSummary to UI bean ClusterSumm
	 * 
	 * @param summ
	 * @return
	 */
	public static List<ClusterSumm> clusterSummary(ClusterSummary summ,
			NimbusClient client, Map conf) throws Exception {
		// "Supervisors" "Used slots" "Free slots" "Total slots" "Running task"
		List<SupervisorSummary> sups = summ.get_supervisors();
		int supSize = 0;

		int totalMemSlots = 0;
		int useMemSlots = 0;
		int freeMemSlots = 0;

		int totalPortSlots = 0;
		int usePortSlots = 0;
		int freePortSlots = 0;

		if (sups != null) {
			supSize = sups.size();
			for (SupervisorSummary ss : sups) {

				totalPortSlots += ss.get_num_workers();
				usePortSlots += ss.get_num_used_workers();
			}

			freeMemSlots = totalMemSlots - useMemSlots;
			freePortSlots = totalPortSlots - usePortSlots;
		}

		// "Running tasks"
		int totalTasks = 0;
		List<TopologySummary> topos = summ.get_topologies();
		if (topos != null) {
			int topoSize = topos.size();
			for (int j = 0; j < topoSize; j++) {
				totalTasks += topos.get(j).get_num_tasks();
			}

		}

		String nimbustime = StatBuckets.prettyUptimeStr(summ
				.get_nimbus_uptime_secs());

		List<ClusterSumm> clusumms = new ArrayList<ClusterSumm>();

		ClusterSumm clusterSumm = new ClusterSumm();
		String master = client.getMasterHost();
		
		if (master.contains(":")) {
			String firstPart = master.substring(0, master.indexOf(":") );
			String lastPart = master.substring(master.indexOf(":"));
			clusterSumm.setNimbusHostname(NetWorkUtils.ip2Host(firstPart) + lastPart);
			clusterSumm.setNimbusIp(NetWorkUtils.host2Ip(firstPart));
		} else {
			clusterSumm.setNimbusHostname(master);
			clusterSumm.setNimbusIp(NetWorkUtils.host2Ip(master));
		}
		int port = ConfigExtension.getNimbusDeamonHttpserverPort(conf);
		clusterSumm.setNimbusLogPort(String.valueOf(port));
		clusterSumm.setNimbusUptime(nimbustime);
		clusterSumm.setSupervisorNum(String.valueOf(supSize));
		clusterSumm.setRunningTaskNum(String.valueOf(totalTasks));

		clusterSumm.setTotalPortSlotNum(String.valueOf(totalPortSlots));
		clusterSumm.setUsedPortSlotNum(String.valueOf(usePortSlots));
		clusterSumm.setFreePortSlotNum(String.valueOf(freePortSlots));
		
		clusterSumm.setVersion(summ.get_version());

		clusumms.add(clusterSumm);
		return clusumms;
	}

	/**
	 * Convert thrift SupervisorSummary to UI bean SupervisorSumm
	 * 
	 * @param ss
	 * @return
	 */
	public static List<SupervisorSumm> supervisorSummary(
			List<SupervisorSummary> ss) {
		// uptime host slots usedslots

		List<SupervisorSumm> ssumm = new ArrayList<SupervisorSumm>();

		if (ss == null) {
			ss = new ArrayList<SupervisorSummary>();
		}

		for (SupervisorSummary s : ss) {
			SupervisorSumm ssum = new SupervisorSumm(s);

			ssumm.add(ssum);
		}

		return ssumm;
	}

	public static Map readUiConfig() {
		Map ret = Utils.readStormConfig();
		String curDir = System.getProperty("user.home");
		String confPath = curDir + File.separator + ".jstorm" + File.separator
				+ "storm.yaml";
		File file = new File(confPath);
		if (file.exists()) {

			FileInputStream fileStream;
			try {
				fileStream = new FileInputStream(file);
				Yaml yaml = new Yaml();

				Map clientConf = (Map) yaml.load(fileStream);

				if (clientConf != null) {
					ret.putAll(clientConf);
				}
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
			}

		}
		if (ret.containsKey(Config.NIMBUS_HOST) == false) {
			ret.put(Config.NIMBUS_HOST, "localhost");

		}
		return ret;
	}
	
	public static double getDoubleValue(Double value) {
		double ret = (value != null ? value.doubleValue() : 0.0);
		return ret;
	}

	public static void getClusterInfoByName(Map conf, String clusterName) {
		List<Map> uiClusters = ConfigExtension.getUiClusters(conf);
		Map cluster = ConfigExtension.getUiClusterInfo(
				uiClusters, clusterName);
		
		conf.put(Config.STORM_ZOOKEEPER_ROOT, 
				ConfigExtension.getUiClusterZkRoot(cluster));
		conf.put(Config.STORM_ZOOKEEPER_SERVERS, 
				ConfigExtension.getUiClusterZkServers(cluster));
		conf.put(Config.STORM_ZOOKEEPER_PORT, 
				ConfigExtension.getUiClusterZkPort(cluster));
	}
	
	public static NimbusClient getNimbusClient(Map conf, String clusterName) throws Exception{
		if(StringUtils.isBlank(clusterName) == false) {
			getClusterInfoByName(conf, clusterName);
		}
		return NimbusClient.getConfiguredClient(conf);
	}
	
	public static void main(String[] args) {
	}
}

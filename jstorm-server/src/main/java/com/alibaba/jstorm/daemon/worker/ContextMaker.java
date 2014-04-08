package com.alibaba.jstorm.daemon.worker;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import backtype.storm.generated.StormTopology;
import backtype.storm.generated.StreamInfo;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.ThriftTopologyUtils;

import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.PathUtils;

/**
 * ContextMaker This class is used to create TopologyContext
 * 
 * @author yannian/Longda
 * 
 */
public class ContextMaker {
	private static Logger LOG = Logger.getLogger(ContextMaker.class);

	private WorkerData workerData;

	private String resourcePath;
	private String pidDir;
	private String codeDir;
	private List<Integer> workerTasks;

	@SuppressWarnings("rawtypes")
	public ContextMaker(WorkerData workerData) {
		/*
		 * Map stormConf, String topologyId, String workerId, HashMap<Integer,
		 * String> tasksToComponent, Integer port, List<Integer> workerTasks
		 */
		this.workerData = workerData;
		this.workerTasks = JStormUtils.mk_list(workerData.getTaskids());

		try {
			Map stormConf = workerData.getStormConf();
			String topologyId = workerData.getTopologyId();
			String workerId = workerData.getWorkerId();

			String distroot = StormConfig.supervisor_stormdist_root(stormConf,
					topologyId);

			resourcePath = StormConfig
					.supervisor_storm_resources_path(distroot);

			pidDir = StormConfig.worker_pids_root(stormConf, workerId);
			

			String codePath = StormConfig.stormcode_path(distroot);
			codeDir = PathUtils.parent_path(codePath);

		} catch (IOException e) {
			LOG.error("Failed to create ContextMaker", e);
			throw new RuntimeException(e);
		}
	}

	public TopologyContext makeTopologyContext(StormTopology topology,
			Integer taskId, clojure.lang.Atom openOrPrepareWasCalled) {

		Map stormConf = workerData.getStormConf();
		String topologyId = workerData.getTopologyId();

		HashMap<String, Map<String, Fields>> componentToStreamToFields = new HashMap<String, Map<String, Fields>>();

		Set<String> components = ThriftTopologyUtils.getComponentIds(topology);
		for (String component : components) {

			Map<String, Fields> streamToFieldsMap = new HashMap<String, Fields>();

			Map<String, StreamInfo> streamInfoMap = ThriftTopologyUtils
					.getComponentCommon(topology, component).get_streams();
			for (Entry<String, StreamInfo> entry : streamInfoMap.entrySet()) {
				String streamId = entry.getKey();
				StreamInfo streamInfo = entry.getValue();

				streamToFieldsMap.put(streamId,
						new Fields(streamInfo.get_output_fields()));
			}

			componentToStreamToFields.put(component, streamToFieldsMap);
		}

		return new TopologyContext(topology, stormConf,
				workerData.getTasksToComponent(),
				workerData.getComponentToSortedTasks(),
				componentToStreamToFields, topologyId, resourcePath, pidDir,
				taskId, workerData.getPort(), workerTasks,
				workerData.getDefaultResources(),
				workerData.getUserResources(), workerData.getExecutorData(),
				workerData.getRegisteredMetrics(), openOrPrepareWasCalled);

	}

}

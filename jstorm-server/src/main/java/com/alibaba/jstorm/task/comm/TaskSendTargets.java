package com.alibaba.jstorm.task.comm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;

import com.alibaba.jstorm.stats.CommonStatsRolling;
import com.alibaba.jstorm.task.group.GrouperType;
import com.alibaba.jstorm.task.group.MkGrouper;
import com.alibaba.jstorm.utils.JStormUtils;

/**
 * 
 * tuple sending object, which get which task should tuple be send to, and
 * update statics
 * 
 * @author yannian/Longda
 * 
 */
public class TaskSendTargets {
	private static Logger LOG = LoggerFactory.getLogger(TaskSendTargets.class);

	private Map<Object, Object> stormConf;
	// it is system TopologyContext
	private TopologyContext topologyContext;

	// <Stream_id,<component, Grouping>>
	private Map<String, Map<String, MkGrouper>> streamComponentgrouper;
	// SpoutTaskStatsRolling or BoltTaskStatsRolling
	private CommonStatsRolling taskStats;

	private Map<String, List<Integer>> componentTasks;
	private String componentId;
	private int taskId;
	private boolean isDebuging = false;
	private String debugIdStr;

	public TaskSendTargets(Map<Object, Object> _storm_conf, String _component,
			Map<String, Map<String, MkGrouper>> _stream_component_grouper,
			TopologyContext _topology_context,
			Map<String, List<Integer>> _component_tasks,
			CommonStatsRolling _task_stats) {
		this.stormConf = _storm_conf;
		this.componentId = _component;
		this.streamComponentgrouper = _stream_component_grouper;
		this.topologyContext = _topology_context;
		this.componentTasks = _component_tasks;
		this.taskStats = _task_stats;

		isDebuging = JStormUtils.parseBoolean(
				stormConf.get(Config.TOPOLOGY_DEBUG), false);

		taskId = topologyContext.getThisTaskId();
		debugIdStr = " Emit from " + componentId + ":" + taskId + " ";
	}

	// direct send tuple to special task
	public java.util.List<Integer> get(Integer out_task_id, String stream,
			List<Object> tuple) {

		// in order to improve acker's speed, skip checking
		// String target_component =
		// topologyContext.getComponentId(out_task_id);
		// Map<String, MkGrouper> component_prouping = streamComponentgrouper
		// .get(stream);
		// MkGrouper grouping = component_prouping.get(target_component);
		// if (grouping != null &&
		// !GrouperType.direct.equals(grouping.gettype())) {
		// throw new IllegalArgumentException(
		// "Cannot emitDirect to a task expecting a regular grouping");
		// }

		if (isDebuging) {
			LOG.info(debugIdStr + stream + " to " + out_task_id + ":"
					+ tuple.toString());
		}

		taskStats.send_tuple(stream, 1);

		java.util.List<Integer> out_tasks = new ArrayList<Integer>();
		out_tasks.add(out_task_id);
		return out_tasks;
	}

	// send tuple according to grouping
	public java.util.List<Integer> get(String stream, List<Object> tuple) {
		java.util.List<Integer> out_tasks = new ArrayList<Integer>();

		// get grouper, then get which task should tuple be sent to.
		Map<String, MkGrouper> componentCrouping = streamComponentgrouper
				.get(stream);
		if (componentCrouping == null) {
			// if the target component's parallelism is 0, don't need send to
			// them
			LOG.debug("Failed to get Grouper of " + stream + " in "
					+ debugIdStr);
			return out_tasks;
		}

		for (Entry<String, MkGrouper> ee : componentCrouping.entrySet()) {
			String targetComponent = ee.getKey();
			MkGrouper g = ee.getValue();

			if (GrouperType.direct.equals(g.gettype())) {
				throw new IllegalArgumentException(
						"Cannot do regular emit to direct stream");
			}

			out_tasks.addAll(g.grouper(tuple));

		}

		if (isDebuging) {

			LOG.info(debugIdStr + stream + " to " + out_tasks + ":"
					+ tuple.toString());
		}

		int num_out_tasks = out_tasks.size();

		taskStats.send_tuple(stream, num_out_tasks);

		return out_tasks;
	}

}

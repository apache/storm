package com.alibaba.jstorm.cluster;

import java.net.URLClassLoader;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import backtype.storm.Config;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.ComponentObject;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.JavaObject;
import backtype.storm.generated.ShellComponent;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StateSpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.StreamInfo;
import backtype.storm.spout.ShellSpout;
import backtype.storm.task.IBolt;
import backtype.storm.task.ShellBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.task.acker.Acker;
import com.alibaba.jstorm.task.group.MkGrouper;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.Thrift;

/**
 * Base utility function
 * 
 * 1. base topology validation 2. add streams/inputs
 * 
 * @author yannian/Longda
 * 
 */
public class Common {
	public static final String ACKER_COMPONENT_ID = Acker.ACKER_COMPONENT_ID;
	public static final String ACKER_INIT_STREAM_ID = Acker.ACKER_INIT_STREAM_ID;
	public static final String ACKER_ACK_STREAM_ID = Acker.ACKER_ACK_STREAM_ID;
	public static final String ACKER_FAIL_STREAM_ID = Acker.ACKER_FAIL_STREAM_ID;

	public static final String SYSTEM_STREAM_ID = "__system";

	public static final String LS_WORKER_HEARTBEAT = "worker-heartbeat";
	public static final String LS_ID = "supervisor-id";
	public static final String LS_LOCAL_ASSIGNMENTS = "local-assignments";
	public static final String LS_APPROVED_WORKERS = "approved-workers";

	public static boolean system_id(String id) {
		return Utils.isSystemId(id);
	}

	private static void validate_component(Object obj)
			throws InvalidTopologyException {

		if (obj instanceof StateSpoutSpec) {
			StateSpoutSpec spec = (StateSpoutSpec) obj;
			for (String id : spec.get_common().get_streams().keySet()) {
				if (system_id(id)) {
					throw new InvalidTopologyException(id
							+ " is not a valid component id");
				}
			}

		} else if (obj instanceof SpoutSpec) {
			SpoutSpec spec = (SpoutSpec) obj;
			for (String id : spec.get_common().get_streams().keySet()) {
				if (system_id(id)) {
					throw new InvalidTopologyException(id
							+ " is not a valid component id");
				}
			}
		} else if (obj instanceof Bolt) {
			Bolt spec = (Bolt) obj;
			for (String id : spec.get_common().get_streams().keySet()) {
				if (system_id(id)) {
					throw new InvalidTopologyException(id
							+ " is not a valid component id");
				}
			}
		} else {
			throw new InvalidTopologyException("Unknow type component");
		}

	}

	/**
	 * Check Whether ID of Bolt or spout is system_id
	 * 
	 * @param topology
	 * @throws InvalidTopologyException
	 */
	@SuppressWarnings("unchecked")
	public static void validate_ids(StormTopology topology)
			throws InvalidTopologyException {

		List<String> list = new ArrayList<String>();

		for (StormTopology._Fields field : Thrift.STORM_TOPOLOGY_FIELDS) {
			Object value = topology.getFieldValue(field);
			if (value != null) {
				Map<String, Object> obj_map = (Map<String, Object>) value;

				Set<String> commids = obj_map.keySet();

				for (String id : commids) {
					if (system_id(id)) {
						throw new InvalidTopologyException(id
								+ " is not a valid component id");
					}
				}

				for (Object obj : obj_map.values()) {
					validate_component(obj);
				}

				list.addAll(commids);
			}
		}

		List<String> offending = JStormUtils.getRepeat(list);
		if (offending.isEmpty() == false) {
			throw new InvalidTopologyException("Duplicate component ids: "
					+ offending);
		}

	}

	private static void validate_component_inputs(Object obj)
			throws InvalidTopologyException {
		if (obj instanceof StateSpoutSpec) {
			StateSpoutSpec spec = (StateSpoutSpec) obj;
			if (!spec.get_common().get_inputs().isEmpty()) {
				throw new InvalidTopologyException(
						"May not declare inputs for a spout");
			}

		}

		if (obj instanceof SpoutSpec) {
			SpoutSpec spec = (SpoutSpec) obj;
			if (!spec.get_common().get_inputs().isEmpty()) {
				throw new InvalidTopologyException(
						"May not declare inputs for a spout");
			}
		}
	}

	/**
	 * Validate the topology 1. component id name is valid or not 2. check some
	 * spout's input is empty or not
	 * 
	 * @param topology
	 * @throws InvalidTopologyException
	 */
	public static void validate_basic(StormTopology topology,
			Map<Object, Object> totalStormConf, String topologyid)
			throws InvalidTopologyException {
		validate_ids(topology);

		for (StormTopology._Fields field : Thrift.SPOUT_FIELDS) {
			Object value = topology.getFieldValue(field);
			if (value != null) {
				Map<String, Object> obj_map = (Map<String, Object>) value;
				for (Object obj : obj_map.values()) {
					validate_component_inputs(obj);
				}
			}

		}

		Integer workerNum = JStormUtils.parseInt(totalStormConf
				.get(Config.TOPOLOGY_WORKERS));
		if (workerNum == null || workerNum <= 0) {
			String errMsg = "There are no Config.TOPOLOGY_WORKERS in configuration of "
					+ topologyid;
			throw new InvalidParameterException(errMsg);
		}

		Integer ackerNum = JStormUtils.parseInt(totalStormConf
				.get(Config.TOPOLOGY_ACKER_EXECUTORS));
		if (ackerNum != null && ackerNum < 0) {
			String errMsg = "Invalide Config.TOPOLOGY_ACKERS in configuration of "
					+ topologyid;
			throw new InvalidParameterException(errMsg);
		}

	}

	/**
	 * Generate acker's input Map<GlobalStreamId, Grouping>
	 * 
	 * for spout <GlobalStreamId(spoutId, ACKER_INIT_STREAM_ID), ...> for bolt
	 * <GlobalStreamId(boltId, ACKER_ACK_STREAM_ID), ...>
	 * <GlobalStreamId(boltId, ACKER_FAIL_STREAM_ID), ...>
	 * 
	 * @param topology
	 * @return
	 */
	public static Map<GlobalStreamId, Grouping> acker_inputs(
			StormTopology topology) {
		Map<GlobalStreamId, Grouping> spout_inputs = new HashMap<GlobalStreamId, Grouping>();
		Map<String, SpoutSpec> spout_ids = topology.get_spouts();
		for (Entry<String, SpoutSpec> spout : spout_ids.entrySet()) {
			String id = spout.getKey();

			GlobalStreamId stream = new GlobalStreamId(id, ACKER_INIT_STREAM_ID);

			Grouping group = Thrift.mkFieldsGrouping(JStormUtils.mk_list("id"));

			spout_inputs.put(stream, group);
		}

		Map<String, Bolt> bolt_ids = topology.get_bolts();
		Map<GlobalStreamId, Grouping> bolt_inputs = new HashMap<GlobalStreamId, Grouping>();
		for (Entry<String, Bolt> bolt : bolt_ids.entrySet()) {
			String id = bolt.getKey();

			GlobalStreamId streamAck = new GlobalStreamId(id,
					ACKER_ACK_STREAM_ID);
			Grouping groupAck = Thrift.mkFieldsGrouping(JStormUtils
					.mk_list("id"));

			GlobalStreamId streamFail = new GlobalStreamId(id,
					ACKER_FAIL_STREAM_ID);
			Grouping groupFail = Thrift.mkFieldsGrouping(JStormUtils
					.mk_list("id"));

			bolt_inputs.put(streamAck, groupAck);
			bolt_inputs.put(streamFail, groupFail);
		}

		Map<GlobalStreamId, Grouping> allInputs = new HashMap<GlobalStreamId, Grouping>();
		allInputs.putAll(bolt_inputs);
		allInputs.putAll(spout_inputs);
		return allInputs;
	}

	/**
	 * Add acker bolt to topology
	 * 
	 * @param num_tasks
	 * @param ret
	 */
	public static void add_acker(Integer ackerNum, StormTopology ret) {
		// generate outputs
		HashMap<String, StreamInfo> outputs = new HashMap<String, StreamInfo>();
		ArrayList<String> fields = new ArrayList<String>();
		fields.add("id");

		outputs.put(ACKER_ACK_STREAM_ID, Thrift.directOutputFields(fields));
		outputs.put(ACKER_FAIL_STREAM_ID, Thrift.directOutputFields(fields));

		IBolt ackerbolt = new Acker();

		// generate inputs
		Map<GlobalStreamId, Grouping> inputs = acker_inputs(ret);

		// generate acker which will be stored in topology
		Bolt acker_bolt = Thrift.mkAckerBolt(inputs, ackerbolt, outputs,
				ackerNum);

		// add every bolt two output stream
		// ACKER_ACK_STREAM_ID/ACKER_FAIL_STREAM_ID
		for (Entry<String, Bolt> e : ret.get_bolts().entrySet()) {

			Bolt bolt = e.getValue();

			ComponentCommon common = bolt.get_common();

			List<String> ackList = JStormUtils.mk_list("id", "ack-val");

			common.put_to_streams(ACKER_ACK_STREAM_ID,
					Thrift.outputFields(ackList));

			List<String> failList = JStormUtils.mk_list("id");
			common.put_to_streams(ACKER_FAIL_STREAM_ID,
					Thrift.outputFields(failList));

			bolt.set_common(common);
		}

		// add every spout output stream ACKER_INIT_STREAM_ID
		// add every spout two intput source
		// ((ACKER_COMPONENT_ID, ACKER_ACK_STREAM_ID), directGrouping)
		// ((ACKER_COMPONENT_ID, ACKER_FAIL_STREAM_ID), directGrouping)
		for (Entry<String, SpoutSpec> kv : ret.get_spouts().entrySet()) {
			SpoutSpec bolt = kv.getValue();
			ComponentCommon common = bolt.get_common();
			List<String> initList = JStormUtils.mk_list("id", "init-val",
					"spout-task");
			common.put_to_streams(ACKER_INIT_STREAM_ID,
					Thrift.outputFields(initList));

			GlobalStreamId ack_ack = new GlobalStreamId(ACKER_COMPONENT_ID,
					ACKER_ACK_STREAM_ID);
			common.put_to_inputs(ack_ack, Thrift.mkDirectGrouping());

			GlobalStreamId ack_fail = new GlobalStreamId(ACKER_COMPONENT_ID,
					ACKER_FAIL_STREAM_ID);
			common.put_to_inputs(ack_fail, Thrift.mkDirectGrouping());
		}

		ret.put_to_bolts(ACKER_COMPONENT_ID, acker_bolt);
	}

	public static List<Object> all_components(StormTopology topology) {
		List<Object> rtn = new ArrayList<Object>();
		for (StormTopology._Fields field : Thrift.STORM_TOPOLOGY_FIELDS) {
			Object fields = topology.getFieldValue(field);
			if (fields != null) {
				rtn.addAll(((Map) fields).values());
			}
		}
		return rtn;
	}

	private static List<String> sysEventFields = JStormUtils.mk_list("event");

	private static void add_component_system_streams(Object obj) {
		ComponentCommon common = null;
		if (obj instanceof StateSpoutSpec) {
			StateSpoutSpec spec = (StateSpoutSpec) obj;
			common = spec.get_common();
		}

		if (obj instanceof SpoutSpec) {
			SpoutSpec spec = (SpoutSpec) obj;
			common = spec.get_common();
		}

		if (obj instanceof Bolt) {
			Bolt spec = (Bolt) obj;
			common = spec.get_common();
		}

		if (common != null) {
			StreamInfo sinfo = Thrift.outputFields(sysEventFields);
			common.put_to_streams(SYSTEM_STREAM_ID, sinfo);
		}
	}

	/**
	 * Add every bolt or spout one output stream <SYSTEM_STREAM_ID, >
	 * 
	 * @param topology
	 */
	public static void add_system_streams(StormTopology topology) {
		for (Object obj : all_components(topology)) {
			add_component_system_streams(obj);
		}
	}

	@SuppressWarnings("rawtypes")
	public static StormTopology system_topology(Map storm_conf,
			StormTopology topology) throws InvalidTopologyException {

		StormTopology ret = topology.deepCopy();

		String key = Config.TOPOLOGY_ACKER_EXECUTORS;

		Integer ackercount = JStormUtils.parseInt(storm_conf.get(key), 0);

		add_acker(ackercount, ret);

		add_system_streams(ret);

		return ret;
	}

	/**
	 * get component configuration
	 * 
	 * @param storm_conf
	 * @param topology_context
	 * @param component_id
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static Map component_conf(Map storm_conf,
			TopologyContext topology_context, String component_id) {
		List<Object> to_remove = StormConfig.All_CONFIGS();
		to_remove.remove(Config.TOPOLOGY_DEBUG);
		to_remove.remove(Config.TOPOLOGY_MAX_SPOUT_PENDING);
		to_remove.remove(Config.TOPOLOGY_MAX_TASK_PARALLELISM);
		to_remove.remove(Config.TOPOLOGY_TRANSACTIONAL_ID);

		Map<Object, Object> componentConf = new HashMap<Object, Object>();

		String jconf = topology_context.getComponentCommon(component_id)
				.get_json_conf();
		if (jconf != null) {
			componentConf = (Map<Object, Object>) JStormUtils.from_json(jconf);
		}

		/**
		 * @@@ Don't know why need remove system configuration from component
		 *     conf? //
		 */
		// for (Object p : to_remove) {
		// componentConf.remove(p);
		// }

		Map<Object, Object> ret = new HashMap<Object, Object>();
		ret.putAll(storm_conf);
		ret.putAll(componentConf);

		return ret;
	}

	/**
	 * get object of component_id
	 * 
	 * @param topology
	 * @param component_id
	 * @return
	 */
	public static Object get_task_object(StormTopology topology,
			String component_id, URLClassLoader loader) {
		Map<String, SpoutSpec> spouts = topology.get_spouts();
		Map<String, Bolt> bolts = topology.get_bolts();
		Map<String, StateSpoutSpec> state_spouts = topology.get_state_spouts();

		ComponentObject obj = null;
		if (spouts.containsKey(component_id)) {
			obj = spouts.get(component_id).get_spout_object();
		} else if (bolts.containsKey(component_id)) {
			obj = bolts.get(component_id).get_bolt_object();
		} else if (state_spouts.containsKey(component_id)) {
			obj = state_spouts.get(component_id).get_state_spout_object();
		}

		if (obj == null) {
			throw new RuntimeException("Could not find " + component_id
					+ " in " + topology.toString());
		}

		Object componentObject = Utils.getSetComponentObject(obj, loader);

		Object rtn = null;

		if (componentObject instanceof JavaObject) {
			rtn = Thrift.instantiateJavaObject((JavaObject) componentObject);
		} else if (componentObject instanceof ShellComponent) {
			if (spouts.containsKey(component_id)) {
				rtn = new ShellSpout((ShellComponent) componentObject);
			} else {
				rtn = new ShellBolt((ShellComponent) componentObject);
			}
		} else {
			rtn = componentObject;
		}
		return rtn;

	}

	/**
	 * get current task's output <Stream_id, <componentId, MkGrouper>>
	 * 
	 * @param topology_context
	 * @return
	 */
	public static Map<String, Map<String, MkGrouper>> outbound_components(
			TopologyContext topology_context) {
		Map<String, Map<String, MkGrouper>> rr = new HashMap<String, Map<String, MkGrouper>>();

		// <Stream_id,<component,Grouping>>
		Map<String, Map<String, Grouping>> output_groupings = topology_context
				.getThisTargets();

		for (Entry<String, Map<String, Grouping>> entry : output_groupings
				.entrySet()) {

			String stream_id = entry.getKey();
			Map<String, Grouping> component_grouping = entry.getValue();

			Fields out_fields = topology_context.getThisOutputFields(stream_id);

			Map<String, MkGrouper> componentGrouper = new HashMap<String, MkGrouper>();

			for (Entry<String, Grouping> cg : component_grouping.entrySet()) {

				String component = cg.getKey();
				Grouping tgrouping = cg.getValue();

				List<Integer> outTasks = topology_context
						.getComponentTasks(component);
				// ATTENTION: If topology set one component parallelism as 0
				// so we don't need send tuple to it
				if (outTasks.size() > 0) {
					MkGrouper grouper = new MkGrouper(topology_context,
							out_fields, tgrouping, outTasks, stream_id);
					componentGrouper.put(component, grouper);
				}
			}
			if (componentGrouper.size() > 0) {
				rr.put(stream_id, componentGrouper);
			}
		}
		return rr;
	}

}

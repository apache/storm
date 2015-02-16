package com.alibaba.jstorm.utils;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.ComponentObject;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.JavaObject;
import backtype.storm.generated.JavaObjectArg;
import backtype.storm.generated.NullStruct;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.StormTopology._Fields;
import backtype.storm.generated.StreamInfo;
import backtype.storm.generated.TopologyInitialStatus;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.IBolt;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.cluster.StormStatus;
import com.alibaba.jstorm.daemon.nimbus.StatusType;

/**
 * Thrift utils
 * 
 * 2012-03-28
 * 
 * @author yannian
 * 
 */
public class Thrift {
	private static Logger LOG = Logger.getLogger(Thrift.class);

	public static StormStatus topologyInitialStatusToStormStatus(
			TopologyInitialStatus tStatus) {
		if (tStatus.equals(TopologyInitialStatus.ACTIVE)) {
			return new StormStatus(StatusType.active);
		} else {
			return new StormStatus(StatusType.inactive);
		}
	}

	public static CustomStreamGrouping instantiateJavaObject(JavaObject obj) {

		List<JavaObjectArg> args = obj.get_args_list();
		Class[] paraTypes = new Class[args.size()];
		Object[] paraValues = new Object[args.size()];
		for (int i = 0; i < args.size(); i++) {
			JavaObjectArg arg = args.get(i);
			paraValues[i] = arg.getFieldValue();

			if (arg.getSetField().equals(JavaObjectArg._Fields.INT_ARG)) {
				paraTypes[i] = Integer.class;
			} else if (arg.getSetField().equals(JavaObjectArg._Fields.LONG_ARG)) {
				paraTypes[i] = Long.class;
			} else if (arg.getSetField().equals(
					JavaObjectArg._Fields.STRING_ARG)) {
				paraTypes[i] = String.class;
			} else if (arg.getSetField().equals(JavaObjectArg._Fields.BOOL_ARG)) {
				paraTypes[i] = Boolean.class;
			} else if (arg.getSetField().equals(
					JavaObjectArg._Fields.BINARY_ARG)) {
				paraTypes[i] = ByteBuffer.class;
			} else if (arg.getSetField().equals(
					JavaObjectArg._Fields.DOUBLE_ARG)) {
				paraTypes[i] = Double.class;
			} else {
				paraTypes[i] = Object.class;
			}
		}

		try {
			Class clas = Class.forName(obj.get_full_class_name());
			Constructor cons = clas.getConstructor(paraTypes);
			return (CustomStreamGrouping) cons.newInstance(paraValues);
		} catch (Exception e) {
			LOG.error("instantiate_java_object fail", e);
		}

		return null;

	}

	public static Grouping._Fields groupingType(Grouping grouping) {
		return grouping.getSetField();
	}

	public static List<String> fieldGrouping(Grouping grouping) {
		if (!Grouping._Fields.FIELDS.equals(groupingType(grouping))) {
			throw new IllegalArgumentException(
					"Tried to get grouping fields from non fields grouping");
		}

		return grouping.get_fields();
	}

	public static boolean isGlobalGrouping(Grouping grouping) {
		if (Grouping._Fields.FIELDS.equals(groupingType(grouping))) {
			return fieldGrouping(grouping).isEmpty();
		}

		return false;
	}

	public static int parallelismHint(ComponentCommon component_common) {
		int phint = component_common.get_parallelism_hint();
		if (!component_common.is_set_parallelism_hint()) {
			phint = 1;
		}
		return phint;
	}

	public static StreamInfo directOutputFields(List<String> fields) {
		return new StreamInfo(fields, true);
	}

	public static StreamInfo outputFields(List<String> fields) {
		return new StreamInfo(fields, false);
	}

	public static Grouping mkFieldsGrouping(List<String> fields) {
		return Grouping.fields(fields);
	}

	public static Grouping mkDirectGrouping() {
		return Grouping.direct(new NullStruct());
	}

	private static ComponentCommon mkComponentcommon(
			Map<GlobalStreamId, Grouping> inputs,
			HashMap<String, StreamInfo> output_spec, Integer parallelism_hint) {
		ComponentCommon ret = new ComponentCommon(inputs, output_spec);
		if (parallelism_hint != null) {
			ret.set_parallelism_hint(parallelism_hint);
		}
		return ret;
	}

	public static Bolt mkBolt(Map<GlobalStreamId, Grouping> inputs,
			IBolt bolt, HashMap<String, StreamInfo> output, Integer p) {
		ComponentCommon common = mkComponentcommon(inputs, output, p);
		byte[] boltSer = Utils.serialize(bolt);
		ComponentObject component = ComponentObject.serialized_java(boltSer);
		return new Bolt(component, common);
	}

	public static StormTopology._Fields[] STORM_TOPOLOGY_FIELDS = null;
	public static StormTopology._Fields[] SPOUT_FIELDS = {
			StormTopology._Fields.SPOUTS, StormTopology._Fields.STATE_SPOUTS };
	static {
		Set<_Fields> keys = StormTopology.metaDataMap.keySet();
		STORM_TOPOLOGY_FIELDS = new StormTopology._Fields[keys.size()];
		keys.toArray(STORM_TOPOLOGY_FIELDS);
	}

	//
	// private static HashMap<GlobalStreamId, Grouping> mk_acker_inputs2(
	// Map<Object, List<String>> inputs) {
	// HashMap<GlobalStreamId, Grouping> rtn = new HashMap<GlobalStreamId,
	// Grouping>();
	// for (Entry<Object, List<String>> e : inputs.entrySet()) {
	// Object stream_id = e.getKey();
	// GlobalStreamId SID = null;
	// if (stream_id instanceof Object[]) {
	// String[] l = (String[]) stream_id;
	// SID = new GlobalStreamId(l[0], l[1]);
	// } else if (stream_id instanceof List) {
	// List<String> l = (List<String>) stream_id;
	// SID = new GlobalStreamId(l.get(0), l.get(1));
	// } else {
	// SID = new GlobalStreamId((String) stream_id,
	// Utils.DEFAULT_STREAM_ID);
	// }
	//
	// rtn.put(SID, mk_fields_grouping(e.getValue()));
	// }
	//
	// return rtn;
	// }
	//
	// public static Bolt mk_ackerBolt(Map<Object, List<String>> inputs,
	// IBolt bolt, HashMap<String, List<String>> output, Integer p) {
	// HashMap<GlobalStreamId, Grouping> commonInputs=mk_inputs(inputs);
	// ComponentCommon common = mk_component_common(commonInputs,output, p);
	// return new Bolt(ComponentObject.serialized_java(Utils.serialize(bolt)),
	// common);
	// }

	// public static Map<String, StreamInfo>
	// mk_output_StreamInfo(HashMap<String, List<String>> output_spec) {
	// Map<String, List<String>> o_spec = output_spec;
	//
	// Map<String, StreamInfo> rtn = new HashMap<String, StreamInfo>();
	//
	// for (Entry<String, List<String>> e : o_spec.entrySet()) {
	// List<String> val = e.getValue();
	// rtn.put(e.getKey(), new StreamInfo((List<String>) val, false));
	//
	// }
	//
	// return rtn;
	// }

	// public static Map<String, StreamInfo>
	// mk_output_StreamInfo2(HashMap<String, StreamInfo> output_spec) {
	// return output_spec;
	// }

	// private static ComponentCommon mk_component_common(
	// Map<GlobalStreamId, Grouping> inputs, HashMap<String, List<String>>
	// output_spec,
	// Integer parallelism_hint) {
	// ComponentCommon ret = new ComponentCommon(inputs,
	// mk_output_StreamInfo(output_spec));
	// if (parallelism_hint != null) {
	// ret.set_parallelism_hint(parallelism_hint);
	// }
	//
	// return ret;
	// }

	// public static Object[] nimbus_client_and_conn(String host, Integer port)
	// throws TTransportException {
	// TFramedTransport transport = new TFramedTransport(new TSocket(host,
	// port));
	// TBinaryProtocol prot = new TBinaryProtocol(transport);
	// Nimbus.Client client = new Nimbus.Client(prot);
	// transport.open();
	// Object[] rtn = { client, transport };
	// return rtn;
	// }
	// public static SpoutSpec mk_spout_spec(Object spout, Object outputs,
	// Integer p) {
	// Map<GlobalStreamId, Grouping> inputs = new HashMap<GlobalStreamId,
	// Grouping>();
	// return new SpoutSpec(ComponentObject.serialized_java(Utils
	// .serialize(spout)), mk_plain_component_common(inputs, outputs,
	// p));
	// }

	// public static SpoutSpec mk_spout_spec(Object spout, Object outputs) {
	// return mk_spout_spec(spout, outputs, null);
	// }

	// public static Grouping mk_shuffle_grouping() {
	// return Grouping.shuffle(new NullStruct());
	// }

	// public static Grouping mk_global_grouping() {
	// return mk_fields_grouping(new ArrayList<String>());
	// }

	// public static Grouping mk_all_grouping() {
	// return Grouping.all(new NullStruct());
	// }

	// public static Grouping mk_none_grouping() {
	// return Grouping.none(new NullStruct());
	// }

	// public static Object deserialized_component_object(ComponentObject obj) {
	// if (obj.getSetField().equals(ComponentObject._Fields.SERIALIZED_JAVA)) {
	// throw new RuntimeException(
	// "Cannot deserialize non-java-serialized object");
	// }
	//
	// return Utils.deserialize(obj.get_serialized_java());
	// }

	// public static ComponentObject serialize_component_object(Object obj) {
	// return ComponentObject.serialized_java(Utils.serialize(obj));
	// }

	// public static Grouping mk_grouping(Object grouping_spec) {
	// if (grouping_spec == null) {
	// return mk_none_grouping();
	// }
	//
	// if (grouping_spec instanceof Grouping) {
	// return (Grouping) grouping_spec;
	// }
	//
	// if (grouping_spec instanceof CustomStreamGrouping) {
	// return Grouping.custom_serialized(Utils.serialize(grouping_spec));
	// }
	//
	// if (grouping_spec instanceof JavaObject) {
	// return Grouping.custom_object((JavaObject) grouping_spec);
	// }
	//
	// if (grouping_spec instanceof List) {
	// return mk_fields_grouping((List<String>) grouping_spec);
	// }
	//
	// //
	// if (GroupingConstants.shuffle.equals(grouping_spec)) {
	// return mk_shuffle_grouping();
	// }
	//
	// if (GroupingConstants.none.equals(grouping_spec)) {
	// return mk_none_grouping();
	// }
	//
	// if (GroupingConstants.all.equals(grouping_spec)) {
	// return mk_all_grouping();
	// }
	//
	// if ("global".equals(grouping_spec)) {
	// return mk_global_grouping();
	// }
	//
	// if (GroupingConstants.direct.equals(grouping_spec)) {
	// return mk_direct_grouping();
	// }
	//
	// throw new IllegalArgumentException(grouping_spec
	// + " is not a valid grouping");
	// }

	// public static Bolt mk_bolt_spec(Map<Object, List<String>> inputs,
	// IBolt bolt, Object output) {
	// return mk_bolt_spec(inputs, bolt, output, null);
	// }

	//
	// public class BoltSpecObj {
	// public IRichBolt obj;
	// public Map conf;
	// public Map<Object,List<String>> inputs;
	// public Integer p;
	// }
	// public static BoltSpecObj mk_bolt_spec(Map<Object, List<String>> inputs,
	// IRichBolt bolt, Object... args) {
	// Integer parallelism_hint = null;
	// Integer p = null;
	// Map conf = null;
	// if (args.length >= 1) {
	// parallelism_hint = (Integer) args[0];
	// }
	// if (args.length >= 2) {
	// p = (Integer) args[1];
	// }
	// if (args.length >= 3) {
	// conf = (Map) args[2];
	// }
	//
	// return mk_bolt_spec(inputs, bolt, parallelism_hint, p, conf);
	// }
	// public class SpoutSpecObj {
	// public IRichSpout obj;
	// public Map conf;
	// public Integer p;
	// }
	// public static SpoutSpecObj mk_spout_spec(Map<Object, List<String>>
	// inputs,
	// IRichBolt bolt, Object... args) {
	// Integer parallelism_hint = null;
	// Integer p = null;
	// Map conf = null;
	// if (args.length >= 1) {
	// parallelism_hint = (Integer) args[0];
	// }
	// if (args.length >= 2) {
	// p = (Integer) args[1];
	// }
	// if (args.length >= 3) {
	// conf = (Map) args[2];
	// }
	//
	// return mk_spout_spec(inputs, bolt, parallelism_hint, p, conf);
	// }
	//
	// public static SpoutSpecObj mk_spout_spec(IRichSpout bolt,
	// Integer parallelism_hint, Integer p, Map conf) {
	// if (p != null) {
	// parallelism_hint = p;
	// }
	// SpoutSpecObj rtn = new SpoutSpecObj();
	// rtn.p = parallelism_hint;
	// rtn.obj = bolt;
	// rtn.conf = conf;
	// return rtn;
	// }
	//
	// public static void add_inputs(BoltDeclarer declarer,
	// Map<Object, List<String>> inputs) {
	// HashMap<GlobalStreamId, Grouping> getinputs = mk_inputs(inputs);
	// for (Entry<GlobalStreamId, Grouping> e : getinputs.entrySet()) {
	// declarer.grouping(e.getKey(), e.getValue());
	// }
	// }

	// public static StormTopology mk_topology(
	// HashMap<String, SpoutSpecObj> spout_map,
	// HashMap<String, BoltSpecObj> bolt_map) {
	// TopologyBuilder builder = new TopologyBuilder();
	// for (Entry<String, SpoutSpecObj> e : spout_map.entrySet()) {
	// String id = e.getKey();
	// SpoutSpecObj spout_obj = e.getValue();
	// SpoutDeclarer dec = builder
	// .setSpout(id, spout_obj.obj, spout_obj.p);
	// if (spout_obj.conf != null) {
	// dec.addConfigurations(spout_obj.conf);
	// }
	// }
	//
	// for (Entry<String, BoltSpecObj> e : bolt_map.entrySet()) {
	// String id = e.getKey();
	// BoltSpecObj bolt = e.getValue();
	// BoltDeclarer dec = builder.setBolt(id, bolt.obj, bolt.p);
	// if (bolt.conf != null) {
	// dec.addConfigurations(bolt.conf);
	// }
	// add_inputs(dec, bolt.inputs);
	// }
	//
	// return builder.createTopology();
	// }

	// public static BoltSpecObj mk_bolt_spec(Map<Object, List<String>> inputs,
	// IRichBolt bolt, Integer parallelism_hint, Integer p, Map conf) {
	// if (p != null) {
	// parallelism_hint = p;
	// }
	// BoltSpecObj rtn = new BoltSpecObj();
	// rtn.inputs = inputs;
	// rtn.p = parallelism_hint;
	// rtn.obj = bolt;
	// rtn.conf = conf;
	// return rtn;
	// }

	// public static BoltSpecObj mk_shellbolt_spec(
	// Map<Object, List<String>> inputs, String command, String script,
	// Object output_spec, Object... args) {
	// Integer parallelism_hint = null;
	// Integer p = null;
	// Map conf = null;
	// if (args.length >= 1) {
	// parallelism_hint = (Integer) args[0];
	// }
	// if (args.length >= 2) {
	// p = (Integer) args[1];
	// }
	// if (args.length >= 3) {
	// conf = (Map) args[2];
	// }
	//
	// return mk_shellbolt_spec(inputs, command, script, output_spec,
	// parallelism_hint, p, conf);
	// }

	// public static BoltSpecObj mk_shellbolt_spec(
	// Map<Object, List<String>> inputs, String command, String script,
	// Object output_spec, Integer parallelism_hint, Integer p, Map conf) {
	// return mk_bolt_spec(inputs, new RichShellBolt(script, command,
	// mk_output_spec(output_spec)), parallelism_hint, p, conf);
	// }

	// public static Bolt mk_spout_spec(Map<GlobalStreamId, Grouping> inputs,
	// IComponent bolt, Object output) {
	// return mk_spout_spec(inputs, bolt, output, null);
	// }

	// public static Bolt mk_spout_spec(Map<GlobalStreamId, Grouping> inputs,
	// IComponent bolt, Object output, Integer p) {
	// ComponentCommon common = mk_plain_component_common(inputs, output, p);
	//
	// return new Bolt(ComponentObject.serialized_java(Utils.serialize(bolt)),
	// common);
	// }

	// public static String COORD_STREAM = Constants.COORDINATED_STREAM_ID;

}

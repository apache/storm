package com.alipay.dw.jstorm.example.sequence;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.TopologyBuilder;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.client.WorkerAssignment;

public class SequenceTopologyUserDefine extends SequenceTopology {
	private static Logger LOG = LoggerFactory
			.getLogger(SequenceTopologyUserDefine.class);

	private static Map conf = new HashMap<Object, Object>();

	public static void SetBuilder(TopologyBuilder builder, Map conf) {

		SequenceTopology.SetBuilder(builder, conf);

		List<WorkerAssignment> userDefinedWorks = new ArrayList<WorkerAssignment>();
		WorkerAssignment spoutWorkerAssignment = new WorkerAssignment();
		spoutWorkerAssignment.addComponent(
				SequenceTopologyDef.SEQUENCE_SPOUT_NAME, 1);
		spoutWorkerAssignment.setHostName((String) conf.get("spout.host"));
		userDefinedWorks.add(spoutWorkerAssignment);

		WorkerAssignment totalWorkerAssignment = new WorkerAssignment();
		totalWorkerAssignment.addComponent(SequenceTopologyDef.TOTAL_BOLT_NAME,
				2);
		totalWorkerAssignment.setHostName((String) conf.get("total.host"));
		userDefinedWorks.add(totalWorkerAssignment);

		ConfigExtension.setUserDefineAssignment(conf, userDefinedWorks);

	}

	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			System.err.println("Please input configuration file");
			System.exit(-1);
		}

		LoadConf(args[0]);

		if (local_mode(conf)) {
			SetLocalTopology();
		} else {
			SetRemoteTopology();
		}

	}

}

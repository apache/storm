/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

package com.alibaba.jstorm.batch;

import org.apache.log4j.Logger;

import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;

import com.alibaba.jstorm.batch.impl.BatchSpoutTrigger;
import com.alibaba.jstorm.batch.impl.CoordinatedBolt;
import com.alibaba.jstorm.batch.util.BatchDef;

public class BatchTopologyBuilder {
	private static final Logger LOG = Logger
			.getLogger(BatchTopologyBuilder.class);

	private TopologyBuilder topologyBuilder;

	private SpoutDeclarer spoutDeclarer;

	public BatchTopologyBuilder(String topologyName) {
		topologyBuilder = new TopologyBuilder();

		spoutDeclarer = topologyBuilder.setSpout(BatchDef.SPOUT_TRIGGER,
				new BatchSpoutTrigger(), 1);
	}

	public BoltDeclarer setSpout(String id, IBatchSpout spout, int paralel) {

		BoltDeclarer boltDeclarer = this
				.setBolt(id, (IBatchSpout) spout, paralel);
		boltDeclarer.allGrouping(BatchDef.SPOUT_TRIGGER,
				BatchDef.COMPUTING_STREAM_ID);

		return boltDeclarer;
	}

	public BoltDeclarer setBolt(String id, IBasicBolt bolt, int paralel) {
		CoordinatedBolt coordinatedBolt = new CoordinatedBolt(bolt);

		BoltDeclarer boltDeclarer = topologyBuilder.setBolt(id,
				coordinatedBolt, paralel);

		if (bolt instanceof IPrepareCommit) {
			boltDeclarer.allGrouping(BatchDef.SPOUT_TRIGGER,
					BatchDef.PREPARE_STREAM_ID);
		}

		if (bolt instanceof ICommitter) {
			boltDeclarer.allGrouping(BatchDef.SPOUT_TRIGGER,
					BatchDef.COMMIT_STREAM_ID);
			boltDeclarer.allGrouping(BatchDef.SPOUT_TRIGGER,
					BatchDef.REVERT_STREAM_ID);
		}

		if (bolt instanceof IPostCommit) {
			boltDeclarer.allGrouping(BatchDef.SPOUT_TRIGGER,
					BatchDef.POST_STREAM_ID);
		}

		return boltDeclarer;
	}

	public TopologyBuilder getTopologyBuilder() {
		return topologyBuilder;
	}

}

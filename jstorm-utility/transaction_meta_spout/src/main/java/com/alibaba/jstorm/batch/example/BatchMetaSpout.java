package com.alibaba.jstorm.batch.example;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.alibaba.jstorm.batch.BatchId;
import com.alibaba.jstorm.batch.IBatchSpout;
import com.alibaba.jstorm.batch.meta.MetaSimpleClient;
import com.alibaba.jstorm.batch.meta.MetaSpoutConfig;
import com.alibaba.jstorm.batch.util.BatchCommon;
import com.alibaba.jstorm.cluster.ClusterState;
import com.alibaba.rocketmq.common.message.MessageExt;

public class BatchMetaSpout implements IBatchSpout{
	private static final long serialVersionUID = 5720810158625748041L;

	private static final Logger LOG = Logger.getLogger(BatchMetaSpout.class);
	
	public static final String SPOUT_NAME = BatchMetaSpout.class.getSimpleName();

	private Map conf;

	private String taskName;
	private int taskIndex;
	private int taskParallel;

	private transient MetaSimpleClient metaClient;
	private MetaSpoutConfig metaSpoutConfig;
	
	

	public BatchMetaSpout(MetaSpoutConfig metaSpoutConfig) {
		this.metaSpoutConfig = metaSpoutConfig;
	}
	
	public void initMetaClient() throws Exception {
		ClusterState zkClient = BatchCommon.getZkClient(conf);
		metaClient = new MetaSimpleClient(metaSpoutConfig, zkClient, taskIndex,
				taskParallel);

		metaClient.init();

		LOG.info("Successfully init meta client " + taskName);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.conf = stormConf;

		taskName = context.getThisComponentId() + "_" + context.getThisTaskId();

		taskIndex = context.getThisTaskIndex();

		taskParallel = context.getComponentTasks(context.getThisComponentId())
				.size();

		try {
			initMetaClient();
		} catch (Exception e) {
			LOG.info("Failed to init Meta Client,", e);
			throw new RuntimeException(e);
		}

		LOG.info(taskName + " successfully do prepare ");
	}

	public void emitBatch(BatchId batchId, BasicOutputCollector collector) {
		List<MessageExt> msgs = metaClient.fetchOneBatch();
		for (MessageExt msgExt : msgs) {
			collector.emit(new Values(batchId, msgExt));
		}
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		
		String streamId = input.getSourceStreamId();
		if (streamId.equals(BatchMetaRebalance.REBALANCE_STREAM_ID)) {
			try {
				metaClient.rebalanceMqList();
			} catch (Exception e) {
				LOG.warn("Failed to do rebalance operation", e);
			}
		}else {
			BatchId batchId = (BatchId) input.getValue(0);

			emitBatch(batchId, collector);
		}
		
	}

	@Override
	public void cleanup() {
		metaClient.cleanup();

	}

	@Override
	public byte[] commit(BatchId id) throws FailedException {
		return metaClient.commit(id);
	}

	@Override
	public void revert(BatchId id, byte[] commitResult) {
		metaClient.revert(id, commitResult);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("BatchId", "MessageExt"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	
	

}

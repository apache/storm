package com.alibaba.jstorm.batch.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.alibaba.jstorm.batch.BatchId;
import com.alibaba.jstorm.batch.util.BatchCommon;
import com.alibaba.jstorm.batch.util.BatchDef;
import com.alibaba.jstorm.batch.util.BatchStatus;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.ClusterState;
import com.alibaba.jstorm.utils.IntervalCheck;
import com.alibaba.jstorm.utils.JStormUtils;

/**
 * Strong Sequence
 * 
 * @author zhongyan.feng
 * @version
 */
public class BatchSpoutTrigger implements IRichSpout {
	/**  */
	private static final long serialVersionUID = 7215109169247425954L;

	private static final Logger LOG = Logger.getLogger(BatchSpoutTrigger.class);

	private LinkedBlockingQueue<BatchSpoutMsgId> batchQueue;

	private transient ClusterState zkClient;

	private transient SpoutOutputCollector collector;

	private static final String ZK_NODE_PATH = "/trigger";

	private static BatchId currentBatchId = null;

	private Map conf;

	private String taskName;
	
	private IntervalCheck intervalCheck;

	/**
	 * @throws Exception
	 * 
	 */
	public void initMsgId() throws Exception {
		Long zkMsgId = null;
		byte[] data = zkClient.get_data(ZK_NODE_PATH, false);
		if (data != null) {
			String value = new String(data);
			try {
				zkMsgId = Long.valueOf(value);
				LOG.info("ZK msgId:" + zkMsgId);
			} catch (Exception e) {
				LOG.warn("Failed to get msgId ", e);

			}

		}

		if (zkMsgId != null) {
			BatchId.updateId(zkMsgId);
		}

		int max_spout_pending = JStormUtils.parseInt(
				conf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING), 1);

		for (int i = 0; i < max_spout_pending; i++) {
			BatchSpoutMsgId msgId = BatchSpoutMsgId.mkInstance();
			if (currentBatchId == null) {
				currentBatchId = msgId.getBatchId();
			}
			batchQueue.offer(msgId);
			LOG.info("Push into queue," + msgId);
		}

	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		batchQueue = new LinkedBlockingQueue<BatchSpoutMsgId>();
		this.collector = collector;
		this.conf = conf;
		taskName = context.getThisComponentId() + "_" + context.getThisTaskId();
		
		intervalCheck = new IntervalCheck();

		try {
			zkClient = BatchCommon.getZkClient(conf);

			initMsgId();

		} catch (Exception e) {
			LOG.error("", e);
			throw new RuntimeException("Failed to init");
		}
		LOG.info("Successfully open " + taskName);
	}

	@Override
	public void close() {
	}

	@Override
	public void activate() {
		LOG.info("Activate " + taskName);
	}

	@Override
	public void deactivate() {
		LOG.info("Deactivate " + taskName);
	}

	protected String getStreamId(BatchStatus batchStatus) {
		if (batchStatus == BatchStatus.COMPUTING) {
			return BatchDef.COMPUTING_STREAM_ID;
		} else if (batchStatus == BatchStatus.PREPARE_COMMIT) {
			return BatchDef.PREPARE_STREAM_ID;
		} else if (batchStatus == BatchStatus.COMMIT) {
			return BatchDef.COMMIT_STREAM_ID;
		} else if (batchStatus == BatchStatus.POST_COMMIT) {
			return BatchDef.POST_STREAM_ID;
		} else if (batchStatus == BatchStatus.REVERT_COMMIT) {
			return BatchDef.REVERT_STREAM_ID;
		} else {
			LOG.error("Occur unkonw type BatchStatus " + batchStatus);
			throw new RuntimeException();
		}
	}

	protected boolean isCommitStatus(BatchStatus batchStatus) {
		if (batchStatus == BatchStatus.COMMIT) {
			return true;
		} else if (batchStatus == BatchStatus.REVERT_COMMIT) {
			return true;
		} else {
			return false;
		}
	}

	protected boolean isCommitWait(BatchSpoutMsgId msgId) {

		if (isCommitStatus(msgId.getBatchStatus()) == false) {
			return false;
		}

		// left status is commit status
		if (currentBatchId.getId() >= msgId.getBatchId().getId()) {
			return false;
		}

		return true;
	}

	@Override
	public void nextTuple() {
		BatchSpoutMsgId msgId = null;
		try {
			msgId = batchQueue.poll(10, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			LOG.error("", e);
		}
		if (msgId == null) {
			return;
		}

		if (isCommitWait(msgId)) {

			batchQueue.offer(msgId);
			if (intervalCheck.check()) {
				LOG.info("Current msgId " + msgId
						+ ", but current commit BatchId is " + currentBatchId);
			}else {
				LOG.debug("Current msgId " + msgId
						+ ", but current commit BatchId is " + currentBatchId);
			}
			
			return;
		}

		String streamId = getStreamId(msgId.getBatchStatus());
		List<Integer> outTasks = collector.emit(streamId,
				new Values(msgId.getBatchId()), msgId);
		if (outTasks.isEmpty()) {
			forward(msgId);
		}
		return;

	}

	protected void mkMsgId(BatchSpoutMsgId oldMsgId) {
		synchronized (BatchSpoutMsgId.class) {
			if (currentBatchId.getId() <= oldMsgId.getBatchId().getId()) {
				// this is normal case

				byte[] data = String.valueOf(currentBatchId.getId()).getBytes();
				try {
					zkClient.set_data(ZK_NODE_PATH, data);
				} catch (Exception e) {
					LOG.error("Failed to update to ZK " + oldMsgId, e);
				}

				currentBatchId = BatchId.incBatchId(oldMsgId.getBatchId());

			} else {
				// bigger batchId has been failed, when old msgId finish
				// it will go here

			}

		}

		BatchSpoutMsgId newMsgId = BatchSpoutMsgId.mkInstance();
		batchQueue.offer(newMsgId);
		StringBuilder sb = new StringBuilder();
		sb.append("Create new BatchId,");
		sb.append("old:").append(oldMsgId);
		sb.append("new:").append(newMsgId);
		sb.append("currentBatchId:").append(currentBatchId);
		LOG.info(sb.toString());
	}

	protected void forward(BatchSpoutMsgId msgId) {
		BatchStatus status = msgId.getBatchStatus();

		BatchStatus newStatus = status.forward();
		if (newStatus == null) {
			// create new status
			mkMsgId(msgId);
			LOG.info("Finish old batch " + msgId);

		} else {
			msgId.setBatchStatus(newStatus);
			batchQueue.offer(msgId);
			LOG.info("Forward batch " + msgId);
		}
	}

	@Override
	public void ack(Object msgId) {
		if (msgId instanceof BatchSpoutMsgId) {
			forward((BatchSpoutMsgId) msgId);
			return;
		} else {
			LOG.warn("Unknown type msgId " + msgId.getClass().getName() + ":"
					+ msgId);
			return;
		}
	}

	protected void handleFail(BatchSpoutMsgId msgId) {
		LOG.info("Failed batch " + msgId);
		BatchStatus status = msgId.getBatchStatus();

		BatchStatus newStatus = status.error();
		if (newStatus == BatchStatus.ERROR) {
			// create new status
			mkMsgId(msgId);

		} else {

			msgId.setBatchStatus(newStatus);
			batchQueue.offer(msgId);

		}
	}

	@Override
	public void fail(Object msgId) {
		if (msgId instanceof BatchSpoutMsgId) {
			handleFail((BatchSpoutMsgId) msgId);
		} else {
			LOG.warn("Unknown type msgId " + msgId.getClass().getName() + ":"
					+ msgId);
			return;
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(BatchDef.COMPUTING_STREAM_ID, new Fields(
				"BatchId"));
		declarer.declareStream(BatchDef.PREPARE_STREAM_ID,
				new Fields("BatchId"));
		declarer.declareStream(BatchDef.COMMIT_STREAM_ID, new Fields("BatchId"));
		declarer.declareStream(BatchDef.REVERT_STREAM_ID, new Fields("BatchId"));
		declarer.declareStream(BatchDef.POST_STREAM_ID, new Fields("BatchId"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> map = new HashMap<String, Object>();
		ConfigExtension.setSpoutSingleThread(map, true);
		return map;
	}

}

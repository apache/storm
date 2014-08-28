package com.alibaba.jstorm.batch.impl;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.ReportedFailedException;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.TimeCacheMap;

import com.alibaba.jstorm.batch.BatchId;
import com.alibaba.jstorm.batch.ICommitter;
import com.alibaba.jstorm.batch.IPostCommit;
import com.alibaba.jstorm.batch.IPrepareCommit;
import com.alibaba.jstorm.batch.util.BatchCommon;
import com.alibaba.jstorm.batch.util.BatchDef;
import com.alibaba.jstorm.batch.util.BatchStatus;
import com.alibaba.jstorm.cluster.ClusterState;

public class CoordinatedBolt implements IRichBolt {
	private static final long serialVersionUID = 5720810158625748046L;
	
	public static Logger LOG = LoggerFactory.getLogger(CoordinatedBolt.class);

	private IBasicBolt delegate;
	private BasicOutputCollector basicCollector;
	private OutputCollector collector;

	private String taskId;
	private String taskName;

	private boolean isCommiter = false;
	private String zkCommitPath;
	private TimeCacheMap<Object, Object> commited;

	public CoordinatedBolt(IBasicBolt delegate) {

		this.delegate = delegate;

	}

	// use static variable to reduce zk connection
	private static ClusterState zkClient = null;

	public void mkCommitDir(Map conf) {

		try {
			zkClient = BatchCommon.getZkClient(conf);

			zkCommitPath = BatchDef.ZK_COMMIT_DIR + BatchDef.ZK_SEPERATOR
					+ taskId;
			if (zkClient.node_existed(zkCommitPath, false)) {
				zkClient.delete_node(zkCommitPath);
			}
			zkClient.mkdirs(zkCommitPath);
			
			LOG.info(taskName + " successfully create commit path" + zkCommitPath);
		} catch (Exception e) {
			LOG.error("Failed to create zk node", e);
			throw new RuntimeException();
		}
	}

	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {

		taskId = String.valueOf(context.getThisTaskId());
		taskName = context.getThisComponentId() + "_" + context.getThisTaskId();

		this.basicCollector = new BasicOutputCollector(collector);
		this.collector = collector;

		if (delegate instanceof ICommitter) {
			isCommiter = true;
			commited = new TimeCacheMap<Object, Object>(
					context.maxTopologyMessageTimeout());
			mkCommitDir(conf);
		}

		delegate.prepare(conf, context);

	}

	public void removeUseless(String path, int reserveSize) throws Exception {
		List<String> childs = zkClient.get_children(path, false);
		Collections.sort(childs, new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				try {
					Long v1 = Long.valueOf(o1);
					Long v2 = Long.valueOf(o2);
					return v1.compareTo(v2);
				}catch(Exception e) {
					return o1.compareTo(o2);
				}
				
			}
    		
    	});

		for (int index = 0; index < childs.size() - reserveSize; index++) {
			zkClient.delete_node(path + BatchDef.ZK_SEPERATOR
					+ childs.get(index));
		}
	}
	
	public String getCommitPath(BatchId id) {
		return zkCommitPath + BatchDef.ZK_SEPERATOR + id.getId();
	}

	public void updateToZk(Object id, byte[] commitResult) {
		try {

			removeUseless(zkCommitPath, BatchDef.ZK_COMMIT_RESERVER_NUM);

			String path = getCommitPath((BatchId)id);
			byte[] data = commitResult;
			if (data == null) {
				data = new byte[0];
			}
			zkClient.set_data(path, data);
			LOG.info("Update " + path + " to zk");
		} catch (Exception e) {
			LOG.warn("Failed to update to zk,", e);

		}

	}

	public byte[] getCommittedData(Object id) {
		try {
			String path = getCommitPath((BatchId)id);
			byte[] data = zkClient.get_data(path, false);

			return data;
		} catch (Exception e) {
			LOG.error("Failed to visit ZK,", e);
			return null;
		}
	}

	public void handleRegular(Tuple tuple) {
		basicCollector.setContext(tuple);
		try {
			delegate.execute(tuple, basicCollector);
			collector.ack(tuple);
		} catch (FailedException e) {
			if (e instanceof ReportedFailedException) {
				collector.reportError(e);
			}
			collector.fail(tuple);
		}

	}

	public void handlePrepareCommit(Tuple tuple) {
		basicCollector.setContext(tuple);
		try {
			BatchId id = (BatchId) tuple.getValue(0);
			((IPrepareCommit) delegate).prepareCommit(id, basicCollector);
			collector.ack(tuple);
		} catch (FailedException e) {
			if (e instanceof ReportedFailedException) {
				collector.reportError(e);
			}
			collector.fail(tuple);
		}

	}

	public void handleCommit(Tuple tuple) {
		Object id = tuple.getValue(0);
		try {
			byte[] commitResult = ((ICommitter) delegate).commit((BatchId) id);

			collector.ack(tuple);

			updateToZk(id, commitResult);
			commited.put(id, commitResult);
		} catch (Exception e) {
			LOG.error("Failed to commit ", e);
			collector.fail(tuple);
		}
	}

	public void handleRevert(Tuple tuple) {
		try {
			Object id = tuple.getValue(0);
			byte[] commitResult = null;

			if (commited.containsKey(id)) {
				commitResult = (byte[]) commited.get(id);
			} else {
				commitResult = getCommittedData(id);
			}

			if (commitResult != null) {
				((ICommitter) delegate).revert((BatchId) id, commitResult);
			}
		} catch (Exception e) {
			LOG.error("Failed to revert,", e);
		}

		collector.ack(tuple);
	}

	public void handlePostCommit(Tuple tuple) {

		basicCollector.setContext(tuple);
		try {
			BatchId id = (BatchId) tuple.getValue(0);
			((IPostCommit) delegate).postCommit(id, basicCollector);
			
		} catch (Exception e) {
			LOG.info("Failed to do postCommit,", e);
		}
		collector.ack(tuple);
	}

	public void execute(Tuple tuple) {

		BatchStatus batchStatus = getBatchStatus(tuple);

		if (batchStatus == BatchStatus.COMPUTING) {
			handleRegular(tuple);
		} else if (batchStatus == BatchStatus.PREPARE_COMMIT) {
			handlePrepareCommit(tuple);
		} else if (batchStatus == BatchStatus.COMMIT) {
			handleCommit(tuple);
		} else if (batchStatus == BatchStatus.REVERT_COMMIT) {
			handleRevert(tuple);
		} else if (batchStatus == BatchStatus.POST_COMMIT) {
			handlePostCommit(tuple);
		} else {
			throw new RuntimeException(
					"Receive commit tuple, but not committer");
		}
	}

	public void cleanup() {
		delegate.cleanup();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		delegate.declareOutputFields(declarer);
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return delegate.getComponentConfiguration();
	}

	private BatchStatus getBatchStatus(Tuple tuple) {
		String streamId = tuple.getSourceStreamId();

		if (streamId.equals(BatchDef.PREPARE_STREAM_ID)) {
			return BatchStatus.PREPARE_COMMIT;
		} else if (streamId.equals(BatchDef.COMMIT_STREAM_ID)) {
			return BatchStatus.COMMIT;
		} else if (streamId.equals(BatchDef.REVERT_STREAM_ID)) {
			return BatchStatus.REVERT_COMMIT;
		} else if (streamId.equals(BatchDef.POST_STREAM_ID)) {
			return BatchStatus.POST_COMMIT;
		} else {
			return BatchStatus.COMPUTING;
		}
	}

}

package com.alibaba.jstorm.batch.meta;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;

import backtype.storm.topology.FailedException;

import com.alibaba.jstorm.batch.BatchId;
import com.alibaba.jstorm.batch.ICommitter;
import com.alibaba.jstorm.batch.util.BatchDef;
import com.alibaba.jstorm.cluster.ClusterState;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.PullStatus;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.taobao.metaq.client.MetaPullConsumer;

public class MetaSimpleClient implements ICommitter{
	private static final Logger LOG = Logger.getLogger(MetaSimpleClient.class);
	
	private final MetaSpoutConfig metaSpoutConfig;
	
	private final int taskIndex;
	private final int taskParallel;
	private int       oneQueueFetchSize;
	
	private Map<MessageQueue, Long> currentOffsets;
	private Map<MessageQueue, Long> frontOffsets;
	private Map<MessageQueue, Long> backendOffset;
	
	private MetaPullConsumer        consumer;
	
	private static String           nameServer;
	
	private final ClusterState      zkClient;
	
	public MetaSimpleClient(MetaSpoutConfig config, 
			ClusterState zkClient,
			int taskIndex, 
			int taskParall) {
		this.metaSpoutConfig = config;
		this.zkClient = zkClient;
		this.taskIndex = taskIndex;
		this.taskParallel = taskParall;
	}
	
	public void initMetaConsumer() throws MQClientException {
		LOG.info("MetaSpoutConfig:" + metaSpoutConfig);
		
		consumer = new MetaPullConsumer(metaSpoutConfig.getConsumerGroup());
		consumer.setInstanceName(taskIndex + "." + JStormUtils.process_pid());
		
		if (metaSpoutConfig.getNameServer() != null) {
			// this is for alipay
			if (nameServer == null) {
				nameServer = metaSpoutConfig.getNameServer();
				
				System.setProperty("rocketmq.namesrv.domain",
						metaSpoutConfig.getNameServer());
			}else if (metaSpoutConfig.getNameServer().equals(nameServer) == false) {
				throw new RuntimeException("Different nameserver address in the same worker "
						+ nameServer + ":" + metaSpoutConfig.getNameServer());
				
			}
		}

        consumer.start();
        
        LOG.info("Successfully start meta consumer");
	}
	
	public int getOneQueueFetchSize() {
		int batchSize = metaSpoutConfig.getBatchMsgNum();
		
		int oneFetchSize = batchSize/(taskParallel * currentOffsets.size());
		if (oneFetchSize <= 0) {
			oneFetchSize = 1;
		}
		
		LOG.info("One queue fetch size:" + oneFetchSize);
		return oneFetchSize;
	}
	
	public void init() throws Exception {
		initMetaConsumer();
		
		frontOffsets = initOffset();
		currentOffsets = frontOffsets;
		backendOffset = new HashMap<MessageQueue, Long>();
		backendOffset.putAll(frontOffsets);
		
		oneQueueFetchSize = getOneQueueFetchSize();
	}
	
	
	protected Set<MessageQueue> getMQ() throws MQClientException {
		Set<MessageQueue> mqs = consumer.
				fetchSubscribeMessageQueues(metaSpoutConfig.getTopic());
		if (taskParallel > mqs.size()) {
			throw new RuntimeException("Two much task to fetch " + metaSpoutConfig);
		}
		
		List<MessageQueue> mqList = JStormUtils.mk_list(mqs);
		Set<MessageQueue> ret = new HashSet<MessageQueue>();
		for (int i = taskIndex; i < mqList.size(); i += taskParallel) {
			ret.add(mqList.get(i));
		}
		
		if (ret.size() == 0) {
			throw new RuntimeException("No meta queue need to be consume");
		}
		return ret;
	}
	
	protected Map<MessageQueue, Long> initOffset() throws MQClientException {
		Set<MessageQueue> queues = getMQ();
		
		Map<MessageQueue, Long> ret = new HashMap<MessageQueue, Long>();
		Set<MessageQueue>       noOffsetQueues = new HashSet<MessageQueue>();
		
		
		if (metaSpoutConfig.getStartTimeStamp() != null) {
			Long timeStamp = metaSpoutConfig.getStartTimeStamp();
			
			
			for (MessageQueue mq : queues) {
				long offset = consumer.searchOffset(mq, timeStamp);
                if (offset >= 0) {
                	LOG.info("Successfully get " + mq + " offset of timestamp " + new Date(timeStamp));
                	ret.put(mq, offset);
                }else {
                	LOG.info("Failed to get " + mq + " offset of timestamp " + new Date(timeStamp));
                	noOffsetQueues.add(mq);
                }
			}
		}else  {
			noOffsetQueues.addAll(queues);
		}
		
		if (noOffsetQueues.size() == 0) {
			return ret;
		}
		
		for (MessageQueue mq : noOffsetQueues) {
			long offset = getOffsetFromZk(mq);
			
			ret.put(mq, offset);
		}
		
		return ret;
	}
	
	
	protected String getZkPath(MessageQueue mq) {
		StringBuffer sb = new StringBuffer();
		sb.append(BatchDef.ZK_SEPERATOR);
		sb.append(metaSpoutConfig.getConsumerGroup());
		sb.append(BatchDef.ZK_SEPERATOR);
		sb.append(mq.getBrokerName());
		sb.append(BatchDef.ZK_SEPERATOR);
		sb.append(mq.getQueueId());
		
		return sb.toString();
	}
	
	protected long getOffsetFromZk(MessageQueue mq) {
		String path = getZkPath(mq);
		
		try {
			if (zkClient.node_existed(path, false) == false) {
				LOG.info("No zk node of " + path);
				return 0;
			}
			
			byte[] data = zkClient.get_data(path, false);
			String value = new String(data);
			
			long ret = Long.valueOf(value);
			return ret;
		}catch (Exception e) {
			LOG.warn("Failed to get offset,", e);
			return 0;
		}
	}
	
	protected void updateOffsetToZk(MessageQueue mq, Long offset) throws Exception {
		String path = getZkPath(mq);
		byte[] data = String.valueOf(offset).getBytes();
		zkClient.set_data(path, data);

	}
	
	protected void  updateOffsetToZk(Map<MessageQueue, Long> mqs) throws Exception{
		for (Entry<MessageQueue, Long> entry : mqs.entrySet()) {
			MessageQueue mq = entry.getKey();
			Long offset = entry.getValue();
			
			updateOffsetToZk(mq, offset);
		}
		
		LOG.info("Update zk offset," + mqs);
	}
	
	protected void switchOffsetMap() {
		Map<MessageQueue, Long> tmp = frontOffsets;
		
		frontOffsets = backendOffset;
		backendOffset = tmp;
		
		currentOffsets = frontOffsets;
	}

	@Override
	public byte[] commit(BatchId id) throws FailedException {
		try {
			updateOffsetToZk(currentOffsets);
			switchOffsetMap();
		}catch(Exception e) {
			LOG.warn("Failed to update offset to ZK", e);
			throw new FailedException(e);
		}
		
		return null;
	}

	@Override
	public void revert(BatchId id, byte[] commitResult) {
		try {
			switchOffsetMap();
			updateOffsetToZk(currentOffsets);
			
		}catch(Exception e) {
			LOG.warn("Failed to update offset to ZK", e);
			throw new FailedException(e);
		}
	}
	
	/**
	 * rebalanceMqList must run after commit
	 * 
	 * @throws MQClientException
	 */
	public void rebalanceMqList() throws Exception {
		LOG.info("Begin to do rebalance operation");
		Set<MessageQueue> newMqs = getMQ();
		
		Set<MessageQueue> oldMqs = currentOffsets.keySet();
		
		if (oldMqs.equals(newMqs) == true) {
			LOG.info("No change of meta queues " + newMqs);
			return ;
		}
		
		Set<MessageQueue> removeMqs = new HashSet<MessageQueue>();
		removeMqs.addAll(oldMqs);
		removeMqs.removeAll(newMqs);
		
		Set<MessageQueue> addMqs = new HashSet<MessageQueue>();
		addMqs.addAll(newMqs);
		addMqs.removeAll(oldMqs);
		
		LOG.info("Remove " + removeMqs);
		for (MessageQueue mq : removeMqs) {
			Long offset = frontOffsets.remove(mq);
			updateOffsetToZk(mq, offset);
			
			backendOffset.remove(mq);
			
		}
		
		LOG.info("Add " + addMqs);
		for (MessageQueue mq : addMqs) {
			long offset = getOffsetFromZk(mq);
			frontOffsets.put(mq, offset);
			backendOffset.put(mq, offset);
		}
	}
	
	public List<MessageExt> fetchOneBatch() {
		List<MessageExt> ret = new ArrayList<MessageExt>();
		
		
		String subexpress = metaSpoutConfig.getSubExpress();
		for(Entry<MessageQueue, Long>entry : currentOffsets.entrySet()) {
			MessageQueue mq = entry.getKey();
			Long offset = entry.getValue();
			
			
			int fetchSize = 0;
			int oneFetchSize = Math.min(oneQueueFetchSize, 32);
			
			while(fetchSize < oneQueueFetchSize) {
				
				PullResult pullResult = null;
	            try {
	                pullResult = consumer.pullBlockIfNotFound(mq, subexpress, offset, oneFetchSize);
	                offset = pullResult.getNextBeginOffset();
	                PullStatus status = pullResult.getPullStatus();
	                if  (status == PullStatus.FOUND) {
	                	List<MessageExt> msgList = pullResult.getMsgFoundList();
	                    ret.addAll(msgList);
	                    fetchSize += msgList.size();
	                    continue;
	                }else if (status ==  PullStatus.NO_MATCHED_MSG) {
	                    continue;
	                }else if (status == PullStatus.NO_NEW_MSG ) {
	                    break;
	                }else if (status == PullStatus.OFFSET_ILLEGAL) {
	                    break;
	                }else {
	                
	                    break;
	                }
	            }
	            catch (Exception e) {
	                LOG.warn("Failed to fetch messages of " + mq + ":" + offset, e);
	                break;
	            }
			}
			
			backendOffset.put(mq, offset);
		}
		
		return ret;
	}

	public void cleanup() {
		consumer.shutdown();
	}
}

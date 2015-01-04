package com.alibaba.jstorm.kafka;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.TopologyContext;

public class PartitionCoordinator {
	private KafkaSpoutConfig config;
	private Map<Integer, PartitionConsumer> partitionConsumerMap;
	private List<PartitionConsumer> partitionConsumers;

	private ZkState zkState;
	public PartitionCoordinator(Map conf, KafkaSpoutConfig config, TopologyContext context, ZkState zkState) {
		this.config = config;
		this.zkState = zkState; 
		partitionConsumers = new LinkedList<PartitionConsumer>();
		createPartitionConsumers(conf, context);
	}

	private void createPartitionConsumers(Map conf, TopologyContext context) {
	    partitionConsumerMap = new HashMap<Integer, PartitionConsumer>();
        int taskSize = context.getComponentTasks(context.getThisComponentId()).size();
        for(int i=context.getThisTaskIndex(); i<config.numPartitions; i+=taskSize) {
            PartitionConsumer partitionConsumer = new PartitionConsumer(conf, config, i, zkState);
            partitionConsumers.add(partitionConsumer);
            partitionConsumerMap.put(i, partitionConsumer);
        }
	}

	public List<PartitionConsumer> getPartitionConsumers() {
		return partitionConsumers;
	}
	
	public PartitionConsumer getConsumer(int partition) {
		return partitionConsumerMap.get(partition);
	}
	
	public void removeConsumer(int partition) {
	    PartitionConsumer partitionConsumer = partitionConsumerMap.get(partition);
		partitionConsumers.remove(partitionConsumer);
		partitionConsumerMap.remove(partition);
	}
	
	
	 
}

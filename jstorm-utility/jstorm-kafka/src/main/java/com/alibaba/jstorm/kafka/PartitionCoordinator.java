package com.alibaba.jstorm.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import kafka.cluster.Broker;
import backtype.storm.task.TopologyContext;

public class PartitionCoordinator {
	private KafkaSpoutConfig config;
	private Map<Partition, PartitionConsumer> partitionConsumerMap;
	private List<PartitionConsumer> partitionConsumers;

	Map<Host, MessageConsumer> kafkaConsumerMap;
	ZkState zkState;
	public PartitionCoordinator(Map conf, KafkaSpoutConfig config, TopologyContext context, ZkState zkState) {
		this.config = config;
		kafkaConsumerMap = new HashMap<Host, MessageConsumer>();
		partitionConsumers = new LinkedList<PartitionConsumer>();
		this.zkState = zkState; 
		createConsumers(conf, context);
	}
	
	private MessageConsumer createConsumer(Partition partition) {
	    MessageConsumer consumer = null;
		if(!kafkaConsumerMap.containsKey(partition.getBroker())) {
		    consumer = new MessageConsumer(config, partition.getBroker());
		   
			kafkaConsumerMap.put(partition.getBroker(), consumer);
			
		}else {
		    consumer = kafkaConsumerMap.get(partition.getBroker());
		}
		consumer.addPartition(partition);
		return consumer;
	}

	private void createConsumers(Map conf, TopologyContext context) {
		partitionConsumerMap = new HashMap<Partition, PartitionConsumer>();

		List<Partition> allPartitions = new ArrayList<Partition>();
		for (Host broker : config.brokers) {
			for (int i = 0; i < config.numPartitions; i++) {
				allPartitions.add(new Partition(broker, i));
			}
		}

		int taskSize = context.getComponentTasks(context.getThisComponentId()).size();
		for (int i = context.getThisTaskIndex(); i < allPartitions.size(); i += taskSize) {
			Partition partition = allPartitions.get(i);
			MessageConsumer smc = createConsumer(partition);
			PartitionConsumer partitionConsumer = new PartitionConsumer(conf, smc ,config, partition, zkState);
			partitionConsumer.setCoordinator(this);
			partitionConsumerMap.put(partition, partitionConsumer);
			partitionConsumers.add(partitionConsumer);
		}
	}

	public List<PartitionConsumer> getPartitionConsumers() {
		return partitionConsumers;
	}
	
	public PartitionConsumer getConsumer(Partition partition) {
		return partitionConsumerMap.get(partition);
	}
	
	public void removeConsumer(Partition partition) {
	    PartitionConsumer partitionConsumer = partitionConsumerMap.get(partition);
		partitionConsumers.remove(partitionConsumer);
		partitionConsumerMap.remove(partition);
		MessageConsumer simpleConsumer = kafkaConsumerMap.get(partition.getBroker());
		if(simpleConsumer != null) {
		    simpleConsumer.removePartition(partition);
		    if(simpleConsumer.getPartitions().isEmpty()) {
		        simpleConsumer.close();
		    }
		}
	}
	
	
	 
}

package com.alibaba.jstorm.kafka;

import java.io.Serializable;

public class Partition  implements Serializable{
	private Host broker;
	private int partition;

	public Partition(Host broker, int partition) {
		this.broker = broker;
		this.partition = partition;
	}

	public Host getBroker() {
		return broker;
	}

	public void setBroker(Host broker) {
		this.broker = broker;
	}

	public int getPartition() {
		return partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

}

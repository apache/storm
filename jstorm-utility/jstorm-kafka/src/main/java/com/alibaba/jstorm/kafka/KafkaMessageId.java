package com.alibaba.jstorm.kafka;

public class KafkaMessageId {
	private int partition;
	private long offset;
    
    public KafkaMessageId(int partition, long offset) {
        this.setPartition(partition);
        this.setOffset(offset);
    }

	public int getPartition() {
		return partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}
}

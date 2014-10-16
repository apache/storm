package com.alibaba.jstorm.kafka;

public class KafkaMessageId {
	private Partition partition;
	private long offset;
    
    public KafkaMessageId(Partition partition, long offset) {
        this.setPartition(partition);
        this.setOffset(offset);
    }

	public Partition getPartition() {
		return partition;
	}

	public void setPartition(Partition partition) {
		this.partition = partition;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}
}

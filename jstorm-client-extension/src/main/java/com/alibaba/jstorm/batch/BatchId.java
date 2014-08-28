package com.alibaba.jstorm.batch;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

public class BatchId implements Serializable {
	private static final long serialVersionUID = 5720810158625748049L;
	protected final long id;

	protected BatchId(long id) {
		this.id = id;
	}

	public long getId() {
		return id;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (id ^ (id >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BatchId other = (BatchId) obj;
		if (id != other.id)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
	}

	private static AtomicLong staticId = new AtomicLong(0);
	
	public static void updateId(long id) {
		staticId.set(id);
	}

	public static BatchId mkInstance() {
		long id = staticId.incrementAndGet();

		return new BatchId(id);
	}
	
	public static BatchId incBatchId(BatchId old) {
		long other = old.getId();
		return new BatchId(other + 1);
	}

}

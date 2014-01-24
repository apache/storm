package com.alibaba.jstorm.resource;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.log4j.Logger;

public class SharedResourcePool implements ResourcePool<Integer>, Serializable {
	private static final Logger LOG = Logger
			.getLogger(SharedResourcePool.class);

	/**  */
	private static final long serialVersionUID = 618811509112791661L;

	private final ResourceType type;
	private final int totalSlotNum;
	private AtomicInteger freeSlotNum;

	public SharedResourcePool(final ResourceType type, final int totalSlotNum) {
		this.type = type;
		this.totalSlotNum = totalSlotNum;
		this.freeSlotNum = new AtomicInteger(totalSlotNum);

		LOG.info("Successfully init " + this.getClass().getSimpleName() + ":"
				+ this.toString());
	}

	@Override
	public ResourceType getType() {
		return type;
	}

	@Override
	public int getTotalNum() {
		return totalSlotNum;
	}

	@Override
	public int getLeftNum() {
		return freeSlotNum.get();
	}

	@Override
	public int getUsedNum() {
		return totalSlotNum - freeSlotNum.get();
	}

	public Integer alloc(Integer slotNum, Object context) {

		LOG.debug(context + " alloc " + type + ":" + slotNum);

		if (slotNum == null) {
			LOG.info(context + " alloc " + type + ":" + slotNum);
			return null;
		}

		if (freeSlotNum.get() >= slotNum) {
			freeSlotNum.addAndGet(-slotNum);
			return slotNum;
		} else
			return null;
	}

	@Override
	public Integer alloc(Object context) {

		return alloc(Integer.valueOf(1), context);
	}

	/**
	 * @@@ Here skip double free check
	 * @see com.alibaba.jstorm.resource.ResourcePool#free(java.lang.Object,
	 *      java.lang.Object)
	 */
	@Override
	public void free(Integer slotNum, Object context) {

		LOG.debug(context + " free " + type + ":" + slotNum);

		if (slotNum != null) {
			freeSlotNum.addAndGet(slotNum);
		}
	}

	@Override
	public boolean isAvailable(Integer slotNum, Object context) {
		if (slotNum == null) {
			LOG.info(context + " check available " + type + ":" + slotNum);
			return true;
		}
		if (freeSlotNum.get() >= slotNum)
			return true;
		else
			return false;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof SharedResourcePool == false) {
			return false;
		}

		SharedResourcePool otherPool = (SharedResourcePool) obj;
		return otherPool.getType().equals(type)
				&& otherPool.totalSlotNum == totalSlotNum
				&& otherPool.freeSlotNum.equals(freeSlotNum);
	}

	@Override
	public int hashCode() {
		return type.hashCode() + totalSlotNum + freeSlotNum.hashCode();
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
	}

}

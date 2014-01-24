package com.alibaba.jstorm.resource;

import java.io.Serializable;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

//one task 's assignment
public class TaskAllocResource implements Serializable {

	/**  */
	private static final long serialVersionUID = -8950078487305922497L;
	private final boolean allocDisk;
	private final int cpuSlotNum;
	private final int memSlotNum;

	public TaskAllocResource(boolean allocDisk, int cpuSlotNum, int memSlotNum) {
		super();
		this.allocDisk = allocDisk;
		this.cpuSlotNum = cpuSlotNum;
		this.memSlotNum = memSlotNum;
	}

	public TaskAllocResource(ResourceAssignment resourceAssignment) {
		super();
		this.allocDisk = !StringUtils.isBlank(resourceAssignment.getDiskSlot());
		this.cpuSlotNum = resourceAssignment.getCpuSlotNum();
		this.memSlotNum = resourceAssignment.getMemSlotNum();
	}

	public boolean isAllocDisk() {
		return allocDisk;
	}

	public int getCpuSlotNum() {
		return cpuSlotNum;
	}

	public int getMemSlotNum() {
		return memSlotNum;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (allocDisk ? 1231 : 1237);
		result = prime * result + cpuSlotNum;
		result = prime * result + memSlotNum;
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
		TaskAllocResource other = (TaskAllocResource) obj;
		if (allocDisk != other.allocDisk)
			return false;
		if (cpuSlotNum != other.cpuSlotNum)
			return false;
		if (memSlotNum != other.memSlotNum)
			return false;
		return true;
	}

	public boolean allocEqual(ResourceAssignment other) {
		if (other == null) {
			return false;
		}

		if (other.getCpuSlotNum() == null) {
			return false;
		} else if (cpuSlotNum != other.getCpuSlotNum()) {
			return false;
		}

		if (other.getMemSlotNum() == null) {
			return false;
		} else if (memSlotNum != other.getMemSlotNum()) {
			return false;
		}

		if (allocDisk == true) {
			if (other.getDiskSlot() == null) {
				return false;
			}
		} else {
			if (other.getDiskSlot() != null) {
				return false;
			}
		}

		return true;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
	}
}

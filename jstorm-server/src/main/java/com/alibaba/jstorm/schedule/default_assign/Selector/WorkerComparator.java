package com.alibaba.jstorm.schedule.default_assign.Selector;

import java.util.Comparator;

import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;

public abstract class WorkerComparator implements Comparator<ResourceWorkerSlot> {
	
	protected String name;
	
	public WorkerComparator get(String name) {
		this.name = name;
		return this;
	}
}

package com.alibaba.jstorm.schedule.default_assign.Selector;

import java.util.Set;

import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;

public interface Selector {
	public Set<ResourceWorkerSlot> select(Set<ResourceWorkerSlot> result,
			String name);
}

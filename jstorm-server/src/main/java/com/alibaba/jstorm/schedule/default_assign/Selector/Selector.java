package com.alibaba.jstorm.schedule.default_assign.Selector;

import java.util.List;

import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;

public interface Selector {
	public List<ResourceWorkerSlot> select(List<ResourceWorkerSlot> result,
			String name);
}

package com.alibaba.jstorm.schedule;

import java.util.Map;
import java.util.Set;

import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.utils.FailedAssignTopologyException;

public interface IToplogyScheduler {
	void prepare(Map conf);

	Set<ResourceWorkerSlot> assignTasks(TopologyAssignContext contex)
			throws FailedAssignTopologyException;
}

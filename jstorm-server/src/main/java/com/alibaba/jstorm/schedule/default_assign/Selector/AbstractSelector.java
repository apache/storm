package com.alibaba.jstorm.schedule.default_assign.Selector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.schedule.default_assign.TaskGankerContext;

public abstract class AbstractSelector implements Selector {

	protected final TaskGankerContext context;

	protected WorkerComparator workerComparator;

	protected WorkerComparator supervisorComparator;

	public AbstractSelector(TaskGankerContext context) {
		this.context = context;
	}

	protected List<ResourceWorkerSlot> selectWorker(
			List<ResourceWorkerSlot> list, Comparator<ResourceWorkerSlot> c) {
		List<ResourceWorkerSlot> result = new ArrayList<ResourceWorkerSlot>();
		ResourceWorkerSlot best = null;
		for (ResourceWorkerSlot worker : list) {
			if (best == null) {
				best = worker;
				result.add(worker);
				continue;
			}
			if (c.compare(best, worker) == 0) {
				result.add(worker);
			} else if (c.compare(best, worker) > 0) {
				best = worker;
				result.clear();
				result.add(best);
			}
		}
		return result;
	}

	@Override
	public List<ResourceWorkerSlot> select(List<ResourceWorkerSlot> result,
			String name) {
		if (result.size() == 1)
			return result;
		result = this.selectWorker(result, workerComparator.get(name));
		if (result.size() == 1)
			return result;
		return this.selectWorker(result, supervisorComparator.get(name));
	}

}

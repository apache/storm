package com.alibaba.jstorm.schedule.default_assign.Selector;

import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.schedule.default_assign.TaskGankerContext;

public class TotalTaskNumSelector extends AbstractSelector {

	public TotalTaskNumSelector(final TaskGankerContext context) {
		super(context);
		// TODO Auto-generated constructor stub
		this.workerComparator = new WorkerComparator() {
			@Override
			public int compare(ResourceWorkerSlot o1, ResourceWorkerSlot o2) {
				// TODO Auto-generated method stub
				int o1Num = context.getTaskNumOnWorker(o1);
				int o2Num = context.getTaskNumOnWorker(o2);
				if (o1Num == o2Num)
					return 0;
				return o1Num > o2Num ? 1 : -1;
			}
		};
		this.supervisorComparator = new WorkerComparator() {
			@Override
			public int compare(ResourceWorkerSlot o1, ResourceWorkerSlot o2) {
				// TODO Auto-generated method stub
				int o1Num = context.getTaskNumOnSupervisor(o1.getNodeId());
				int o2Num = context.getTaskNumOnSupervisor(o2.getNodeId());
				if (o1Num == o2Num)
					return 0;
				return o1Num > o2Num ? 1 : -1;
			}
		};
	}

}

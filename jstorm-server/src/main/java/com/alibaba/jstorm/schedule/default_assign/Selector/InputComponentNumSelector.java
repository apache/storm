package com.alibaba.jstorm.schedule.default_assign.Selector;

import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.schedule.default_assign.TaskGankerContext;

public class InputComponentNumSelector extends AbstractSelector {

	public InputComponentNumSelector(final TaskGankerContext context) {
		super(context);
		// TODO Auto-generated constructor stub
		this.workerComparator = new WorkerComparator() {
			@Override
			public int compare(ResourceWorkerSlot o1, ResourceWorkerSlot o2) {
				// TODO Auto-generated method stub
				int o1Num = context.getInputComponentNumOnWorker(o1, name);
				int o2Num = context.getInputComponentNumOnWorker(o2, name);
				if (o1Num == o2Num)
					return 0;
				return o1Num > o2Num ? -1 : 1;
			}
		};
		this.supervisorComparator = new WorkerComparator() {
			@Override
			public int compare(ResourceWorkerSlot o1, ResourceWorkerSlot o2) {
				// TODO Auto-generated method stub
				int o1Num = context.getInputComponentNumOnSupervisor(
						o1.getNodeId(), name);
				int o2Num = context.getInputComponentNumOnSupervisor(
						o2.getNodeId(), name);
				if (o1Num == o2Num)
					return 0;
				return o1Num > o2Num ? -1 : 1;
			}
		};
	}
}

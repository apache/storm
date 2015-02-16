package com.alibaba.jstorm.task.group;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.RandomRange;

public class MkLocalShuffer extends Shuffer{

	private List<Integer> outTasks;
	private RandomRange randomrange;
	private boolean isLocal;

	public MkLocalShuffer(List<Integer> workerTasks, List<Integer> allOutTasks, WorkerData workerData) {
	    super(workerData);
		List<Integer> localOutTasks = new ArrayList<Integer>();

		for (Integer outTask : allOutTasks) {
			if (workerTasks.contains(outTask)) {
				localOutTasks.add(outTask);
			}
		}

		if (localOutTasks.size() != 0) {
			this.outTasks = localOutTasks;
			isLocal = true;
		} else {
			this.outTasks = new ArrayList<Integer>() ;
			this.outTasks.addAll(allOutTasks);
			isLocal = false;
		}

		randomrange = new RandomRange(outTasks.size());
	}

	public List<Integer> grouper(List<Object> values) {
		int index = getActiveTask(randomrange, outTasks);
		// If none active tasks were found, still send message to a task
		if (index == -1)
		    index = randomrange.nextInt();

		return JStormUtils.mk_list(outTasks.get(index));
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}

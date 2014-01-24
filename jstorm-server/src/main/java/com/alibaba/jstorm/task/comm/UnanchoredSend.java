package com.alibaba.jstorm.task.comm;

import java.util.List;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.TupleImplExt;

import com.alibaba.jstorm.task.TaskTransfer;

/**
 * Send init/ack/fail tuple to acker
 * 
 * @author yannian
 * 
 */

public class UnanchoredSend {
	public static void send(TopologyContext topologyContext,
			TaskSendTargets taskTargets, TaskTransfer transfer_fn,
			String stream, List<Object> values) {

		java.util.List<Integer> tasks = taskTargets.get(stream, values);
		if (tasks.size() == 0) {
			return;
		}

		Integer taskId = topologyContext.getThisTaskId();

		for (Integer task : tasks) {
			TupleImplExt tup = new TupleImplExt(topologyContext, values,
					taskId, stream);
			tup.setTargetTaskId(task);

			transfer_fn.transfer(tup);
		}
	}
}

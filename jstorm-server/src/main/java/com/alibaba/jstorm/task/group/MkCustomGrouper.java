package com.alibaba.jstorm.task.group;

import java.util.List;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.TopologyContext;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Fields;

/**
 * user defined grouping method
 * 
 * @author Longda/yannian
 * 
 */
public class MkCustomGrouper {
	private CustomStreamGrouping grouping;

	private int myTaskId;

	public MkCustomGrouper(TopologyContext context,
			CustomStreamGrouping _grouping, GlobalStreamId stream,
			List<Integer> targetTask, int myTaskId) {
		this.myTaskId = myTaskId;
		this.grouping = _grouping;
		this.grouping.prepare(context, stream, targetTask);

	}

	public List<Integer> grouper(List<Object> values) {
		return this.grouping.chooseTasks(myTaskId, values);
	}
}

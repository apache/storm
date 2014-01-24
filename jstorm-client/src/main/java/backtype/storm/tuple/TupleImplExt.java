package backtype.storm.tuple;

import java.util.List;

import backtype.storm.task.GeneralTopologyContext;

public class TupleImplExt extends TupleImpl implements TupleExt {

	protected int targetTaskId;

	public TupleImplExt(GeneralTopologyContext context, List<Object> values,
			int taskId, String streamId) {
		super(context, values, taskId, streamId);
	}

	public TupleImplExt(GeneralTopologyContext context, List<Object> values,
			int taskId, String streamId, MessageId id) {
		super(context, values, taskId, streamId, id);
	}

	@Override
	public int getTargetTaskId() {
		return targetTaskId;
	}

	@Override
	public void setTargetTaskId(int targetTaskId) {
		this.targetTaskId = targetTaskId;
	}

}

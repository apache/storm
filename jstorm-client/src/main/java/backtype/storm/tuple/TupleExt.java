package backtype.storm.tuple;

public interface TupleExt extends Tuple {
	/**
	 * Get Target TaskId
	 * 
	 * @return
	 */
	int getTargetTaskId();

	void setTargetTaskId(int targetTaskId);
}

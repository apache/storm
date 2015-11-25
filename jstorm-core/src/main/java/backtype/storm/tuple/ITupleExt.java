package backtype.storm.tuple;

public interface ITupleExt {
    
    /**
     * Get Target TaskId
     * 
     * @return
     */
    int getTargetTaskId();

    void setTargetTaskId(int targetTaskId);

    /**
     * Get the timeStamp of creating tuple
     * 
     * @return
     */
    long getCreationTimeStamp();

    /*
     * set ms
     */
    void setCreationTimeStamp(long timeStamp);
}

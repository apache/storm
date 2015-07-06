package storm.kafka;

/**
 * Created by olgagorun on 6/29/15.
 */
public interface IFailureRepository {
    public void putTuple(Object tuple, long offset, String topic, String partition);
    public Object get(long offset);
}

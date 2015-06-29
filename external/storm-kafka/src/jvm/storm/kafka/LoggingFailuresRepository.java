package storm.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by olgagorun on 6/29/15.
 */
public class LoggingFailuresRepository implements IFailureRepository{
    private static final Logger LOG = LoggerFactory.getLogger(LoggingFailuresRepository.class);

    public void putTuple(Object tuple, long offset) {
      LOG.info("putTuple(" + tuple.toString() + ", " + offset + ")");

    }

    public Object get(long offset) {
        LOG.info("get(" + offset + ")");
        return null;
    }
}

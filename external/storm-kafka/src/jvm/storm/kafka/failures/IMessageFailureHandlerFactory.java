package storm.kafka.failures;

import storm.kafka.FailedMsgRetryManager;
import storm.kafka.PartitionManager;

import java.util.Map;

/**
 * Created by olgagorun on 1/29/15.
 */
public interface IMessageFailureHandlerFactory {

    public FailedMsgRetryManager getHandler(PartitionManager pm, Map conf);
}

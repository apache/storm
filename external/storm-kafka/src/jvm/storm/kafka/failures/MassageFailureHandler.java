package storm.kafka.failures;

import kafka.message.MessageAndOffset;

/**
 * Created by olgagorun on 1/28/15.
 */
public interface MassageFailureHandler {

    long getOffset(Long expectedOffset);

    boolean isOffsetFailed(long offset);

    void startMessageProcessing(MessageAndOffset enrichedMessage);

    void fail(Long offset) throws RuntimeException;
}

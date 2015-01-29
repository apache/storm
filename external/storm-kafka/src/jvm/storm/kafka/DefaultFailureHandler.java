package storm.kafka;

import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.failures.IMassageFailureHandler;

import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Created by olgagorun on 1/28/15.
 *
 * Note: this class is not moved to failures package to be able to use package access level members of PartitionManager
 *
 */
public class DefaultFailureHandler implements IMassageFailureHandler {
    public static final Logger LOG = LoggerFactory.getLogger(DefaultFailureHandler.class);

    SortedSet<Long> failed;
    long numberFailed;
    long maxOffsetBehind;
    PartitionManager _pm;

    public DefaultFailureHandler(PartitionManager pm) {
        failed = new TreeSet<Long>();
        numberFailed = 0;
        _pm = pm;
        maxOffsetBehind = pm._spoutConfig.maxOffsetBehind;
    }

    @Override
    public long getOffset(Long expectedOffset) {
        return !failed.isEmpty() ? failed.first() : -1;
    }

    @Override
    public boolean isOffsetFailed(long offset) {
        return failed.contains(offset);
    }

    @Override
    public void startMessageProcessing(MessageAndOffset enrichedMessage) {
        Long cur_offset = enrichedMessage.offset();
        if (failed.contains(cur_offset)) {
           failed.remove(cur_offset);
        }
    }

    @Override
    public void fail(Long offset) throws RuntimeException {

        if (offset < _pm._emittedToOffset - maxOffsetBehind) {
            LOG.info(
                    "Skipping failed tuple at offset=" + offset +
                            " because it's more than maxOffsetBehind=" + maxOffsetBehind +
                            " behind _emittedToOffset=" + _pm._emittedToOffset
            );
        } else {
            LOG.debug("failing at offset=" + offset + " with _pending.size()=" + _pm._pending.size() + " pending and _emittedToOffset=" + _pm._emittedToOffset);
            failed.add(offset);
            numberFailed++;
            if (_pm.numberAcked == 0 && numberFailed > maxOffsetBehind) {
                throw new RuntimeException("Too many tuple failures");
            }
        }

    }

    public void ack(Long offset) {}
}

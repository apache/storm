package storm.kafka;

import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.failures.IMassageFailureHandler;

import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by olgagorun on 1/29/15.
 *
 * Note: this class is not moved to failures package to be able to use package access level members of PartitionManager
 */
public class ConfigurableRetriesFailureHandler implements IMassageFailureHandler {
    public static final Logger LOG = LoggerFactory.getLogger(ConfigurableRetriesFailureHandler.class);

    ConcurrentSkipListMap<Long,Integer> failed;
    ConcurrentSkipListMap<Long,Integer> pending;

    long numberFailed;
    long maxOffsetBehind;
    int _retriesNumber;
    PartitionManager _pm;

    public ConfigurableRetriesFailureHandler(PartitionManager pm, int retriesNumber) {
        failed = new ConcurrentSkipListMap<Long, Integer>();
        pending = new ConcurrentSkipListMap<Long, Integer>();

        numberFailed = 0;
        _pm = pm;
        maxOffsetBehind = pm._spoutConfig.maxOffsetBehind;

        _retriesNumber = retriesNumber;
    }

    @Override
    public long getOffset(Long expectedOffset) {
        return !failed.isEmpty() ? failed.entrySet().iterator().next().getKey() : -1;
    }

    @Override
    public boolean isOffsetFailed(long offset) {
        return failed.containsKey(offset);
    }

    @Override
    public void startMessageProcessing(MessageAndOffset enrichedMessage) {
        Long cur_offset = enrichedMessage.offset();
        if (failed.containsKey(cur_offset)) {
            Integer value = failed.remove(cur_offset);
            pending.put(cur_offset, value);
        }
    }

    @Override
    public void fail(Long offset) throws RuntimeException {
        if (offset < _pm._emittedToOffset - maxOffsetBehind) {
            failed.remove(offset);
            pending.remove(offset);
            LOG.info(
                    "Skipping failed tuple at offset=" + offset +
                            " because it's more than maxOffsetBehind=" + maxOffsetBehind +
                            " behind _emittedToOffset=" + _pm._emittedToOffset
            );
            return;
        }

        numberFailed++;
        if (_pm.numberAcked == 0 && numberFailed > maxOffsetBehind) {
            throw new RuntimeException("Too many tuple failures");
        }

        LOG.debug("failing at offset=" + offset + " with _pending.size()=" + _pm._pending.size() + " pending and _emittedToOffset=" + _pm._emittedToOffset);
        Integer number = pending.remove(offset);
        number = number == null ? 1 : number + 1;
        pending.remove(offset);

        if (number >= _retriesNumber) {
            LOG.debug(String.format("failing at offset=%s %s of times. Configured limit: %s", offset, number, _retriesNumber));
            failed.remove(offset);
        }
        else {
            failed.put(offset, number);
        }

    }

    @Override
    public void ack(Long offset) {
        failed.remove(offset);
        pending.remove(offset);
    }
}

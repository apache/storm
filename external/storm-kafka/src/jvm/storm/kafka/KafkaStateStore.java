package storm.kafka;

import com.google.common.collect.Maps;
import kafka.api.ConsumerMetadataRequest;
import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.network.BlockingChannel;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KafkaStateStore implements StateStore {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaStateStore.class);

    private SpoutConfig _spoutConfig;

    private int _correlationId = 0;
    // https://en.wikipedia.org/wiki/Double-checked_locking#Usage_in_Java
    private volatile BlockingChannel _offsetManager;

    public KafkaStateStore(Map stormConf, SpoutConfig spoutConfig) {
        this._spoutConfig = spoutConfig;
    }

    @Override
    public void writeState(Partition p, Map<Object, Object> state) {
        assert state.containsKey("offset");

        Long offsetOfPartition = (Long)state.get("offset");
        String stateData = JSONValue.toJSONString(state);
        write(offsetOfPartition, stateData, p);
    }

    @Override
    public Map<Object, Object> readState(Partition p) {
        return  (Map<Object, Object>) JSONValue.parse(read(p));
    }

    @Override
    public void close() {
        _offsetManager.disconnect();
        _offsetManager = null;
    }

    private BlockingChannel getOffsetManager(Partition partition) {
        if (_offsetManager == null) {
            _offsetManager = locateOffsetManager(partition);
        }
        return _offsetManager;
    }

    // supposedly we only need to locate offset manager once. Other cases, such as the offsetManager
    // gets relocated, should be rare. So it is ok to sync.
    //
    // although we take a particular partition to locate the offset manager, the location of the
    // offset manager should apply to the entire consumer group
    private synchronized BlockingChannel locateOffsetManager(Partition partition) {

        // if another invocation has already
        if (_offsetManager != null) {
            return _offsetManager;
        }

        BlockingChannel channel = new BlockingChannel(partition.host.host, partition.host.port,
                BlockingChannel.UseDefaultBufferSize(),
                BlockingChannel.UseDefaultBufferSize(),
                1000000 /* read timeout in millis */);
        channel.connect();

        ConsumerMetadataResponse metadataResponse = null;
        long backoffMillis = 3000L;
        int maxRetry = 3;
        int retryCount = 0;
        // this usually only happens when the internal offsets topic does not exist before and we need to wait until
        // the topic is automatically created and the meta data are populated across cluster. So we hard-code the retry here.

        // one scenario when this could happen is during unit test.
        while (retryCount < maxRetry) {
            channel.send(new ConsumerMetadataRequest(_spoutConfig.id, ConsumerMetadataRequest.CurrentVersion(),
                    _correlationId++, _spoutConfig.clientId));
            metadataResponse = ConsumerMetadataResponse.readFrom(channel.receive().buffer());
            if (metadataResponse.errorCode() == ErrorMapping.ConsumerCoordinatorNotAvailableCode()) {
                LOG.warn("Failed to get coordinator: " + metadataResponse.errorCode());
                retryCount++;
                try {
                    Thread.sleep(backoffMillis);
                } catch (InterruptedException e) {
                    // eat the exception
                }
            } else {
                break;
            }
        }

        assert (metadataResponse != null);
        if (metadataResponse.errorCode() == ErrorMapping.NoError()) {
            kafka.cluster.Broker offsetManager = metadataResponse.coordinator();
            if (!offsetManager.host().equals(partition.host.host)
                    || !(offsetManager.port() == partition.host.port)) {
                // if the coordinator is different, from the above channel's host then reconnect
                channel.disconnect();
                channel = new BlockingChannel(offsetManager.host(), offsetManager.port(),
                        BlockingChannel.UseDefaultBufferSize(),
                        BlockingChannel.UseDefaultBufferSize(),
                        _spoutConfig.stateOpTimeout /* read timeout in millis */);
                channel.connect();
            }
        } else {
            throw new RuntimeException("Kafka metadata fetch error: " + metadataResponse.errorCode());
        }
        return channel;
    }

    private String attemptToRead(Partition partition) {
        List<TopicAndPartition> partitions = new ArrayList<TopicAndPartition>();
        TopicAndPartition thisTopicPartition = new TopicAndPartition(_spoutConfig.topic, partition.partition);
        partitions.add(thisTopicPartition);
        OffsetFetchRequest fetchRequest = new OffsetFetchRequest(
                _spoutConfig.id,
                partitions,
                (short) 1, // version 1 and above fetch from Kafka, version 0 fetches from ZooKeeper
                _correlationId++,
                _spoutConfig.clientId);

        BlockingChannel offsetManager = getOffsetManager(partition);
        offsetManager.send(fetchRequest.underlying());
        OffsetFetchResponse fetchResponse = OffsetFetchResponse.readFrom(offsetManager.receive().buffer());
        OffsetMetadataAndError result = fetchResponse.offsets().get(thisTopicPartition);
        if (result.error() == ErrorMapping.NoError()) {
            String retrievedMetadata = result.metadata();
            if (retrievedMetadata != null) {
                return retrievedMetadata;
            } else {
                // let it return null, this maybe the first time it is called before the state is persisted
                return null;
            }

        } else {
            _offsetManager = null;
            throw new RuntimeException("Kafka offset fetch error: " + result.error());
        }
    }

    private String read(Partition partition) {
        int attemptCount = 0;
        while (true) {
            try {
                return attemptToRead(partition);

            } catch(RuntimeException re) {
                if (++attemptCount > _spoutConfig.stateOpMaxRetry) {
                    _offsetManager = null;
                    throw re;
                } else {
                    LOG.warn("Attempt " + attemptCount + " out of " + _spoutConfig.stateOpMaxRetry
                            + ". Failed to fetch state for partition " + partition.partition
                            + " of topic " + _spoutConfig.topic + " due to Kafka offset fetch error: " + re.getMessage());
                }
            }
        }
    }

    private void attemptToWrite(long offsetOfPartition, String state, Partition partition) {
        long now = System.currentTimeMillis();
        Map<TopicAndPartition, OffsetAndMetadata> offsets = Maps.newLinkedHashMap();
        TopicAndPartition thisTopicPartition = new TopicAndPartition(_spoutConfig.topic, partition.partition);
        offsets.put(thisTopicPartition, new OffsetAndMetadata(
                offsetOfPartition,
                state,
                now));
        OffsetCommitRequest commitRequest = new OffsetCommitRequest(
                _spoutConfig.id,
                offsets,
                _correlationId,
                _spoutConfig.clientId,
                (short) 1); // version 1 and above commit to Kafka, version 0 commits to ZooKeeper

        BlockingChannel offsetManager = getOffsetManager(partition);
        offsetManager.send(commitRequest.underlying());
        OffsetCommitResponse commitResponse = OffsetCommitResponse.readFrom(offsetManager.receive().buffer());
        if (commitResponse.hasError()) {
            // note: here we should have only 1 error for the partition in request
            for (Object partitionErrorCode : commitResponse.errors().values()) {
                if (partitionErrorCode.equals(ErrorMapping.OffsetMetadataTooLargeCode())) {
                    throw new RuntimeException("Data is too big. The data object is " + state);
                } else {
                    _offsetManager = null;
                    throw new RuntimeException("Kafka offset commit error: " + partitionErrorCode);
                }
            }
        }
    }

    private void write(Long offsetOfPartition, String state, Partition partition) {
        int attemptCount = 0;
        while (true) {
            try {
                attemptToWrite(offsetOfPartition, state, partition);
                return;

            } catch(RuntimeException re) {
                if (++attemptCount > _spoutConfig.stateOpMaxRetry) {
                    _offsetManager = null;
                    throw re;
                } else {
                    LOG.warn("Attempt " + attemptCount + " out of " + _spoutConfig.stateOpMaxRetry
                            + ". Failed to save state for partition " + partition.partition
                            + " of topic " + _spoutConfig.topic + " due to Kafka offset commit error: " + re.getMessage());
                }
            }
        }
    }
}

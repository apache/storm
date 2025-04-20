package org.apache.storm.kafka.spout.metrics2;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.storm.kafka.spout.internal.OffsetManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

/**
 * This class is used compute the partition and topic level offset metrics.
 * <p>
 * Partition level metrics are:
 * topicName/partition_{number}/earliestTimeOffset //gives beginning offset of the partition
 * topicName/partition_{number}/latestTimeOffset //gives end offset of the partition
 * topicName/partition_{number}/latestEmittedOffset //gives latest emitted offset of the partition from the spout
 * topicName/partition_{number}/latestCompletedOffset //gives latest committed offset of the partition from the spout
 * topicName/partition_{number}/spoutLag // the delta between the latest Offset and latestCompletedOffset
 * topicName/partition_{number}/recordsInPartition // total number of records in the partition
 * </p>
 * <p>
 * Topic level metrics are:
 * topicName/totalEarliestTimeOffset //gives the total beginning offset of all the associated partitions of this spout
 * topicName/totalLatestTimeOffset //gives the total end offset of all the associated partitions of this spout
 * topicName/totalLatestEmittedOffset //gives the total latest emitted offset of all the associated partitions of this spout
 * topicName/totalLatestCompletedOffset //gives the total latest committed offset of all the associated partitions of this spout
 * topicName/spoutLag // total spout lag of all the associated partitions of this spout
 * topicName/totalRecordsInPartitions //total number of records in all the associated partitions of this spout
 * </p>
 */
public class KafkaOffsetPartitionAndTopicMetrics <K, V> implements MetricSet {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaOffsetPartitionAndTopicMetrics.class);
    private final Supplier<Map<TopicPartition, OffsetManager>> offsetManagerSupplier;
    private final Supplier<Admin> adminSupplier;
    private final Set<TopicPartition> assignment;
    private Map<String, KafkaOffsetTopicMetrics> topicMetricsMap;


    public KafkaOffsetPartitionAndTopicMetrics(Supplier<Map<TopicPartition, OffsetManager>> offsetManagerSupplier, Supplier<Admin> adminSupplier, Set<TopicPartition> assignment) {
        this.offsetManagerSupplier = offsetManagerSupplier;
        this.adminSupplier = adminSupplier;
        this.assignment = assignment;
    }

    @Override
    public Map<String, Metric> getMetrics() {

        Map<String, Metric> metrics = new HashMap<>();

        for (TopicPartition topicPartition : assignment) {

            String topic=topicPartition.topic();
            KafkaOffsetTopicMetrics topicMetrics = topicMetricsMap.get(topic);
            if (topicMetrics == null) {
                topicMetrics = new KafkaOffsetTopicMetrics(topic);
                topicMetricsMap.put(topic, topicMetrics);
            }

            String metricPath = topicPartition.topic()  + "/partition_" + topicPartition.partition();
            KafkaOffsetTopicMetrics finalTopicMetrics = topicMetrics;
            Gauge<Long> spoutLagGauge = () -> {
                Map<TopicPartition, Long> endOffsets = getEndOffsets(Collections.singleton(topicPartition));
                if (endOffsets == null || endOffsets.isEmpty()) {
                    LOG.error("Failed to get endOffsets from Kafka for topic partitions: {}.", topicPartition);
                    return 0L;
                }
                // add value to topic level metric
                OffsetManager offsetManager = offsetManagerSupplier.get().get(topicPartition);
                Long ret = endOffsets.get(topicPartition) - offsetManager.getCommittedOffset();
                finalTopicMetrics.totalSpoutLag += ret;
                return ret;
            };

            Gauge<Long> earliestTimeOffsetGauge = () -> {
                Map<TopicPartition, Long> beginningOffsets = getBeginningOffsets(Collections.singleton(topicPartition));
                if (beginningOffsets == null || beginningOffsets.isEmpty()) {
                    LOG.error("Failed to get beginningOffsets from Kafka for topic partitions: {}.", topicPartition);
                    return 0L;
                }
                // add value to topic level metric
                Long ret = beginningOffsets.get(topicPartition);
                finalTopicMetrics.totalEarliestTimeOffset += ret;
                return ret;
            };

            Gauge<Long> latestTimeOffsetGauge = () -> {
                Map<TopicPartition, Long> endOffsets = getEndOffsets(Collections.singleton(topicPartition));
                if (endOffsets == null || endOffsets.isEmpty()) {
                    LOG.error("Failed to get endOffsets from Kafka for topic partitions: {}.", topicPartition);
                    return 0L;
                }
                // add value to topic level metric
                Long ret = endOffsets.get(topicPartition);
                finalTopicMetrics.totalLatestTimeOffset += ret;
                return ret;
            };

            Gauge<Long> latestEmittedOffsetGauge = () -> {
                // add value to topic level metric
                OffsetManager offsetManager = offsetManagerSupplier.get().get(topicPartition);
                Long ret = offsetManager.getLatestEmittedOffset();
                finalTopicMetrics.totalLatestEmittedOffset+=ret;
                return ret;
            };

            Gauge<Long> latestCompletedOffsetGauge = () -> {
                // add value to topic level metric
                OffsetManager offsetManager = offsetManagerSupplier.get().get(topicPartition);
                Long ret = offsetManager.getCommittedOffset();
                finalTopicMetrics.totalLatestCompletedOffset+=ret;
                return ret;
            };

            Gauge<Long> recordsInPartitionGauge = () -> {
                Map<TopicPartition, Long> endOffsets = getEndOffsets(Collections.singleton(topicPartition));
                if (endOffsets == null || endOffsets.isEmpty()) {
                    LOG.error("Failed to get endOffsets from Kafka for topic partitions: {}.", topicPartition);
                    return 0L;
                }
                Map<TopicPartition, Long> beginningOffsets = getBeginningOffsets(Collections.singleton(topicPartition));
                if (beginningOffsets == null || beginningOffsets.isEmpty()) {
                    LOG.error("Failed to get beginningOffsets from Kafka for topic partitions: {}.", topicPartition);
                    return 0L;
                }
                // add value to topic level metric
                Long ret = endOffsets.get(topicPartition) - beginningOffsets.get(topicPartition);
                finalTopicMetrics.totalRecordsInPartitions+=ret;
                return ret;
            };

            metrics.put(metricPath + "/" + "spoutLag", spoutLagGauge);
            metrics.put(metricPath + "/" + "earliestTimeOffset", earliestTimeOffsetGauge);
            metrics.put(metricPath + "/" + "latestTimeOffset", latestTimeOffsetGauge);
            metrics.put(metricPath + "/" + "latestEmittedOffset", latestEmittedOffsetGauge);
            metrics.put(metricPath + "/" + "latestCompletedOffset", latestCompletedOffsetGauge);
            metrics.put(metricPath + "/" + "recordsInPartition", recordsInPartitionGauge);

        }

        metrics.putAll(topicMetricsMap);

        return metrics;

    }

    private Map<TopicPartition, Long> getBeginningOffsets(Set<TopicPartition> topicPartitions) {
        Admin admin = adminSupplier.get();
        if (admin == null) {
            LOG.error("Kafka admin object is null, returning 0.");
            return Collections.EMPTY_MAP;
        }

        Map<TopicPartition, Long> beginningOffsets;
        try {
            beginningOffsets = getOffsets(admin, topicPartitions, OffsetSpec.earliest());
        } catch (RetriableException | ExecutionException | InterruptedException e) {
            LOG.error("Failed to get offset from Kafka for topic partitions: {}.", topicPartitions, e);
            return Collections.EMPTY_MAP;
        }
        return beginningOffsets;
    }

    private Map<TopicPartition, Long> getEndOffsets(Set<TopicPartition> topicPartitions) {
        Admin admin = adminSupplier.get();
        if (admin == null) {
            LOG.error("Kafka admin object is null, returning 0.");
            return Collections.EMPTY_MAP;
        }

        Map<TopicPartition, Long> endOffsets;
        try {
            endOffsets = getOffsets(admin, topicPartitions, OffsetSpec.latest());
        } catch (RetriableException | ExecutionException | InterruptedException e) {
            LOG.error("Failed to get offset from Kafka for topic partitions: {}.", topicPartitions, e);
            return Collections.EMPTY_MAP;
        }
        return endOffsets;
    }

    private static Map<TopicPartition, Long> getOffsets(Admin admin, Set<TopicPartition> topicPartitions, OffsetSpec offsetSpec)
            throws InterruptedException, ExecutionException {

        Map<TopicPartition, OffsetSpec> offsetSpecMap = new HashMap<>();
        for (TopicPartition topicPartition : topicPartitions) {
            offsetSpecMap.put(topicPartition, offsetSpec);
        }
        Map<TopicPartition, Long> ret = new HashMap<>();
        ListOffsetsResult listOffsetsResult = admin.listOffsets(offsetSpecMap);
        KafkaFuture<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> all = listOffsetsResult.all();
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> topicPartitionListOffsetsResultInfoMap = all.get();
        for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> entry :
                topicPartitionListOffsetsResultInfoMap.entrySet()) {
            ret.put(entry.getKey(), entry.getValue().offset());
        }
        return ret;
    }
}

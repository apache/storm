package org.apache.storm.kafka.spout;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by liurenjie on 12/7/16.
 */
public final class KafkaUtils {
    public static List<PartitionInfo> readPartitions(KafkaConsumer<?, ?> consumer, Iterable<String> topics) {
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        for (String topic : topics) {
            partitionInfos.addAll(consumer.partitionsFor(topic));
        }

        return partitionInfos;
    }
}

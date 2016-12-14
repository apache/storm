package org.apache.storm.kafka.spout;

import org.apache.kafka.common.TopicPartition;

import java.util.Comparator;

/**
 * Created by liurenjie on 12/7/16.
 */
public enum TopicPartitionComparator implements Comparator<TopicPartition> {
    INSTANCE;

    @Override
    public int compare(TopicPartition o1, TopicPartition o2) {
        if (!o1.topic().equals(o2.topic())) {
            return o1.topic().compareTo(o2.topic());
        } else {
            return o1.partition() - o2.partition();
        }
    }
}

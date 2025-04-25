/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.kafka.spout.metrics2;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Topic level metrics.
 * <p>
 * topicName/totalEarliestTimeOffset //gives the total beginning offset of all the associated partitions of this spout
 * topicName/totalLatestTimeOffset //gives the total end offset of all the associated partitions of this spout
 * topicName/totalLatestEmittedOffset //gives the total latest emitted offset of all the associated partitions of this spout
 * topicName/totalLatestCompletedOffset //gives the total latest committed offset of all the associated partitions of this spout
 * topicName/spoutLag // total spout lag of all the associated partitions of this spout
 * topicName/totalRecordsInPartitions //total number of records in all the associated partitions of this spout
 * </p>
 */
public class KafkaOffsetTopicMetrics implements MetricSet {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaOffsetTopicMetrics.class);

    private final String topic;
    long totalSpoutLag;
    long totalEarliestTimeOffset;
    long totalLatestTimeOffset;
    long totalLatestEmittedOffset;
    long totalLatestCompletedOffset;
    long totalRecordsInPartitions;


    public KafkaOffsetTopicMetrics(String topic) {
        this.topic = topic;
        this.totalSpoutLag = 0L;
        this.totalEarliestTimeOffset = 0L;
        this.totalLatestTimeOffset = 0L;
        this.totalLatestEmittedOffset = 0L;
        this.totalLatestCompletedOffset = 0L;
        this.totalRecordsInPartitions = 0L;
        LOG.info("Create KafkaOffsetTopicMetrics for topic: {}", topic);
    }

    @Override
    public Map<String, Metric> getMetrics() {
        Map<String, Metric> metrics = new HashMap();

        Gauge<Long> totalSpoutLagGauge = new Gauge<Long>() {
            @Override
            public Long getValue() {
                return totalSpoutLag;
            }
        };

        Gauge<Long> totalEarliestTimeOffsetGauge = new Gauge<Long>() {
            @Override
            public Long getValue() {
                return totalEarliestTimeOffset;
            }
        };

        Gauge<Long> totalLatestTimeOffsetGauge = new Gauge<Long>() {
            @Override
            public Long getValue() {
                return totalLatestTimeOffset;
            }
        };

        Gauge<Long> totalLatestEmittedOffsetGauge = new Gauge<Long>() {
            @Override
            public Long getValue() {
                return totalLatestEmittedOffset;
            }
        };

        Gauge<Long> totalLatestCompletedOffsetGauge = new Gauge<Long>() {
            @Override
            public Long getValue() {
                return totalLatestCompletedOffset;
            }
        };

        Gauge<Long> totalRecordsInPartitionsGauge = new Gauge<Long>() {
            @Override
            public Long getValue() {
                return totalRecordsInPartitions;
            }
        };

        metrics.put(topic + "/" + "totalSpoutLag", totalSpoutLagGauge);
        metrics.put(topic + "/" + "totalEarliestTimeOffset", totalEarliestTimeOffsetGauge);
        metrics.put(topic + "/" + "totalLatestTimeOffset", totalLatestTimeOffsetGauge);
        metrics.put(topic + "/" + "totalLatestEmittedOffset", totalLatestEmittedOffsetGauge);
        metrics.put(topic + "/" + "totalLatestCompletedOffset", totalLatestCompletedOffsetGauge);
        metrics.put(topic + "/" + "totalRecordsInPartitions", totalRecordsInPartitionsGauge);
        return metrics;
    }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.kafka.spout.metrics2;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaOffsetUtil {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaOffsetUtil.class);

    public static Map<TopicPartition, Long> getBeginningOffsets(Set<TopicPartition> topicPartitions, Supplier<Admin> adminSupplier) {
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

    public static Map<TopicPartition, Long> getEndOffsets(Set<TopicPartition> topicPartitions, Supplier<Admin> adminSupplier) {
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

    public static Map<TopicPartition, Long> getOffsets(Admin admin, Set<TopicPartition> topicPartitions, OffsetSpec offsetSpec)
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

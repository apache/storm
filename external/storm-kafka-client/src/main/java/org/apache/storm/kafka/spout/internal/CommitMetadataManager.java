/*
 * Copyright 2018 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.kafka.spout.internal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig.ProcessingGuarantee;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates and reads commit metadata.
 */
public final class CommitMetadataManager {

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(CommitMetadataManager.class);
    // Metadata information to commit to Kafka. It is unique per spout instance.
    private final String commitMetadata;
    private final ProcessingGuarantee processingGuarantee;
    private final TopologyContext context;

    /**
     * Create a manager with the given context.
     */
    public CommitMetadataManager(TopologyContext context, ProcessingGuarantee processingGuarantee) {
        this.context = context;
        try {
            commitMetadata = JSON_MAPPER.writeValueAsString(new CommitMetadata(
                context.getStormId(), context.getThisTaskId(), Thread.currentThread().getName()));
            this.processingGuarantee = processingGuarantee;
        } catch (JsonProcessingException e) {
            LOG.error("Failed to create Kafka commit metadata due to JSON serialization error", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Checks if {@link OffsetAndMetadata} was committed by a {@link KafkaSpout} instance in this topology.
     *
     * @param tp The topic partition the commit metadata belongs to.
     * @param committedOffset {@link OffsetAndMetadata} info committed to Kafka
     * @param offsetManagers The offset managers.
     * @return true if this topology committed this {@link OffsetAndMetadata}, false otherwise
     */
    public boolean isOffsetCommittedByThisTopology(TopicPartition tp, OffsetAndMetadata committedOffset,
        Map<TopicPartition, OffsetManager> offsetManagers) {
        try {
            if (processingGuarantee == ProcessingGuarantee.AT_LEAST_ONCE
                && offsetManagers.containsKey(tp)
                && offsetManagers.get(tp).hasCommitted()) {
                return true;
            }

            final CommitMetadata committedMetadata = JSON_MAPPER.readValue(committedOffset.metadata(), CommitMetadata.class);
            return committedMetadata.getTopologyId().equals(context.getStormId());
        } catch (IOException e) {
            LOG.warn("Failed to deserialize expected commit metadata [{}]."
                + " This error is expected to occur once per partition, if the last commit to each partition"
                + " was by an earlier version of the KafkaSpout, or by a process other than the KafkaSpout. "
                + "Defaulting to behavior compatible with earlier version", committedOffset);
            LOG.trace("", e);
            return false;
        }
    }

    public String getCommitMetadata() {
        return commitMetadata;
    }

}

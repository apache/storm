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

package org.apache.storm.kafka.spout.trident;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IPartitionedTridentSpout;
import org.apache.storm.trident.topology.TransactionAttempt;

public class KafkaTridentTransactionalSpoutEmitter<K, V> implements IPartitionedTridentSpout.Emitter<
        List<Map<String, Object>>,
        KafkaTridentSpoutTopicPartition,
        Map<String, Object>>,
        Serializable {

    private static final long serialVersionUID = 1;
    private final KafkaTridentSpoutEmitter<K, V> emitter;

    public KafkaTridentTransactionalSpoutEmitter(KafkaTridentSpoutEmitter<K, V> emitter) {
        this.emitter = emitter;
    }

    @Override
    public List<KafkaTridentSpoutTopicPartition> getOrderedPartitions(List<Map<String, Object>> allPartitionInfo) {
        return emitter.getOrderedPartitions(allPartitionInfo);
    }

    @Override
    public Map<String, Object> emitPartitionBatchNew(TransactionAttempt tx, TridentCollector collector,
        KafkaTridentSpoutTopicPartition partition, Map<String, Object> lastPartitionMeta) {
        return emitter.emitPartitionBatchNew(tx, collector, partition, lastPartitionMeta);
    }

    @Override
    public void refreshPartitions(List<KafkaTridentSpoutTopicPartition> partitionResponsibilities) {
        emitter.refreshPartitions(partitionResponsibilities);
    }

    @Override
    public void emitPartitionBatch(TransactionAttempt tx, TridentCollector collector,
        KafkaTridentSpoutTopicPartition partition, Map<String, Object> partitionMeta) {
        emitter.reEmitPartitionBatch(tx, collector, partition, partitionMeta);
    }

    @Override
    public void close() {
        emitter.close();
    }
    
    
    
}

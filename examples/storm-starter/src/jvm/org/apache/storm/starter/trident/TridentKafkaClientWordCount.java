/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.storm.starter.trident;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.kafka.spout.KafkaSpoutStreams;
import org.apache.storm.kafka.spout.KafkaSpoutTupleBuilder;
import org.apache.storm.kafka.spout.KafkaSpoutTuplesBuilder;
import org.apache.storm.kafka.spout.trident.KafkaManagerTridentSpout;
import org.apache.storm.kafka.spout.trident.KafkaOpaquePartitionedTridentSpout;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;

public class TridentKafkaClientWordCount extends TridentKafkaWordCount {
    public TridentKafkaClientWordCount() {
        this(null, null);
    }

    public TridentKafkaClientWordCount(String zkUrl, String brokerUrl) {
        super(zkUrl, brokerUrl);
    }

    protected TridentState addTridentState(TridentTopology tridentTopology) {
        final Stream spoutStream = tridentTopology.newStream("spout1", createOpaqueKafkaSpoutNew()).parallelismHint(1);

        return spoutStream.each(spoutStream.getOutputFields(), new Debug(true))
                .each(new Fields("str"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new DebugMemoryMapState.Factory(), new Count(), new Fields("count"));
    }

    private KafkaOpaquePartitionedTridentSpout<String, String> createOpaqueKafkaSpoutNew() {
        return new KafkaOpaquePartitionedTridentSpout<String, String>(getKafkaManager());
    }

    private KafkaManagerTridentSpout<String, String> getKafkaManager() {
        return new KafkaManagerTridentSpout<>(getKafkaSpoutConfig(getKafkaSpoutStreams()));
    }

    private KafkaSpoutConfig<String,String> getKafkaSpoutConfig(KafkaSpoutStreams kafkaSpoutStreams) {
        return new KafkaSpoutConfig.Builder<String, String>(getKafkaConsumerProps(), kafkaSpoutStreams, getTuplesBuilder(), getRetryService())
                .setOffsetCommitPeriodMs(10_000)
                .setFirstPollOffsetStrategy(EARLIEST)
                .setMaxUncommittedOffsets(250)
                .build();
    }

    private Map<String,Object> getKafkaConsumerProps() {
        Map<String, Object> props = new HashMap<>();
//        props.put(KafkaSpoutConfig.Consumer.ENABLE_AUTO_COMMIT, "true");
        props.put(KafkaSpoutConfig.Consumer.BOOTSTRAP_SERVERS, "127.0.0.1:9092");
        props.put(KafkaSpoutConfig.Consumer.GROUP_ID, "kafkaSpoutTestGroup");
        props.put(KafkaSpoutConfig.Consumer.KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(KafkaSpoutConfig.Consumer.VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    private KafkaSpoutTuplesBuilder<String, String> getTuplesBuilder() {
        return new KafkaSpoutTuplesBuilder.Builder<>(
                new TopicTestTupleBuilder<String, String>("test"))
                .build();
    }

    private static KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(getTimeInterval(500, TimeUnit.MICROSECONDS),
                KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
    }

    private static KafkaSpoutRetryExponentialBackoff.TimeInterval getTimeInterval(long delay, TimeUnit timeUnit) {
        return new KafkaSpoutRetryExponentialBackoff.TimeInterval(delay, timeUnit);
    }

    private KafkaSpoutStreams getKafkaSpoutStreams() {
        final Fields outputFields = new Fields("str");
        return new KafkaSpoutStreams.Builder(outputFields, new String[]{"test"}).build();
    }

    private class TopicTestTupleBuilder<K, V> extends KafkaSpoutTupleBuilder<K,V> {

        /**
         * @param topics list of topics that use this implementation to build tuples
         */
        public TopicTestTupleBuilder(String... topics) {
            super(topics);
        }
        @Override
        public List<Object> buildTuple(ConsumerRecord<K, V> consumerRecord) {
            return new Values(consumerRecord.value());
        }
    }

    public static void main(String[] args) throws Exception {
        String zkUrl = "localhost:2181";        // the defaults.
        String brokerUrl = "localhost:9092";

        runMain(args, new TridentKafkaClientWordCount(zkUrl, brokerUrl));
    }
}

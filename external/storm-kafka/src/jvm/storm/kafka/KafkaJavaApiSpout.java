package storm.kafka;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class KafkaJavaApiSpout extends BaseRichSpout {

    public static final Logger LOG = LoggerFactory.getLogger(KafkaJavaApiSpout.class);

    SpoutConfig _spoutConfig;
    SpoutOutputCollector _collector;
    KafkaConsumer consumer;
    ConcurrentMap<Long, Values> messages;
    long pollTimeout;
    long maxFailCount;

    public KafkaJavaApiSpout(SpoutConfig spoutConfig) {
        this._spoutConfig = spoutConfig;
    }

    @Override
    public void open(Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
        _collector = collector;

        LOG.debug("Opening KafkaJavaApiSpout...");

        if(conf.containsKey(Config.KAFKA_POLL_TIMEOUT)) {
            pollTimeout = (Long) conf.get(Config.KAFKA_POLL_TIMEOUT);
        }else{
            pollTimeout = 100;
        }
        if(conf.containsKey(Config.KAFKA_MAX_FAIL_ATTEMPTS_COUNT)){
            maxFailCount = (Long) conf.get(Config.KAFKA_MAX_FAIL_ATTEMPTS_COUNT);
        }else{
            maxFailCount = 5;
        }
        if(consumer == null){
            consumer = new KafkaConsumer(conf);
        }
        messages = new ConcurrentHashMap<Long, Values>();

        //check is multi-topic
        if(_spoutConfig.topic == null){
            consumer.subscribe(_spoutConfig.topics);
        }else{
            consumer.subscribe(Collections.singletonList(_spoutConfig.topic));
        }
    }

    @Override
    public void nextTuple() {
        LOG.debug("Polling next tuple...");
        ConsumerRecords records = consumer.poll(pollTimeout);
        for (ConsumerRecord<String, String> record : (Iterable<ConsumerRecord<String, String>>) records) {
            Values message = new Values(record.key(), record.value(), record.topic(), maxFailCount);
            messages.putIfAbsent(record.offset(), message);
            _collector.emit(message, record.offset());
        }
    }

    @Override
    public void fail(Object msgId) {
        LOG.debug("Message with offset {} failed", msgId);
        Values message = messages.get(msgId);
        Long currentAttempt = (Long) message.get(3); //get attempt (which is 3rd arg)
        if(currentAttempt < 1){
            LOG.debug("Message with offset {} reached maximum fail attempts. Skipping...", msgId);
        }else{
            message.set(3, currentAttempt-1);
            _collector.emit(message, msgId);
        }
    }

    @Override
    public void ack(Object msgId) {
        LOG.debug("Message with offset {} proceeded successfully", msgId);
        messages.remove(msgId);
    }

    @Override
    public void close() {
        consumer.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "message", "topic", "attempt"));
    }



}

package org.apache.storm.kafka.mytest;

import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * @author ：panda_z
 * @date ：Created in 2022/8/4
 * @description:
 * @version:
 */
public class MyKafkaSpout {
    public static final String MYBOOTSTRAPSERVER = "119.29.120.151:9092";
    public static final String MYTOPIC = "test";

    public KafkaSpout getKafkaSpout(){
        Map<String, Object> kafkaParams = getKafkaParams();
        KafkaSpoutConfig<String, String> kafkaConfig = KafkaSpoutConfig.builder(MYBOOTSTRAPSERVER, new String[] { MYTOPIC })
                .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST).setProp(kafkaParams).build();

        return new KafkaSpout<>(kafkaConfig);
    }

    private Map<String, Object> getKafkaParams() {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("session.timeout.ms", "30000");
        kafkaParams.put("heartbeat.interval.ms", "10000");

        return kafkaParams;
    }
}

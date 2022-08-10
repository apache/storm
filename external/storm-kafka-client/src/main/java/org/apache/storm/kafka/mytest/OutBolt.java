package org.apache.storm.kafka.mytest;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 * @author ：panda_z
 * @date ：Created in 2022/8/8
 * @description: print the msg from kafka
 * @version: 1.0
 */
public class OutBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String msg = tuple.getStringByField("value");
        System.out.println(msg);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //this bolt emit nothing.
    }
}

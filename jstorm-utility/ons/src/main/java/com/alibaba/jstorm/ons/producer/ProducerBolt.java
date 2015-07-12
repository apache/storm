package com.alibaba.jstorm.ons.producer;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.alibaba.jstorm.ons.OnsTuple;
import com.alibaba.jstorm.utils.RunCounter;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.Producer;
import com.aliyun.openservices.ons.api.SendResult;
import org.apache.log4j.Logger;

import java.util.Map;


public class ProducerBolt implements IRichBolt {

    private static final long serialVersionUID = 2495121976857546346L;
    
    private static final Logger LOG              = Logger.getLogger(ProducerBolt.class);

    protected OutputCollector      collector;
    protected ProducerConfig       producerConfig;
    protected Producer             producer;
    protected RunCounter           runCounter;
    
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        this.collector = collector;
        this.runCounter = new RunCounter(ProducerBolt.class);
        this.producerConfig = new ProducerConfig(stormConf);
        try {
			this.producer = ProducerFactory.mkInstance(producerConfig);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(e);
		}
        
    }
    
    public void execute(Tuple tuple) {
        // TODO Auto-generated method stub
        OnsTuple msgTuple = (OnsTuple)tuple.getValue(0);
        long before = System.currentTimeMillis();
        SendResult sendResult = null;
        try {
        	Message msg = new Message(
        			producerConfig.getTopic(),
        			producerConfig.getSubExpress(),
        			//Message Body
        			//任何二进制形式的数据，ONS不做任何干预，需要Producer与Consumer协商好一致的序列化和反序列化方式
        			msgTuple.getMessage().getBody());
        	
        	// 设置代表消息的业务关键属性，请尽可能全局唯一。
        	// 以方便您在无法正常收到消息情况下，可通过ONS Console查询消息并补发。
        	// 注意：不设置也不会影响消息正常收发
        	if (msgTuple.getMessage().getKey() != null) {
        		msg.setKey(msgTuple.getMessage().getKey());
        	}
        	//发送消息，只要不抛异常就是成功
        	sendResult = producer.send(msg);
        	
            LOG.info("Success send msg of " + msgTuple.getMessage().getMsgID());
        	runCounter.count(System.currentTimeMillis() - before);
        } catch (Exception e) {
        	LOG.error("Failed to send message, SendResult:" + sendResult + "\n", e);
        	runCounter.count(System.currentTimeMillis() - before);
            collector.fail(tuple);
            return ;
            //throw new FailedException(e);
        }
        
        collector.ack(tuple);
    }
    
    public void cleanup() {
        // TODO Auto-generated method stub
    	ProducerFactory.rmInstance(producerConfig.getProducerId());
    	producer = null;
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub
        
    }
    
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
}

package com.alipay.dw.jstorm.example.sequence.bolt;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.alibaba.jstorm.utils.JStormUtils;
import com.alipay.dw.jstorm.example.TpsCounter;
import com.alipay.dw.jstorm.example.sequence.bean.TradeCustomer;

public class TotalCount implements IRichBolt {
    public static Logger     LOG         = LoggerFactory.getLogger(TotalCount.class);
    
    private OutputCollector  collector;
    private TpsCounter          tpsCounter;
    private long                lastTupleId = -1;
    
    private boolean             checkTupleId = false;
    
    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        // TODO Auto-generated method stub
        this.collector = collector;
        
        tpsCounter = new TpsCounter(context.getThisComponentId() + 
                ":" + context.getThisTaskId());
        
        checkTupleId = JStormUtils.parseBoolean(stormConf.get("bolt.check.tupleId"), false);
        
        LOG.info("Finished preparation");
    }
    
    private AtomicLong tradeSum    = new AtomicLong(0);
    private AtomicLong customerSum = new AtomicLong(1);
    
    @Override
    public void execute(Tuple input) {
    	
    	if (checkTupleId) {
    		Long tupleId = input.getLong(0);
            if (tupleId <= lastTupleId) {
            	LOG.error("LastTupleId is " + lastTupleId + ", but now:" + tupleId);
            }
            lastTupleId = tupleId;
    	}

        TradeCustomer tradeCustomer = (TradeCustomer) input.getValue(1);
        
        tradeSum.addAndGet(tradeCustomer.getTrade().getValue());
        customerSum.addAndGet(tradeCustomer.getCustomer().getValue());
        
        collector.ack(input);
        
        long now = System.currentTimeMillis();
        long spend = now - tradeCustomer.getTimestamp();
        
        tpsCounter.count(spend);
        
//    	  long spend = System.currentTimeMillis() - input.getLong(0);
//    	  tpsCounter.count(spend);
    }
    
    public void cleanup() {
        tpsCounter.cleanup();
        LOG.info("tradeSum:" + tradeSum + ",cumsterSum" + customerSum);
        LOG.info("Finish cleanup");
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        
    }
    
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
    
}

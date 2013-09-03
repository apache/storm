package com.alipay.dw.jstorm.example.sequence.bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.alipay.dw.jstorm.example.TpsCounter;
import com.alipay.dw.jstorm.example.sequence.SequenceTopologyDef;
import com.alipay.dw.jstorm.example.sequence.bean.Pair;

public class MergeRecord implements IRichBolt {
    /**
     * 
     */
    private static final long serialVersionUID = -5984311734042330577L;

    public static Logger     LOG         = Logger.getLogger(MergeRecord.class);
    
    private Map<Long, Tuple> tradeMap    = new HashMap<Long, Tuple>();
    private Map<Long, Tuple> customerMap = new HashMap<Long, Tuple>();
    
    private TpsCounter          tpsCounter;
    
    private OutputCollector  collector;
    
    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        // TODO Auto-generated method stub
        this.collector = collector;
        
        tpsCounter = new TpsCounter(context.getThisComponentId() + 
                ":" + context.getThisTaskId());
        
        LOG.info("Finished preparation");
    }
    
    private AtomicLong tradeSum    = new AtomicLong(0);
    private AtomicLong customerSum = new AtomicLong(0);
    
    @Override
    public void execute(Tuple input) {
        
        tpsCounter.count();
        
        Long tupleId = input.getLong(0);
        Pair pair = (Pair) input.getValue(1);
        
        Pair trade = null;
        Pair customer = null;
        
        Tuple tradeTuple = null;
        Tuple customerTuple = null;
        
        if (input.getSourceComponent().equals(SequenceTopologyDef.CUSTOMER_BOLT_NAME) ) {
            customer = pair;
            customerTuple = input;
            
            tradeTuple = tradeMap.get(tupleId);
            if (tradeTuple == null) {
                customerMap.put(tupleId, input);
                return;
            }
            
            trade = (Pair) tradeTuple.getValue(1);
            
        } else if (input.getSourceComponent().equals(SequenceTopologyDef.TRADE_BOLT_NAME)) {
            trade = pair;
            tradeTuple = input;
            
            customerTuple = customerMap.get(tupleId);
            if (customerTuple == null) {
                tradeMap.put(tupleId, input);
                return;
            }
            
            customer = (Pair) customerTuple.getValue(1);
        } else {
            LOG.info("Unknow source component");
            collector.fail(input);
            return;
        }
        
        tradeSum.addAndGet(trade.getValue());
        customerSum.addAndGet(customer.getValue());
        
        collector.ack(tradeTuple);
        collector.ack(customerTuple);
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

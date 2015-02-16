package com.alipay.dw.jstorm.example.sequence.bolt;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.TupleHelpers;

import com.alibaba.jstorm.client.metric.MetricCallback;
import com.alibaba.jstorm.client.metric.MetricClient;
import com.alibaba.jstorm.metric.JStormHistogram;
import com.alibaba.jstorm.metric.JStormTimer;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alipay.dw.jstorm.example.TpsCounter;
import com.alipay.dw.jstorm.example.sequence.bean.TradeCustomer;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;

public class TotalCount implements IRichBolt {
    public static Logger     LOG         = LoggerFactory.getLogger(TotalCount.class);
    
    private OutputCollector  collector;
    private TpsCounter          tpsCounter;
    private long                lastTupleId = -1;
    
    private boolean             checkTupleId = false;
    private boolean             slowDonw = false;
    
    private MetricClient       metricClient;
    private Gauge<Integer>      myGauge;
    private JStormTimer         myTimer;
    private Counter             myCounter;
    private Meter               myMeter;
    private JStormHistogram     myJStormHistogram;
    private MetricCallback      myCallback;
    
    
    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        // TODO Auto-generated method stub
        this.collector = collector;
        
        tpsCounter = new TpsCounter(context.getThisComponentId() + 
                ":" + context.getThisTaskId());
        
        checkTupleId = JStormUtils.parseBoolean(stormConf.get("bolt.check.tupleId"), false);
        
        slowDonw = JStormUtils.parseBoolean(stormConf.get("bolt.slow.down"), false);
        
        metricClient = new MetricClient(context);
        myCallback = new MetricCallback<Metric>() {

			@Override
			public void callback(Metric metric) {
				LOG.info("Callback " + metric.getClass().getName() + ":" + metric);
			}
		};
		
        
        myGauge = new Gauge<Integer>() {
        	private Random random = new Random();

			@Override
			public Integer getValue() {
				
				return random.nextInt(100);
			}
        	
		};
		myGauge = (Gauge<Integer>) metricClient.registerGauge("name1", myGauge, myCallback);
		
		myTimer = metricClient.registerTimer("name2", myCallback);
		
		myCounter = metricClient.registerCounter("name3", myCallback);
		
		myMeter = metricClient.registerMeter("name4", myCallback);
		
		myJStormHistogram = metricClient.registerHistogram("name5", myCallback);
		
		
		
        
        LOG.info("Finished preparation " + stormConf);
    }
    
    private AtomicLong tradeSum    = new AtomicLong(0);
    private AtomicLong customerSum = new AtomicLong(1);
    
    @Override
    public void execute(Tuple input) {
    	if (TupleHelpers.isTickTuple(input) ){
    		LOG.info("Receive one Ticket Tuple " + input.getSourceComponent());
    		return ;
    	}
    	long before = System.currentTimeMillis();
    	myTimer.start();
    	try {
    		//LOG.info(input.toString());
	    	myCounter.inc();
	    	myMeter.mark();
	    	
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
	        
	        if (slowDonw) {
	        	JStormUtils.sleepMs(20);
	        }
    	}finally {
    		myTimer.stop();
    	}
        long after = System.currentTimeMillis();
        myJStormHistogram.update(after - before);
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

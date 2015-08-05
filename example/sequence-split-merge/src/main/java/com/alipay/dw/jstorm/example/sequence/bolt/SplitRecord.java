/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.dw.jstorm.example.sequence.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.alipay.dw.jstorm.example.TpsCounter;
import com.alipay.dw.jstorm.example.sequence.SequenceTopologyDef;
import com.alipay.dw.jstorm.example.sequence.bean.Pair;
import com.alipay.dw.jstorm.example.sequence.bean.TradeCustomer;


public class SplitRecord implements IBasicBolt {
	
	public static Logger LOG = LoggerFactory.getLogger(SplitRecord.class);
	
	private TpsCounter          tpsCounter;

	public void prepare(Map conf, TopologyContext context) {
	    
	    tpsCounter = new TpsCounter(context.getThisComponentId() + 
                ":" + context.getThisTaskId());
	    
		LOG.info("Successfully do prepare");
		
		
	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
	    tpsCounter.count();
	    
	    Long tupleId = tuple.getLong(0);
	    Object obj = tuple.getValue(1);
	    
	    if (obj instanceof TradeCustomer) {
	    
    	    TradeCustomer tradeCustomer = (TradeCustomer)obj;
    	    
    	    Pair trade = tradeCustomer.getTrade();
    	    Pair customer = tradeCustomer.getCustomer();
            
            collector.emit(SequenceTopologyDef.TRADE_STREAM_ID, 
                    new Values(tupleId, trade));
            
            collector.emit(SequenceTopologyDef.CUSTOMER_STREAM_ID, 
                    new Values(tupleId, customer));
	    }else if (obj != null){
	        LOG.info("Unknow type " + obj.getClass().getName());
	    }else {
	        LOG.info("Nullpointer " );
	    }
		
	}
	
	

	public void cleanup() {
	    
	    tpsCounter.cleanup();
        LOG.info("Finish cleanup");

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(SequenceTopologyDef.TRADE_STREAM_ID, new Fields("ID", "TRADE"));
		declarer.declareStream(SequenceTopologyDef.CUSTOMER_STREAM_ID, new Fields("ID", "CUSTOMER"));
	}

	public Map<String, Object> getComponentConfiguration() {
	    // TODO Auto-generated method stub
	    return null;
	}
}
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
package com.alipay.dw.jstorm.example.sequence.spout;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alipay.dw.jstorm.example.TpsCounter;
import com.alipay.dw.jstorm.example.sequence.bean.Pair;
import com.alipay.dw.jstorm.example.sequence.bean.PairMaker;
import com.alipay.dw.jstorm.example.sequence.bean.TradeCustomer;

public class SequenceSpout implements IRichSpout {

	/**
     * 
     */
	private static final long serialVersionUID = 1L;

	public static final Logger LOG = LoggerFactory
			.getLogger(SequenceSpout.class);

	public static final String STREAM_ID = "extra_stream_id";

	SpoutOutputCollector collector;

	// I special use long not AtomicLong to check competition
	private long tupleId;
	private long succeedCount;
	private long failedCount;

	private AtomicLong handleCounter = new AtomicLong(0);

	private Long MAX_PENDING_COUNTER;

	private TpsCounter tpsCounter;

	private boolean isFinished;

	private boolean isLimited = false;

	private long SPOUT_MAX_SEND_NUM;
	
	private int bufferLen = 0;
	private Random random;

	public boolean isDistributed() {
		return true;
	}

	public void SequenceSpout() {

	}

	public long getMaxPending(Map conf) {
		// if single spout thread, MAX_PENDING should be Long.MAX_VALUE
		if (ConfigExtension.isSpoutSingleThread(conf)) {
			return Long.MAX_VALUE;
		}

		Object pending = conf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING);
		if (pending == null) {
			return Long.MAX_VALUE;
		}

		int pendingNum = JStormUtils.parseInt(pending);
		if (pendingNum == 1) {
			return Long.MAX_VALUE;
		}

		return pendingNum;
	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;

		if (conf.get("spout.max.sending.num") == null) {
			isLimited = false;
		} else {
			isLimited = true;
			SPOUT_MAX_SEND_NUM = JStormUtils.parseLong(conf
					.get("spout.max.sending.num"));
		}

		isFinished = false;

		tpsCounter = new TpsCounter(context.getThisComponentId() + ":"
				+ context.getThisTaskId());

		MAX_PENDING_COUNTER = getMaxPending(conf);
		
		bufferLen = JStormUtils.parseInt(conf.get("byte.buffer.len"), 0);
		
		random = new Random();
		random.setSeed(System.currentTimeMillis());
		
		LOG.info("Finish open, buffer Len:"  + bufferLen);

	}

	private AtomicLong tradeSum = new AtomicLong(0);
	private AtomicLong customerSum = new AtomicLong(0);

	public void emit() {
		
		String buffer = null;
		if (bufferLen > 0) {
			byte[] byteBuffer = new byte[bufferLen];
			
			for (int i = 0; i < bufferLen; i++) {
				byteBuffer[i] = (byte)random.nextInt(200);
			}
			
			buffer = new String(byteBuffer);
		}
		

		Pair trade = PairMaker.makeTradeInstance();
		Pair customer = PairMaker.makeCustomerInstance();

		TradeCustomer tradeCustomer = new TradeCustomer();
		tradeCustomer.setTrade(trade);
		tradeCustomer.setCustomer(customer);
		tradeCustomer.setBuffer(buffer);

		tradeSum.addAndGet(trade.getValue());
		customerSum.addAndGet(customer.getValue());

		collector.emit(new Values(tupleId, tradeCustomer),
				Long.valueOf(tupleId));

		tupleId++;

		handleCounter.incrementAndGet();

		while (handleCounter.get() >= MAX_PENDING_COUNTER - 1) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
			}
		}

		tpsCounter.count();

	}

	public void nextTuple() {
		if (isLimited == false) {
			emit();

			return;
		}

		if (isFinished == true) {
			LOG.info("Finish sending ");
			JStormUtils.sleepMs(10000);
			return;
		}

		if (tupleId > SPOUT_MAX_SEND_NUM) {
			isFinished = true;
			return;
		}

		emit();

	}

	public void close() {

		tpsCounter.cleanup();
		LOG.info("Sending :" + tupleId + ", success:" + succeedCount
				+ ", failed:" + failedCount);
		LOG.info("tradeSum:" + tradeSum + ",cumsterSum" + customerSum);
	}

	public void ack(Object id) {
		Long tupleId = (Long) id;

		// if (tupleId != this.tupleId - 1) {
		// LOG.error("Not sync, current :" + this.tupleId + ",ack:" + tupleId);
		// }

		succeedCount++;

		handleCounter.decrementAndGet();

		return;
	}

	public void fail(Object id) {

		failedCount++;
		handleCounter.decrementAndGet();
		Long failId = (Long) id;
		LOG.info("Failed to handle " + failId);

		return;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("ID", "RECORD"));
		// declarer.declare(new Fields("ID"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		LOG.info("Start active");
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		LOG.info("Start deactive");

	}

}
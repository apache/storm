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
package com.alipay.dw.jstorm.example.batch;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.TimeCacheMap;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.batch.BatchId;
import com.alibaba.jstorm.batch.ICommitter;
import com.alibaba.jstorm.utils.JStormUtils;

public class SimpleBolt implements IBasicBolt, ICommitter {

	private static final long serialVersionUID = 5720810158625748042L;

	private static final Logger LOG = Logger.getLogger(SimpleBolt.class);

	public static final String COUNT_BOLT_NAME = "Count";
	public static final String SUM_BOLT_NAME = "Sum";

	private Map conf;

	private TimeCacheMap<BatchId, AtomicLong> counters;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.conf = stormConf;

		int timeoutSeconds = JStormUtils.parseInt(
				conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS), 30);
		counters = new TimeCacheMap<BatchId, AtomicLong>(timeoutSeconds);
		
		LOG.info("Successfully do prepare");

	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		BatchId id = (BatchId) input.getValue(0);
		Long value = input.getLong(1);

		AtomicLong counter = counters.get(id);
		if (counter == null) {
			counter = new AtomicLong(0);
			counters.put(id, counter);
		}

		counter.addAndGet(value);

	}

	@Override
	public void cleanup() {
		LOG.info("Successfully do cleanup");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("BatchId", "counters"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	private BatchId currentId;
	@Override
	public byte[] commit(BatchId id) throws FailedException {
		LOG.info("Receive BatchId " + id);
		if (currentId == null) {
			currentId = id;
		}else if (currentId.getId() >= id.getId()) {
			LOG.info("Current BatchId is " + currentId + ", receive:" 
					+ id);
			throw new RuntimeException();
		}
		currentId = id;
		
		AtomicLong counter = (AtomicLong) counters.remove(id);
		if (counter == null) {
			counter = new AtomicLong(0);
		}

		LOG.info("Flush " + id + "," + counter);
		return Utils.serialize(id);
	}


	@Override
	public void revert(BatchId id, byte[] commitResult) {
		LOG.info("Receive BatchId " + id);
		
		BatchId failedId = (BatchId)Utils.javaDeserialize(commitResult);
		
		if (failedId.equals(id) == false) {
			LOG.info("Deserialized error  " + id);
		}
	}
	
}

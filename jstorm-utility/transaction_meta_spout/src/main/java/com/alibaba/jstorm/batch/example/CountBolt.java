package com.alibaba.jstorm.batch.example;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.TimeCacheMap;

import com.alibaba.jstorm.batch.BatchId;
import com.alibaba.jstorm.batch.IPrepareCommit;
import com.alibaba.jstorm.utils.JStormUtils;

public class CountBolt implements IBasicBolt, IPrepareCommit {
	private static final long serialVersionUID = 5720810158625748042L;

	private static final Logger LOG = Logger.getLogger(CountBolt.class);

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

		AtomicLong counter = counters.get(id);
		if (counter == null) {
			counter = new AtomicLong(0);
			counters.put(id, counter);
		}

		counter.incrementAndGet();

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

	@Override
	public void prepareCommit(BatchId id, BasicOutputCollector collector) {
		AtomicLong counter = (AtomicLong) counters.remove(id);
		if (counter == null) {
			counter = new AtomicLong(0);
		}

		collector.emit(new Values(id, counter.get()));
	}
}

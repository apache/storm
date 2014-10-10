package com.alibaba.jstorm.batch.example;

import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.alibaba.jstorm.batch.BatchId;

public class TransformBolt implements IBasicBolt {
	private static final long serialVersionUID = 5720810158625748044L;

	private static final Logger LOG = Logger.getLogger(TransformBolt.class);

	public static final String BOLT_NAME = TransformBolt.class.getSimpleName();

	private Map conf;

	private Random rand;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.conf = stormConf;
		this.rand = new Random();
		rand.setSeed(System.currentTimeMillis());
		
		LOG.info("Successfully do prepare");
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		BatchId id = (BatchId) input.getValue(0);

		long value = rand.nextInt(100);

		collector.emit(new Values(id, value));

	}

	@Override
	public void cleanup() {
		LOG.info("Successfully do cleanup");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("BatchId", "Value"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}

package com.alibaba.jstorm.batch.example;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.alibaba.jstorm.batch.BatchId;
import com.alibaba.jstorm.batch.IPostCommit;
import com.alibaba.jstorm.utils.JStormUtils;

public class BatchMetaRebalance implements IBasicBolt, IPostCommit {
	/**  */
	private static final long serialVersionUID = 2991323223385556163L;

	private static final Logger LOG = Logger
			.getLogger(BatchMetaRebalance.class);

	public static final String BOLT_NAME = BatchMetaRebalance.class
			.getSimpleName();

	public static final String REBALANCE_STREAM_ID = "rebalance";

	private transient AtomicBoolean isNeedRebalance;
	private transient ScheduledExecutorService scheduExec;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		isNeedRebalance = new AtomicBoolean(false);
		CheckRebalanceTimer timer = new CheckRebalanceTimer();
		
		int rebalanceTimeInterval = JStormUtils.parseInt(
				stormConf.get("meta.rebalance.frequency.sec"), 3600);
		
		long now = System.currentTimeMillis();
		long next = (now/(rebalanceTimeInterval * 1000) + 1) * rebalanceTimeInterval * 1000;
		long diff = (next - now )/1000;

//		Calendar start = Calendar.getInstance();
//
//		start.add(Calendar.HOUR_OF_DAY, 1);
//
//		start.set(Calendar.MINUTE, 30);
//		start.set(Calendar.SECOND, 0);
//		start.set(Calendar.MILLISECOND, 0);
//		long startMillis = start.getTimeInMillis();
//
//		long now = System.currentTimeMillis();
//
//		long diff = (startMillis - now) / (1000);

		ScheduledExecutorService scheduExec = null;

		scheduExec = Executors.newSingleThreadScheduledExecutor();
		scheduExec.scheduleAtFixedRate(timer, diff, rebalanceTimeInterval, 
				TimeUnit.SECONDS);

		LOG.info("Successfully init rebalance timer");
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		LOG.warn("Receive unkonw message");
	}

	@Override
	public void cleanup() {
		scheduExec.shutdown();
		LOG.info("Successfully do cleanup");
	}

	@Override
	public void postCommit(BatchId id, BasicOutputCollector collector) {
		if (isNeedRebalance.get() == true) {
			isNeedRebalance.set(false);
			collector.emit(REBALANCE_STREAM_ID, new Values(id));
			LOG.info("Emit rebalance command");
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(REBALANCE_STREAM_ID, new Fields("BatchId"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public class CheckRebalanceTimer implements Runnable {
		public void run() {
			BatchMetaRebalance.this.isNeedRebalance.set(true);
		}
	}

}

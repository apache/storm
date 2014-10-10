package com.alibaba.jstorm.batch.example;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.TimeCacheMap;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.batch.BatchId;
import com.alibaba.jstorm.batch.ICommitter;
import com.alibaba.jstorm.utils.JStormUtils;

public class DBBolt implements IBasicBolt, ICommitter, Runnable {
	private static final long serialVersionUID = 5720810158625748043L;

	private static final Logger LOG = Logger.getLogger(DBBolt.class);

	public static final String BOLT_NAME = DBBolt.class.getSimpleName();

	private TimeCacheMap<BatchId, AtomicLong> counters;
	private TimeCacheMap<BatchId, AtomicLong> sums;

	private Map conf;

	private LinkedBlockingQueue<CommitedValue> revertQueue;
	private Thread revertThread;
	private AtomicBoolean isRunRevert;

	public void initRevert() {
		revertQueue = new LinkedBlockingQueue<DBBolt.CommitedValue>();
		isRunRevert = new AtomicBoolean(false);
		revertThread = new Thread(this);
		revertThread.start();

		LOG.info("Successfully init revert thread");

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.conf = stormConf;

		int timeoutSeconds = JStormUtils.parseInt(
				conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS), 30);

		counters = new TimeCacheMap<BatchId, AtomicLong>(timeoutSeconds);
		sums = new TimeCacheMap<BatchId, AtomicLong>(timeoutSeconds);

		initRevert();

		LOG.info("Successfully do prepare");

	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		BatchId batchId = (BatchId) input.getValue(0);

		if (input.getSourceComponent().equals(CountBolt.COUNT_BOLT_NAME)) {
			AtomicLong counter = counters.get(batchId);
			if (counter == null) {
				counter = new AtomicLong(0);
				counters.put(batchId, counter);
			}
			long value = input.getLong(1);

			counter.addAndGet(value);

		} else if (input.getSourceComponent().equals(CountBolt.SUM_BOLT_NAME)) {
			AtomicLong sum = sums.get(batchId);
			if (sum == null) {
				sum = new AtomicLong(0);
				sums.put(batchId, sum);
			}
			long value = input.getLong(1);

			sum.addAndGet(value);
		} else {
			LOG.warn("Unknow source type");
		}
	}

	@Override
	public void cleanup() {
		LOG.info("Begin to exit ");
		isRunRevert.set(false);
		try {
			revertThread.join();
		} catch (InterruptedException e) {

		}

		LOG.info("Successfully exit ");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public byte[] commit(BatchId id) throws FailedException {
		AtomicLong count = (AtomicLong) counters.remove(id);
		if (count == null) {
			count = new AtomicLong(0);
		}

		AtomicLong sum = (AtomicLong) sums.remove(id);
		if (sum == null) {
			sum = new AtomicLong(0);
		}

		CommitedValue commitedValue = new CommitedValue(count, sum);

		try {

			commitedValue.commit();

			return Utils.serialize(commitedValue);
		} catch (Exception e) {
			LOG.error("Failed to commit " + commitedValue, e);
			throw new FailedException(e);
		}

	}

	@Override
	public void revert(BatchId id, byte[] commitResult) {
		CommitedValue commitedValue = (CommitedValue) Utils
				.deserialize(commitResult);
		if (commitedValue == null) {
			LOG.warn("Failed to deserialize commited value");
			return;
		}

		revertQueue.offer(commitedValue);
	}

	@Override
	public void run() {
		LOG.info("Begin to run revert thread");
		isRunRevert.set(true);

		while (isRunRevert.get() == true) {
			CommitedValue commitedValue = null;
			try {
				commitedValue = revertQueue.take();
			} catch (InterruptedException e1) {
			}
			if (commitedValue == null) {
				continue;
			}

			try {
				commitedValue.revert();
				LOG.info("Successfully revert " + commitedValue);
			} catch (Exception e) {
				LOG.info("Failed to do revert " + commitedValue);
				JStormUtils.sleepMs(100);
				revertQueue.offer(commitedValue);
			}
		}

		LOG.info("Successfully quit revert thread");
	}

	public static class CommitedValue implements Serializable {
		private static final long serialVersionUID = 5720810158625748047L;

		private final AtomicLong counter;
		private final AtomicLong sum;

		private static AtomicLong dbCounter = new AtomicLong(0);
		private static AtomicLong dbSum = new AtomicLong(0);

		public CommitedValue(AtomicLong counter, AtomicLong sum) {
			this.counter = counter;
			this.sum = sum;
		}

		public void commit() throws Exception {
			boolean doCount = false;
			boolean doSum = false;
			try {
				long countValue = dbCounter.addAndGet(counter.get());
				doCount = true;
				long sumValue = dbSum.addAndGet(sum.get());
				doSum = true;

				LOG.info("Successfully commit " + this);
				LOG.info("DB counter:" + countValue + ", sum:" + sumValue);
			} catch (Exception e) {
				if (doSum) {
					dbSum.addAndGet(-sum.get());
					LOG.info("Revert sum, " + sum.get());
				}

				if (doCount) {
					dbCounter.addAndGet(-counter.get());
					LOG.info("Revert count," + counter.get());
				}

				LOG.warn(e.getCause(), e);
				throw e;
			}

		}

		public void revert() throws Exception {
			boolean doCount = false;
			boolean doSum = false;
			try {
				long countValue = dbCounter.addAndGet(-counter.get());
				doCount = true;
				long sumValue = dbSum.addAndGet(-sum.get());
				doSum = true;

				LOG.info("Successfully commit " + this);
				LOG.info("DB counter:" + countValue + ", sum:" + sumValue);
			} catch (Exception e) {
				if (doSum) {
					dbSum.addAndGet(sum.get());
					LOG.info("Revert sum, " + sum.get());
				}

				if (doCount) {
					dbCounter.addAndGet(counter.get());
					LOG.info("Revert count," + counter.get());
				}

				LOG.warn(e.getCause(), e);
				throw e;
			}
		}

		@Override
		public String toString() {
			return ToStringBuilder.reflectionToString(this,
					ToStringStyle.SHORT_PREFIX_STYLE);
		}
	}

}

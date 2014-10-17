package com.alibaba.aloha.meta;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.alibaba.jstorm.client.metric.MetricClient;
import com.alibaba.jstorm.client.spout.IAckValueSpout;
import com.alibaba.jstorm.client.spout.IFailValueSpout;
import com.alibaba.jstorm.metric.JStormHistogram;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.taobao.metaq.client.MetaPushConsumer;

public class MetaSpout implements IRichSpout, IAckValueSpout, IFailValueSpout,
		MessageListenerConcurrently {
	/**  */
	private static final long serialVersionUID = 8476906628618859716L;
	private static final Logger LOG = Logger.getLogger(MetaSpout.class);

	protected MetaClientConfig metaClientConfig;
	protected SpoutOutputCollector collector;
	protected transient MetaPushConsumer consumer;

	protected Map conf;
	protected String id;
	protected boolean flowControl;
	protected boolean autoAck;

	protected transient LinkedBlockingDeque<MetaTuple> sendingQueue;

	protected transient MetricClient metricClient;
	protected transient JStormHistogram waithHistogram;
	protected transient JStormHistogram processHistogram;

	public MetaSpout() {

	}

	public void initMetricClient(TopologyContext context) {
		metricClient = new MetricClient(context);
		waithHistogram = metricClient.registerHistogram("MetaTupleWait", null);
		processHistogram = metricClient.registerHistogram("MetaTupleProcess",
				null);
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.conf = conf;
		this.collector = collector;
		this.id = context.getThisComponentId() + ":" + context.getThisTaskId();
		this.sendingQueue = new LinkedBlockingDeque<MetaTuple>();

		this.flowControl = JStormUtils.parseBoolean(
				conf.get(MetaClientConfig.META_SPOUT_FLOW_CONTROL), true);
		this.autoAck = JStormUtils.parseBoolean(
				conf.get(MetaClientConfig.META_SPOUT_AUTO_ACK), false);
		
		StringBuilder sb = new StringBuilder();
		sb.append("Begin to init MetaSpout:").append(id);
		sb.append(", flowControl:").append(flowControl);
		sb.append(", autoAck:").append(autoAck);
		LOG.info( sb.toString());

		initMetricClient(context);

		metaClientConfig = MetaClientConfig.mkInstance(conf);

		try {
			consumer = MetaConsumerFactory.mkInstance(metaClientConfig, this);
		} catch (Exception e) {
			LOG.error("Failed to create Meta Consumer ", e);
			throw new RuntimeException("Failed to create MetaConsumer" + id, e);
		}

		if (consumer == null) {
			LOG.warn(id
					+ " already exist consumer in current worker, don't need to fetch data ");

			new Thread(new Runnable() {

				@Override
				public void run() {
					while (true) {
						try {
							Thread.sleep(10000);
						} catch (InterruptedException e) {
							break;
						}

						StringBuilder sb = new StringBuilder();
						sb.append("Only on meta consumer can be run on one process,");
						sb.append(" but there are mutliple spout consumes with the same topic@groupid meta, so the second one ");
						sb.append(id).append(" do nothing ");
						LOG.info(sb.toString());
					}
				}
			}).start();
		}
		
		LOG.info("Successfully init " + id);
	}

	@Override
	public void close() {
		if (consumer != null) {
			consumer.shutdown();
		}

	}

	@Override
	public void activate() {
		if (consumer != null) {
			consumer.resume();
		}

	}

	@Override
	public void deactivate() {
		if (consumer != null) {
			consumer.suspend();
		}
	}

	public void sendTuple(MetaTuple metaTuple) {
		metaTuple.updateEmitMs();
		collector.emit(new Values(metaTuple), metaTuple.getCreateMs());
	}

	@Override
	public void nextTuple() {
		MetaTuple metaTuple = null;
		try {
			metaTuple = sendingQueue.take();
		} catch (InterruptedException e) {
		}

		if (metaTuple == null) {
			return;
		}

		sendTuple(metaTuple);

	}

	@Deprecated
	public void ack(Object msgId) {
		LOG.warn("Shouldn't go this function");
	}

	@Deprecated
	public void fail(Object msgId) {
		LOG.warn("Shouldn't go this function");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("MetaTuple"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void fail(Object msgId, List<Object> values) {
		MetaTuple metaTuple = (MetaTuple) values.get(0);
		AtomicInteger failTimes = metaTuple.getFailureTimes();

		int failNum = failTimes.incrementAndGet();
		if (failNum > metaClientConfig.getMaxFailTimes()) {
			LOG.warn("Message " + metaTuple.getMq() + " fail times " + failNum);
			finishTuple(metaTuple);
			return;
		}

		if (flowControl) {
			sendingQueue.offer(metaTuple);
		} else {
			sendTuple(metaTuple);
		}
	}

	public void finishTuple(MetaTuple metaTuple) {
		waithHistogram.update(metaTuple.getEmitMs() - metaTuple.getCreateMs());
		processHistogram.update(System.currentTimeMillis() - metaTuple.getEmitMs());
		metaTuple.done();
	}

	@Override
	public void ack(Object msgId, List<Object> values) {
		MetaTuple metaTuple = (MetaTuple) values.get(0);
		finishTuple(metaTuple);
	}

	@Override
	public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
			ConsumeConcurrentlyContext context) {
		try {
			MetaTuple metaTuple = new MetaTuple(msgs, context.getMessageQueue());

			if (flowControl) {
				sendingQueue.offer(metaTuple);
			} else {
				sendTuple(metaTuple);
			}

			if (autoAck) {
				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
			} else {
				metaTuple.waitFinish();
				if (metaTuple.isSuccess() == true) {
					return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
				} else {
					return ConsumeConcurrentlyStatus.RECONSUME_LATER;
				}
			}

		} catch (Exception e) {
			LOG.error("Failed to emit " + id, e);
			return ConsumeConcurrentlyStatus.RECONSUME_LATER;
		}

	}

	public MetaPushConsumer getConsumer() {
		return consumer;
	}

}

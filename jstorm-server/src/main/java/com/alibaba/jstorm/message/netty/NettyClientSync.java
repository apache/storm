package com.alibaba.jstorm.message.netty;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.metric.Metrics;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.codahale.metrics.Gauge;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

class NettyClientSync extends NettyClient implements EventHandler {
	private static final Logger LOG = LoggerFactory
			.getLogger(NettyClientSync.class);

	private ConcurrentLinkedQueue<MessageBatch> batchQueue;
	private DisruptorQueue disruptorQueue;
	private ExecutorService bossExecutor;
	private ExecutorService workerExecutor;

	private AtomicLong emitTs = new AtomicLong(0);

	@SuppressWarnings("rawtypes")
	NettyClientSync(Map storm_conf, ChannelFactory factory,
			ScheduledExecutorService scheduler, String host, int port,
			ReconnectRunnable reconnector) {
		super(storm_conf, factory, scheduler, host, port, reconnector);
		
		batchQueue = new ConcurrentLinkedQueue<MessageBatch>();
		Metrics.register(address, MetricDef.NETTY_CLI_SYNC_BATCH_QUEUE, 
				new Gauge<Integer>() {
			        @Override
			        public Integer getValue() {
				        return batchQueue.size();
			        }
		        }, null, Metrics.MetricType.WORKER);

		WaitStrategy waitStrategy = (WaitStrategy) Utils
				.newInstance((String) storm_conf
						.get(Config.TOPOLOGY_DISRUPTOR_WAIT_STRATEGY));

		disruptorQueue = DisruptorQueue.mkInstance(name, ProducerType.MULTI,
				MAX_SEND_PENDING * 8, waitStrategy);
		disruptorQueue.consumerStarted();

		Metrics.registerQueue(address, MetricDef.NETTY_CLI_SYNC_DISR_QUEUE, disruptorQueue, 
				null, Metrics.MetricType.WORKER);

		Runnable trigger = new Runnable() {
			@Override
			public void run() {
				trigger();
			}
		};

		scheduler.scheduleWithFixedDelay(trigger, 10, 1, TimeUnit.SECONDS);

		/**
		 * In sync mode, it can't directly use common factory,
		 * it will occur problem when client close and restart
		 */
		ThreadFactory bossFactory = new NettyRenameThreadFactory(
				PREFIX + JStormServerUtils.getName(host, port) + "-boss");
		bossExecutor = Executors.newCachedThreadPool(bossFactory);
		ThreadFactory workerFactory = new NettyRenameThreadFactory(
				PREFIX + JStormServerUtils.getName(host, port) + "-worker");
		workerExecutor = Executors.newCachedThreadPool(workerFactory);
		
		clientChannelFactory = new NioClientSocketChannelFactory(
				bossExecutor,
				workerExecutor, 1);

		start();

		LOG.info(this.toString());
	}

	/**
	 * Enqueue a task message to be sent to server
	 */
	@Override
	public void send(List<TaskMessage> messages) {
		for (TaskMessage msg : messages) {
			disruptorQueue.publish(msg);
		}
	}

	@Override
	public void send(TaskMessage message) {
		disruptorQueue.publish(message);
	}

	public void flushBatch(MessageBatch batch, Channel channel) {
		emitTs.set(System.currentTimeMillis());
		if (batch == null) {
			LOG.warn("Handle no data to {}, this shouldn't occur", name);

		} else if (channel == null || channel.isWritable() == false) {
			LOG.warn("Channel occur exception, during batch messages {}", name);
			batchQueue.offer(batch);
		} else {

			flushRequest(channel, batch);
		}
	}

	/**
	 * Don't take care of competition
	 * 
	 * @param blocked
	 */
	public void sendData() {
		sendTimer.start();
		try {
			MessageBatch batch = batchQueue.poll();
			if (batch == null) {

				disruptorQueue.consumeBatchWhenAvailable(this);

				batch = batchQueue.poll();
			}

			Channel channel = channelRef.get();
			flushBatch(batch, channel);
		} catch (Throwable e) {
			LOG.error("Occur e", e);
			String err = name + " nettyclient occur unknow exception";
			JStormUtils.halt_process(-1, err);
		} finally {
			sendTimer.stop();
		}
	}

	public void sendAllData() {

		sendTimer.start();
		try {
			disruptorQueue.consumeBatch(this);
			MessageBatch batch = batchQueue.poll();
			while (batch != null) {
				Channel channel = channelRef.get();
				if (channel == null) {
					LOG.info("No channel {} to flush all data", name);
					return;
				} else if (channel.isWritable() == false) {
					LOG.info("Channel {} is no writable", name);
					return;
				}
				flushBatch(batch, channel);
				batch = batchQueue.poll();
			}
		} catch (Throwable e) {
			LOG.error("Occur e", e);
			String err = name + " nettyclient occur unknow exception";
			JStormUtils.halt_process(-1, err);
		} finally {
			sendTimer.stop();
		}
	}

	@Override
	public void handleResponse() {
		emitTs.set(0);
		sendData();
	}

	@Override
	public void onEvent(Object event, long sequence, boolean endOfBatch)
			throws Exception {
		if (event == null) {
			return;
		}

		TaskMessage message = (TaskMessage) event;

		MessageBatch messageBatch = messageBatchRef.getAndSet(null);
		if (null == messageBatch) {
			messageBatch = new MessageBatch(messageBatchSize);
		}

		messageBatch.add(message);

		if (messageBatch.isFull()) {
			batchQueue.offer(messageBatch);
		} else if (endOfBatch == true) {
			batchQueue.offer(messageBatch);
		} else {
			messageBatchRef.set(messageBatch);
		}
	}

	/**
	 * Handle lost message case
	 */
	void trigger() {
		if (isClosed() == true) {
			return;
		}

		// if long time no receive NettyServer response
		// it is likely lost message
		long emitTime = emitTs.get();
		if (emitTime == 0) {
			return;
		}

		long now = System.currentTimeMillis();

		long delt = now - emitTime;
		if (delt < timeoutSecond * 100) {
			return;
		}

		Channel channel = channelRef.get();
		if (channel != null) {
			LOG.info("Long time no response of {}, {}s", name, delt / 1000);
			channel.write(ControlMessage.EOB_MESSAGE);
		}

	}
	
	protected void shutdownPool() {
		bossExecutor.shutdownNow();
		workerExecutor.shutdownNow();

        try {
        	bossExecutor.awaitTermination(1, TimeUnit.SECONDS);
        	workerExecutor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("Error when shutting down client scheduler", e);
        }
		
		clientChannelFactory.releaseExternalResources();
	}

	@Override
	public void close() {
		LOG.info(
				"Begin to close connection to {} and flush all data, batchQueue {}, disruptor {}",
				name, batchQueue.size(), disruptorQueue.population());
		sendAllData();
		disruptorQueue.haltWithInterrupt();
		Metrics.unregister(address, MetricDef.NETTY_CLI_SYNC_BATCH_QUEUE, null, Metrics.MetricType.WORKER);
		Metrics.unregister(address, MetricDef.NETTY_CLI_SYNC_DISR_QUEUE, null, Metrics.MetricType.WORKER);
		super.close();

		shutdownPool();
		
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
	}
}

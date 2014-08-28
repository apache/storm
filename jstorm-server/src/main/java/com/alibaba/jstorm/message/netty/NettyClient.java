package com.alibaba.jstorm.message.netty;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.daemon.worker.metrics.JStormHistogram;
import com.alibaba.jstorm.daemon.worker.metrics.JStormTimer;
import com.alibaba.jstorm.daemon.worker.metrics.Metrics;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.codahale.metrics.Gauge;

class NettyClient implements IConnection {
	private static final Logger LOG = LoggerFactory
			.getLogger(NettyClient.class);
	public static final String PREFIX = "Netty-Client-";

	// when batch buffer size is more than BATCH_THREASHOLD_WARN
	// it will block Drainer thread
	private final long BATCH_THREASHOLD_WARN;

	private final int max_retries;
	private final int base_sleep_ms;
	private final int max_sleep_ms;

	private AtomicReference<Channel> channelRef;
	private final ClientBootstrap bootstrap;
	private final InetSocketAddress remote_addr;
	private AtomicInteger retries;
	// private final Random random = new Random();
	private final ChannelFactory factory;
	private final int buffer_size;
	private final AtomicBoolean being_closed;

	/**
	 * Don't use request-response method, client just send/server just receive
	 */
	private final boolean noResponse = true;
	private final boolean directlySend;

	private int messageBatchSize;

	private AtomicLong pendings;

	private AtomicReference<MessageBatch> messageBatchRef;
	private AtomicBoolean flush_later;
	private int flushCheckInterval;
	private ScheduledExecutorService scheduler;

	private String sendTimerName;
	private JStormTimer sendTimer;
	private String histogramName;
	private JStormHistogram histogram;
	private String pendingGaugeName;

	private ReconnectRunnable reconnector;

	boolean isDirectSend(Map conf) {

		if (JStormServerUtils.isOnePending(conf) == true) {
			return true;
		}

		return !ConfigExtension.isNettyTransferAsyncBatch(conf);
	}

	@SuppressWarnings("rawtypes")
	NettyClient(Map storm_conf, ChannelFactory factory,
			ScheduledExecutorService scheduler, String host, int port,
			ReconnectRunnable reconnector) {
		this.factory = factory;
		this.scheduler = scheduler;
		this.reconnector = reconnector;
		retries = new AtomicInteger(0);
		channelRef = new AtomicReference<Channel>(null);
		being_closed = new AtomicBoolean(false);
		pendings = new AtomicLong(0);
		flush_later = new AtomicBoolean(false);
		// Configure
		buffer_size = Utils.getInt(storm_conf
				.get(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE));
		max_retries = Math.min(30, Utils.getInt(storm_conf
				.get(Config.STORM_MESSAGING_NETTY_MAX_RETRIES)));
		base_sleep_ms = Utils.getInt(storm_conf
				.get(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS));
		max_sleep_ms = Utils.getInt(storm_conf
				.get(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS));

		BATCH_THREASHOLD_WARN = ConfigExtension
				.getNettyBufferThresholdSize(storm_conf);

		directlySend = isDirectSend(storm_conf);

		this.messageBatchSize = Utils.getInt(
				storm_conf.get(Config.STORM_NETTY_MESSAGE_BATCH_SIZE), 262144);
		flushCheckInterval = Utils.getInt(
				storm_conf.get(Config.STORM_NETTY_FLUSH_CHECK_INTERVAL_MS), 10);

		messageBatchRef = new AtomicReference<MessageBatch>();

		bootstrap = new ClientBootstrap(factory);
		bootstrap.setOption("tcpNoDelay", true);
		bootstrap.setOption("sendBufferSize", buffer_size);
		bootstrap.setOption("keepAlive", true);

		// Set up the pipeline factory.
		bootstrap.setPipelineFactory(new StormClientPipelineFactory(this));

		// Start the connection attempt.
		remote_addr = new InetSocketAddress(host, port);
		bootstrap.connect(remote_addr);

		sendTimerName = JStormServerUtils.getName(host, port)
				+ "-netty-send-timer";
		sendTimer = Metrics.registerTimer(sendTimerName);
		histogramName = JStormServerUtils.getName(host, port)
				+ "-netty-send-histogram";
		histogram = Metrics.registerHistograms(histogramName);

		pendingGaugeName = JStormServerUtils.getName(host, port)
				+ "-netty-send-pending-gauge";
		Metrics.register(pendingGaugeName, new Gauge<Long>() {

			@Override
			public Long getValue() {
				return pendings.get();
			}
		});

		StringBuilder sb = new StringBuilder();
		sb.append("New Netty Client, connect to ").append(remote_addr);
		sb.append(", netty buffer size").append(buffer_size);
		sb.append(", batch size:").append(messageBatchSize);
		sb.append(", directlySend:").append(directlySend);
		sb.append(", slow down buffer threshold size:").append(
				BATCH_THREASHOLD_WARN);
		LOG.info(sb.toString());

		Runnable flusher = new Runnable() {
			@Override
			public void run() {
				flush();
			}
		};
		long initialDelay = Math.min(1000, max_sleep_ms * max_retries);
		scheduler.scheduleWithFixedDelay(flusher, initialDelay,
				flushCheckInterval, TimeUnit.MILLISECONDS);
	}

	/**
	 * The function can't be synchronized, otherwise it will be deadlock
	 * 
	 */
	public void doReconnect() {
		if (channelRef.get() != null) {
			return;
		}

		if (isClosed() == true) {
			return;
		}

		long sleepMs = getSleepTimeMs();
		LOG.info("Reconnect ... [{}], {}, sleep {}ms", retries.get(),
				remote_addr, sleepMs);
		ChannelFuture future = bootstrap.connect(remote_addr);
		future.addListener(new ChannelFutureListener() {
			public void operationComplete(ChannelFuture future)
					throws Exception {

				if (future.isSuccess()) {
					// do something else
				} else {
					LOG.info("Failed to reconnect ... [{}], {}", retries.get(),
							remote_addr);
					reconnect();
				}
			}
		});
		JStormUtils.sleepMs(sleepMs);

		return;

	}

	public void reconnect() {
		reconnector.pushEvent(this);
	}

	/**
	 * # of milliseconds to wait per exponential back-off policy
	 */
	private int getSleepTimeMs() {

		int sleepMs =  base_sleep_ms * retries.incrementAndGet();
		if (sleepMs > 1000) {
			sleepMs = 1000;
		}
		return sleepMs ;
	}

	/**
	 * Enqueue a task message to be sent to server
	 */
	@Override
	public void send(List<TaskMessage> messages) {
		// throw exception if the client is being closed
		if (isClosed()) {
			LOG.warn("Client is being closed, and does not take requests any more");
			return;
		}

		sendTimer.start();
		try {
			pushBatch(messages);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			sendTimer.stop();
			
		}
	}

	@Override
	public void send(TaskMessage message) {
		// throw exception if the client is being closed
		if (isClosed()) {
			LOG.warn("Client is being closed, and does not take requests any more");
			return;
		}

		sendTimer.start();
		try {
			pushBatch(message);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			sendTimer.stop();
		}
	}

	void handleFailedChannel(MessageBatch messageBatch) {

		messageBatchRef.set(messageBatch);
		flush_later.set(true);

		long cachedSize = messageBatch.getEncoded_length();
		if (cachedSize > BATCH_THREASHOLD_WARN) {
			long count = (cachedSize + BATCH_THREASHOLD_WARN - 1)
					/ BATCH_THREASHOLD_WARN;
			long sleepMs = count * 20;
			LOG.warn("Too much cached buffer {}, sleep {}ms, {}", cachedSize,
					sleepMs, remote_addr.toString());

			JStormUtils.sleepMs(sleepMs);
			reconnect();
		}
		return;
	}

	void pushBatch(List<TaskMessage> messages) {

		if (messages.isEmpty()) {
			return;
		}

		MessageBatch messageBatch = messageBatchRef.getAndSet(null);
		if (null == messageBatch) {
			messageBatch = new MessageBatch(messageBatchSize);
		}

		for (TaskMessage message : messages) {
			if (TaskMessage.isEmpty(message)) {
				continue;
			}

			messageBatch.add(message);

			if (messageBatch.isFull()) {
				Channel channel = isChannelReady();
				if (channel != null) {
					flushRequest(channel, messageBatch);

					messageBatch = new MessageBatch(messageBatchSize);
				}

			}
		}

		Channel channel = isChannelReady();
		if (channel == null) {
			handleFailedChannel(messageBatch);
			return;
		} else if (messageBatch.isEmpty() == false) {
			flushRequest(channel, messageBatch);
		}

		return;
	}

	void pushBatch(TaskMessage message) {

		if (TaskMessage.isEmpty(message)) {
			return;
		}

		MessageBatch messageBatch = messageBatchRef.getAndSet(null);
		if (null == messageBatch) {
			messageBatch = new MessageBatch(messageBatchSize);
		}

		messageBatch.add(message);

		Channel channel = isChannelReady();
		if (channel == null) {
			handleFailedChannel(messageBatch);
			return;
		}

		if (messageBatch.isFull()) {
			flushRequest(channel, messageBatch);

			return;
		}

		if (directlySend) {
			flushRequest(channel, messageBatch);
		} else {
			messageBatchRef.compareAndSet(null, messageBatch);
			flush_later.set(true);
		}

		return;
	}

	/**
	 * Take all enqueued messages from queue
	 * 
	 * @return
	 * @throws InterruptedException
	 */
	@Deprecated
	MessageBatch takeMessages() throws Exception {
		return messageBatchRef.getAndSet(null);
	}

	public String name() {
		if (null != remote_addr) {
			return PREFIX + remote_addr.toString();
		}
		return "";
	}

	private void flush() {
		if (isClosed() == true) {
			return;
		}

		if (flush_later.get() == false) {
			return;
		}

		Channel channel = isChannelReady();
		if (channel == null) {
			return;
		}

		MessageBatch toBeFlushed = messageBatchRef.getAndSet(null);
		flushRequest(channel, toBeFlushed);
		flush_later.set(false);
	}

	private synchronized void flushRequest(Channel channel,
			final MessageBatch requests) {
		if (requests == null || requests.isEmpty())
			return;

		histogram.update(requests.getEncoded_length());
		long pending = pendings.incrementAndGet();
		LOG.debug("Flush pending {}, this message size:{}", pending,
				requests.getEncoded_length());
		ChannelFuture future = channel.write(requests);
		future.addListener(new ChannelFutureListener() {
			public void operationComplete(ChannelFuture future)
					throws Exception {

				pendings.decrementAndGet();
				if (!future.isSuccess()) {
					if (isClosed() == false) {
						LOG.info(
								"Failed to send requests to "
										+ remote_addr.toString() + ": ",
								future.getCause());
					}

					Channel channel = future.getChannel();

					if (null != channel) {

						exceptionChannel(channel);
					}
				} else {
					LOG.debug("{} request(s) sent", requests.size());
				}
			}
		});
	}

	/**
	 * gracefully close this client.
	 * 
	 * We will send all existing requests, and then invoke close_n_release()
	 * method
	 */
	public synchronized void close() {
		LOG.info("Close netty connection to {}", name());

		Metrics.unregister(pendingGaugeName);
		Metrics.unregister(histogramName);
		Metrics.unregister(sendTimerName);

		if (isClosed() == true) {
			return;
		}
		being_closed.set(true);

		Channel channel = channelRef.get();
		if (channel == null) {
			LOG.info("Channel {} has been closed before", name());
			return;
		}

		if (channel.isWritable()) {
			MessageBatch toBeFlushed = messageBatchRef.getAndSet(null);
			flushRequest(channel, toBeFlushed);
		}

		// wait for pendings to exit
		final long timeoutMilliSeconds = 10 * 1000;
		final long start = System.currentTimeMillis();

		LOG.info("Waiting for pending batchs to be sent with " + name()
				+ "..., timeout: {}ms, pendings: {}", timeoutMilliSeconds,
				pendings.get());

		while (pendings.get() != 0) {
			try {
				long delta = System.currentTimeMillis() - start;
				if (delta > timeoutMilliSeconds) {
					LOG.error(
							"Timeout when sending pending batchs with {}..., there are still {} pending batchs not sent",
							name(), pendings.get());
					break;
				}
				Thread.sleep(1000); // sleep 1s
			} catch (InterruptedException e) {
				break;
			}
		}

		close_n_release();

	}

	/**
	 * close_n_release() is invoked after all messages have been sent.
	 */
	void close_n_release() {
		if (channelRef.get() != null) {
			setChannel(null);
		}
	}

	void exceptionChannel(Channel channel) {
		if (channel == channelRef.get()) {
			setChannel(null);
		} else {
			channel.close();
		}
	}

	void setChannel(Channel newChannel) {
		Channel oldChannel = channelRef.getAndSet(newChannel);

		if (newChannel != null) {
			retries.set(0);
		}

		String oldLocalAddres = (oldChannel == null) ? "null" : oldChannel
				.getLocalAddress().toString();
		String newLocalAddress = (newChannel == null) ? "null" : newChannel
				.getLocalAddress().toString();
		LOG.info("Use new channel {} replace old channel {}", newLocalAddress,
				oldLocalAddres);

		// avoid one netty client use too much connection, close old one
		if (oldChannel != newChannel && oldChannel != null) {
			oldChannel.close();
			LOG.info("Close old channel " + oldLocalAddres);
		}
	}

	Channel isChannelReady() {
		Channel channel = channelRef.get();
		if (channel == null) {
			return null;
		}

		// improve performance skill check
		if (channel.isWritable() == false) {
			return null;
		}
		return channel;
	}

	@Override
	public boolean isClosed() {
		return being_closed.get();
	}

	public AtomicBoolean getBeing_closed() {
		return being_closed;
	}

	public int getBuffer_size() {
		return buffer_size;
	}

	public SocketAddress getRemoteAddr() {
		return remote_addr;
	}

	public boolean isNoResponse() {
		return noResponse;
	}

	@Override
	public TaskMessage recv(int flags) {
		throw new UnsupportedOperationException(
				"recvTask: Client connection should not receive any messages");
	}

	@Override
	public void registerQueue(DisruptorQueue recvQueu) {
		throw new UnsupportedOperationException(
				"recvTask: Client connection should not receive any messages");
	}

	@Override
	public void enqueue(TaskMessage message) {
		throw new UnsupportedOperationException(
				"recvTask: Client connection should not receive any messages");
	}

	public void blockReConnect() {

		Channel channel = null;

		try {

			int tried = 0;
			while (tried <= max_retries) {

				LOG.info("Reconnect started for {}... [{}]", name(), tried);

				ChannelFuture future = bootstrap.connect(remote_addr);
				future.awaitUninterruptibly(10, TimeUnit.SECONDS);
				Channel current = future.getChannel();
				if (!future.isSuccess()) {
					if (null != current) {
						current.close();
					}
				} else {
					channel = current;
					break;
				}
				JStormUtils.sleepMs(getSleepTimeMs());
				tried++;
			}
		} catch (Exception e) {
			LOG.error("Occur exception when reconnect to " + name());
			channel = null;
		}

		if (null != channel) {
			LOG.info("connection established to a remote host " + name() + ", "
					+ channel.toString());
			setChannel(channel);
		} else {
			LOG.info("Failed to connect to a remote host " + name());
			JStormUtils.sleepMs(getSleepTimeMs());
		}
	}
}

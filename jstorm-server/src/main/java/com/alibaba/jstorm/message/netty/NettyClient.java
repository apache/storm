package com.alibaba.jstorm.message.netty;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
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
	protected String name;

	protected final int max_retries;
	protected final int base_sleep_ms;
	protected final int max_sleep_ms;
	protected final int timeoutSecond;
	protected final int MAX_SEND_PENDING;

	protected AtomicInteger retries;

	protected AtomicReference<Channel> channelRef;
	protected ClientBootstrap bootstrap;
	protected final InetSocketAddress remote_addr;
	protected final ChannelFactory factory;

	protected final int buffer_size;
	protected final AtomicBoolean being_closed;

	protected AtomicLong pendings;
	protected int messageBatchSize;
	protected AtomicReference<MessageBatch> messageBatchRef;

	protected ScheduledExecutorService scheduler;

	protected String sendTimerName;
	protected JStormTimer sendTimer;
	protected String histogramName;
	protected JStormHistogram histogram;
	protected String pendingGaugeName;

	protected ReconnectRunnable reconnector;
	protected ChannelFactory clientChannelFactory;

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

		// Configure
		buffer_size = Utils.getInt(storm_conf
				.get(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE));
		max_retries = Math.min(30, Utils.getInt(storm_conf
				.get(Config.STORM_MESSAGING_NETTY_MAX_RETRIES)));
		base_sleep_ms = Utils.getInt(storm_conf
				.get(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS));
		max_sleep_ms = Utils.getInt(storm_conf
				.get(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS));

		timeoutSecond = JStormUtils.parseInt(
				storm_conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS), 30);
		MAX_SEND_PENDING = (int) ConfigExtension
				.getNettyMaxSendPending(storm_conf);

		this.messageBatchSize = Utils.getInt(
				storm_conf.get(Config.STORM_NETTY_MESSAGE_BATCH_SIZE), 262144);
		messageBatchRef = new AtomicReference<MessageBatch>();

		// Start the connection attempt.
		remote_addr = new InetSocketAddress(host, port);
		name = remote_addr.toString();

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

	}

	public void start() {
		bootstrap = new ClientBootstrap(clientChannelFactory);
		bootstrap.setOption("tcpNoDelay", true);
		bootstrap.setOption("sendBufferSize", buffer_size);
		bootstrap.setOption("keepAlive", true);

		// Set up the pipeline factory.
		bootstrap.setPipelineFactory(new StormClientPipelineFactory(this));
		doReconnect();
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
		LOG.info("Reconnect ... [{}], {}, sleep {}ms", retries.get(), name,
				sleepMs);
		ChannelFuture future = bootstrap.connect(remote_addr);
		future.addListener(new ChannelFutureListener() {
			public void operationComplete(ChannelFuture future)
					throws Exception {

				if (future.isSuccess()) {
					// do something else
				} else {
					LOG.info("Failed to reconnect ... [{}], {}", retries.get(),
							name);
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

		int sleepMs = base_sleep_ms * retries.incrementAndGet();
		if (sleepMs > 1000) {
			sleepMs = 1000;
		}
		return sleepMs;
	}

	/**
	 * Enqueue a task message to be sent to server
	 */
	@Override
	public void send(List<TaskMessage> messages) {
		LOG.warn("Should be overload");
	}

	@Override
	public void send(TaskMessage message) {
		LOG.warn("Should be overload");
	}

	protected synchronized void flushRequest(Channel channel,
			final MessageBatch requests) {
		if (requests == null || requests.isEmpty())
			return;

		histogram.update(requests.getEncoded_length());
		pendings.incrementAndGet();
		ChannelFuture future = channel.write(requests);
		future.addListener(new ChannelFutureListener() {
			public void operationComplete(ChannelFuture future)
					throws Exception {

				pendings.decrementAndGet();
				if (!future.isSuccess()) {
					if (isClosed() == false) {
						LOG.info("Failed to send requests to " + name + ": ",
								future.getCause());
					}

					Channel channel = future.getChannel();

					if (null != channel) {

						exceptionChannel(channel);
					}
				} else {
					// LOG.debug("{} request(s) sent", requests.size());
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
		if (isClosed() == true) {
			return;
		}
		being_closed.set(true);

		Metrics.unregister(pendingGaugeName);
		Metrics.unregister(histogramName);
		Metrics.unregister(sendTimerName);

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

			// @@@ todo
			// pendings.set(0);
		}
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

	public String name() {
		return name;
	}

	public void handleResponse() {
		LOG.warn("Should be overload");
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
}

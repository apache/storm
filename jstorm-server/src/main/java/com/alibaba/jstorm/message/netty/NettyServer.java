package com.alibaba.jstorm.message.netty;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.utils.JStormUtils;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.WaitStrategy;

class NettyServer implements IConnection {
	private static final Logger LOG = LoggerFactory
			.getLogger(NettyServer.class);
	@SuppressWarnings("rawtypes")
	Map storm_conf;
	int port;
	private LinkedBlockingQueue<TaskMessage> blockQ;
	private DisruptorQueue disruptorQ;
	private final boolean useDisruptor;
	// private LinkedBlockingQueue message_queue;
	volatile ChannelGroup allChannels = new DefaultChannelGroup("jstorm-server");
	final ChannelFactory factory;
	final ServerBootstrap bootstrap;

	// @@@ testing code
	private final AtomicInteger counter = new AtomicInteger(0);

	@SuppressWarnings("rawtypes")
	NettyServer(Map storm_conf, int port) {
		this.storm_conf = storm_conf;
		this.port = port;

		useDisruptor = ConfigExtension.isNettyEnableDisruptor(storm_conf);
		blockQ = new LinkedBlockingQueue<TaskMessage>();
		int queue_size = Utils.getInt(storm_conf
				.get(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE));
		WaitStrategy waitStrategy = (WaitStrategy) Utils
				.newInstance((String) storm_conf
						.get(Config.TOPOLOGY_DISRUPTOR_WAIT_STRATEGY));
		disruptorQ = new DisruptorQueue(new SingleThreadedClaimStrategy(
				queue_size), waitStrategy);

		// Configure the server.
		int buffer_size = Utils.getInt(storm_conf
				.get(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE));
		int maxWorkers = Utils.getInt(storm_conf
				.get(Config.STORM_MESSAGING_NETTY_SERVER_WORKER_THREADS));

		if (maxWorkers > 0) {
			factory = new NioServerSocketChannelFactory(
					Executors.newCachedThreadPool(),
					Executors.newCachedThreadPool(), maxWorkers);
		} else {
			factory = new NioServerSocketChannelFactory(
					Executors.newCachedThreadPool(),
					Executors.newCachedThreadPool());
		}
		bootstrap = new ServerBootstrap(factory);
		bootstrap.setOption("child.tcpNoDelay", true);
		bootstrap.setOption("child.receiveBufferSize", buffer_size);
		bootstrap.setOption("child.keepAlive", true);

		// Set up the pipeline factory.
		bootstrap.setPipelineFactory(new StormServerPipelineFactory(this));

		// Bind and start to accept incoming connections.
		Channel channel = bootstrap.bind(new InetSocketAddress(port));
		allChannels.add(channel);

		LOG.info("Successfull bind {}, use disruptor:{}", port, useDisruptor);
	}

	/**
	 * enqueue a received message
	 * 
	 * @param message
	 * @throws InterruptedException
	 */
	protected void enqueue(TaskMessage message) throws InterruptedException {
		LOG.debug("message received with task: {},  receive: {} ",
				message.task(), counter.incrementAndGet());

		if (useDisruptor) {
			disruptorQ.publish(message);
		} else {
			blockQ.offer(message);
		}

	}

	/**
	 * fetch a message from message queue synchronously (flags != 1) or
	 * asynchronously (flags==1)
	 */
	public byte[] recv(int flags) {
		try {
			Object message = null;
			if ((flags & 0x01) == 0x01) {
				// non-blocking
				if (useDisruptor) {
					message = disruptorQ.poll();
				} else {
					message = blockQ.poll();
				}
			} else {
				if (useDisruptor) {
					message = disruptorQ.take();
				} else {
					message = blockQ.take();
				}
				// LOG.debug("request to be processed: {}", message);
			}

			if (message != null) {
				return ((TaskMessage) message).message();
			} else {
				return null;
			}
		} catch (Exception e) {
			LOG.warn("Occur unexception ", e);
			return null;
		}

	}

	/**
	 * register a newly created channel
	 * 
	 * @param channel
	 */
	protected void addChannel(Channel channel) {
		allChannels.add(channel);
	}

	/**
	 * close a channel
	 * 
	 * @param channel
	 */
	protected void closeChannel(Channel channel) {
		channel.close().awaitUninterruptibly();
		allChannels.remove(channel);
	}

	/**
	 * close all channels, and release resources
	 */
	public synchronized void close() {
		if (allChannels != null) {
			allChannels.close().awaitUninterruptibly();
			factory.releaseExternalResources();
			allChannels = null;
		}
	}

	public void send(int task, byte[] message) {
		throw new UnsupportedOperationException(
				"Server connection should not send any messages");
	}

	@Override
	public boolean isClosed() {
		// TODO Auto-generated method stub
		return false;
	}
}

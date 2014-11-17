package com.alibaba.jstorm.message.netty;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.utils.JStormUtils;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;

class NettyServer implements IConnection {
	private static final Logger LOG = LoggerFactory
			.getLogger(NettyServer.class);
	@SuppressWarnings("rawtypes")
	Map storm_conf;
	int port;
	DisruptorQueue recvQueue;
	// private LinkedBlockingQueue message_queue;
	volatile ChannelGroup allChannels = new DefaultChannelGroup("jstorm-server");
	final ChannelFactory factory;
	final ServerBootstrap bootstrap;

	// ayncBatch is only one solution, so directly set it as true
	private final boolean isSyncMode;

	@SuppressWarnings("rawtypes")
	NettyServer(Map storm_conf, int port, boolean isSyncMode) {
		this.storm_conf = storm_conf;
		this.port = port;
		this.isSyncMode = isSyncMode;
		
		// Configure the server.
		int buffer_size = Utils.getInt(storm_conf
				.get(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE));
		int maxWorkers = Utils.getInt(storm_conf
				.get(Config.STORM_MESSAGING_NETTY_SERVER_WORKER_THREADS));

		//asyncBatch = ConfigExtension.isNettyTransferAsyncBatch(storm_conf);

		ThreadFactory bossFactory = new NettyRenameThreadFactory("server"
				+ "-boss");
		ThreadFactory workerFactory = new NettyRenameThreadFactory("server"
				+ "-worker");
		if (maxWorkers > 0) {
			factory = new NioServerSocketChannelFactory(
					Executors.newCachedThreadPool(bossFactory),
					Executors.newCachedThreadPool(workerFactory), maxWorkers);
		} else {
			factory = new NioServerSocketChannelFactory(
					Executors.newCachedThreadPool(bossFactory),
					Executors.newCachedThreadPool(workerFactory));
		}

		bootstrap = new ServerBootstrap(factory);
		bootstrap.setOption("reuserAddress", true);
		bootstrap.setOption("child.tcpNoDelay", true);
		bootstrap.setOption("child.receiveBufferSize", buffer_size);
		bootstrap.setOption("child.keepAlive", true);

		// Set up the pipeline factory.
		bootstrap.setPipelineFactory(new StormServerPipelineFactory(this));

		// Bind and start to accept incoming connections.
		Channel channel = bootstrap.bind(new InetSocketAddress(port));
		allChannels.add(channel);

		LOG.info("Successfull bind {}, buffer_size:{}, maxWorkers:{}", port,
				buffer_size, maxWorkers);
	}

	@Override
	public void registerQueue(DisruptorQueue recvQueu) {
		this.recvQueue = recvQueu;
	}
	/**
	 * enqueue a received message
	 * 
	 * @param message
	 * @throws InterruptedException
	 */
	public void enqueue(TaskMessage message) {

		recvQueue.publish(message);

	}

	/**
	 * fetch a message from message queue synchronously (flags != 1) or
	 * asynchronously (flags==1)
	 */
	public TaskMessage recv(int flags) {
		try {
			if ((flags & 0x01) == 0x01) {
				return (TaskMessage) recvQueue.poll();
				// non-blocking

			} else {
				return (TaskMessage) recvQueue.take();

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
		LOG.info("Begin to shutdown NettyServer");
		if (allChannels != null) {
			new Thread(new Runnable() {
				
				@Override
				public void run() {
					try {
						// await(5, TimeUnit.SECONDS)
						// sometimes allChannels.close() will block the exit thread
						allChannels.close().await(1, TimeUnit.SECONDS);
						LOG.info("Successfully close all channel");
						factory.releaseExternalResources();
					}catch(Exception e) {
						
					}
					allChannels = null;
				}
			}).start();
			
			JStormUtils.sleepMs(1 * 1000);
		}
		LOG.info("Successfully shutdown NettyServer");
	}

	@Override
	public void send(List<TaskMessage> messages) {
		throw new UnsupportedOperationException(
				"Server connection should not send any messages");
	}
	
	@Override
	public void send(TaskMessage message) {
		throw new UnsupportedOperationException(
				"Server connection should not send any messages");
	}

	@Override
	public boolean isClosed() {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean isSyncMode() {
		return isSyncMode;
	}

	

	

}

package com.alibaba.jstorm.message.netty;

import java.net.ConnectException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormClientHandler extends SimpleChannelUpstreamHandler {
	private static final Logger LOG = LoggerFactory
			.getLogger(StormClientHandler.class);
	private NettyClient client;
	private AtomicBoolean being_closed;
	long start_time;

	// @@@ testing code
	private final AtomicInteger msgCounter = new AtomicInteger(0);
	private final AtomicInteger batchCounter = new AtomicInteger(0);
	private final AtomicInteger trymsgCounter = new AtomicInteger(0);
	private final AtomicInteger trybatchCounter = new AtomicInteger(0);
	private final AtomicInteger recvCounter = new AtomicInteger(0);

	StormClientHandler(NettyClient client) {
		this.client = client;
		being_closed = client.getBeing_closed();
		start_time = System.currentTimeMillis();
	}

	@Override
	public void channelConnected(ChannelHandlerContext ctx,
			ChannelStateEvent event) {
		// register the newly established channel
		Channel channel = event.getChannel();
		client.setChannel(channel);
		LOG.info("connection established to :{}", client.getRemoteAddr());

		try {
			// send next request
			sendRequests(channel, client.takeMessages());
		} catch (Exception e) {
			if (being_closed.get() == false) {
				LOG.warn("Occur channel error\n", e);
				channel.close();

				// @@@ right now, throw the exception
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent event) {
		// LOG.debug("{} send/recv time (ms): {}",
		// recvCounter.incrementAndGet(),
		// (System.currentTimeMillis() - start_time));

		// examine the response message from server
		ControlMessage msg = (ControlMessage) event.getMessage();
		if (msg == ControlMessage.FAILURE_RESPONSE)
			LOG.info("failure response:{}", msg);

		// send next request
		Channel channel = event.getChannel();

		try {
			// send next request
			sendRequests(channel, client.takeMessages());
		} catch (Exception e) {
			if (being_closed.get() == false) {
				LOG.warn("Occur channel error\n", e);
				channel.close();

				// @@@ right now, throw the exception
				throw new RuntimeException(e);
			}
		}

	}

	/**
	 * Retrieve a request from message queue, and send to server
	 * 
	 * @param channel
	 */
	private void sendRequests(final Channel channel, final MessageBatch requests) {
		if (requests == null || requests.isEmpty() || being_closed.get())
			return;

		// if task==CLOSE_MESSAGE for our last request, the channel is to be
		// closed
		Object last_msg = requests.get(requests.size() - 1);
		if (last_msg == ControlMessage.CLOSE_MESSAGE) {
			being_closed.set(true);
			requests.remove(last_msg);
		}

		// we may don't need do anything if no requests found
		if (requests.isEmpty()) {
			// if (being_closed.get()) {
			// client.close_n_release();
			// }
			return;
		}

		// LOG.debug("try send batch trybatchCounter:{}, trymsgCounter:{}",
		// trybatchCounter.incrementAndGet(),
		// trymsgCounter.addAndGet(requests.size()));

		// write request into socket channel
		ChannelFuture future = channel.write(requests);
		future.addListener(new ChannelFutureListener() {
			public void operationComplete(ChannelFuture future)
					throws Exception {
				if (!future.isSuccess()) {
					LOG.warn("Failed to send requests:", future.getCause());
					future.getChannel().close();
				} else {
					// LOG.debug("Send batch counter:{}, request.size:{}",
					// batchCounter.incrementAndGet(),
					// msgCounter.addAndGet(requests.size()));
				}

				/**
				 * client.close_n_release() only do in client.close
				 */
				// if (being_closed.get()) {
				// client.close_n_release();
				// }
			}
		});
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent event) {
		Throwable cause = event.getCause();
		if (being_closed.get() == false) {
			if (!(cause instanceof ConnectException)) {
				LOG.info("Connection failed:" + client.getRemoteAddr(),
						cause);
			}
			client.setChannel(null);
			client.reconnect();
		}
	}

	@Override
	public void channelDisconnected(ChannelHandlerContext ctx,
			ChannelStateEvent e) throws Exception {
		// ctx.sendUpstream(e);
		super.channelDisconnected(ctx, e);
		LOG.info("Receive channelDisconnected to {}", client.getRemoteAddr());

		if (!being_closed.get()) {
			client.setChannel(null);
			client.reconnect();
		}
	}

	// @Override
	// public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
	// throws Exception {
	// super.channelClosed(ctx, e);
	//
	// LOG.info("Connection to {} has been closed", client.getTarget_Server());
	//
	// }

}

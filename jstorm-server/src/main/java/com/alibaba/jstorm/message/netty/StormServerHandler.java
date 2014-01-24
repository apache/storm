package com.alibaba.jstorm.message.netty;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.messaging.TaskMessage;

class StormServerHandler extends SimpleChannelUpstreamHandler {
	private static final Logger LOG = LoggerFactory
			.getLogger(StormServerHandler.class);
	private NettyServer server;
	private Map<Channel, Integer> failureCounters;

	StormServerHandler(NettyServer server) {
		this.server = server;
		failureCounters = new ConcurrentHashMap<Channel, Integer>();
	}

	protected int getFailureCounter(Channel channel) {
		Integer num = failureCounters.get(channel);
		if (num == null) {
			return 0;
		}

		return num;
	}

	protected void incFailureCounter(Channel channel) {
		Integer num = failureCounters.get(channel);
		if (num == null) {
			num = Integer.valueOf(0);
		}
		num = num + 1;

		failureCounters.put(channel, num);
	}

	protected void removeFailureCounter(Channel channel) {
		failureCounters.remove(channel);
	}

	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
		server.addChannel(e.getChannel());
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
		Object msg = e.getMessage();
		if (msg == null)
			return;

		// end of batch?
		if (msg == ControlMessage.EOB_MESSAGE) {
			Channel channel = ctx.getChannel();
			LOG.debug("Receive ...{}", msg);
			// simplify the logic, just send OK_RESPONSE
			channel.write(ControlMessage.OK_RESPONSE);
			// if (getFailureCounter(channel) == 0) {
			// channel.write(ControlMessage.OK_RESPONSE);
			// }else {
			// channel.write(ControlMessage.FAILURE_RESPONSE);
			// removeFailureCounter(channel);
			// }

			return;
		} else if (msg instanceof ControlMessage) {
			LOG.debug("Receive ...{}", msg);
			return;
		}

		// enqueue the received message for processing
		try {
			server.enqueue((TaskMessage) msg);
		} catch (Exception e1) {
			LOG.warn("Failed to enqueue a request message", e);
			// Channel channel = ctx.getChannel();
			// incFailureCounter(channel);
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
		// removeFailureCounter(e.getChannel());
		server.closeChannel(e.getChannel());
	}
}

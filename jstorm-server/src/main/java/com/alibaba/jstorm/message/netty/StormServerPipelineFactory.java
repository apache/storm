package com.alibaba.jstorm.message.netty;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;

class StormServerPipelineFactory implements ChannelPipelineFactory {
	private NettyServer server;

	StormServerPipelineFactory(NettyServer server) {
		this.server = server;
	}

	public ChannelPipeline getPipeline() throws Exception {
		// Create a default pipeline implementation.
		ChannelPipeline pipeline = Channels.pipeline();

		// Decoder
		pipeline.addLast("decoder", new MessageDecoder(true));
		// Encoder
		pipeline.addLast("encoder", new MessageEncoder());
		// business logic.
		pipeline.addLast("handler", new StormServerHandler(server));

		return pipeline;
	}
}

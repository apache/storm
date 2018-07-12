
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.messaging.netty;

import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.serialization.KryoValuesDeserializer;
import org.apache.storm.serialization.KryoValuesSerializer;
import org.apache.storm.shade.io.netty.channel.Channel;
import org.apache.storm.shade.io.netty.channel.ChannelInitializer;
import org.apache.storm.shade.io.netty.channel.ChannelPipeline;

class StormServerPipelineFactory extends ChannelInitializer<Channel> {

    private final Map<String, Object> topoConf;
    private final Server server;

    StormServerPipelineFactory(Map<String, Object> topoConf, Server server) {
        this.topoConf = topoConf;
        this.server = server;
    }

    @Override
    protected void initChannel(Channel ch) throws Exception {
        // Create a default pipeline implementation.
        ChannelPipeline pipeline = ch.pipeline();

        // Decoder
        pipeline.addLast("decoder", new MessageDecoder(new KryoValuesDeserializer(topoConf)));
        // Encoders
        pipeline.addLast("netty-serializable-encoder", NettySerializableMessageEncoder.INSTANCE);
        pipeline.addLast("backpressure-encoder", new BackPressureStatusEncoder(new KryoValuesSerializer(topoConf)));

        boolean isNettyAuth = (Boolean) topoConf
            .get(Config.STORM_MESSAGING_NETTY_AUTHENTICATION);
        if (isNettyAuth) {
            // Authenticate: Removed after authentication completes
            pipeline.addLast("saslServerHandler", new SaslStormServerHandler(
                server));
            // Authorize
            pipeline.addLast("authorizeServerHandler",
                new SaslStormServerAuthorizeHandler());
        }
        // business logic.
        pipeline.addLast("handler", new StormServerHandler(server));
    }
}

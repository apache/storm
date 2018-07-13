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
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.storm.Config;
import org.apache.storm.serialization.KryoValuesDeserializer;
import org.apache.storm.shade.io.netty.channel.Channel;
import org.apache.storm.shade.io.netty.channel.ChannelInitializer;
import org.apache.storm.shade.io.netty.channel.ChannelPipeline;

class StormClientPipelineFactory extends ChannelInitializer<Channel> {
    private final Client client;
    private final AtomicBoolean[] remoteBpStatus;
    private final Map<String, Object> conf;

    StormClientPipelineFactory(Client client, AtomicBoolean[] remoteBpStatus, Map<String, Object> conf) {
        this.client = client;
        this.remoteBpStatus = remoteBpStatus;
        this.conf = conf;
    }

    @Override
    protected void initChannel(Channel ch) throws Exception {
        // Create a default pipeline implementation.
        ChannelPipeline pipeline = ch.pipeline();

        // Decoder
        pipeline.addLast("decoder", new MessageDecoder(new KryoValuesDeserializer(conf)));
        // Encoder
        pipeline.addLast("encoder", NettySerializableMessageEncoder.INSTANCE);

        boolean isNettyAuth = (Boolean) conf
            .get(Config.STORM_MESSAGING_NETTY_AUTHENTICATION);
        if (isNettyAuth) {
            // Authenticate: Removed after authentication completes
            pipeline.addLast("saslClientHandler", new SaslStormClientHandler(
                client));
        }
        // business logic.
        pipeline.addLast("handler", new StormClientHandler(client, remoteBpStatus, conf));
    }
}

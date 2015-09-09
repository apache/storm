/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm.messaging.netty;


import backtype.storm.Config;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;

class StormClientPipelineFactory extends ChannelInitializer {
    private Client client;

    StormClientPipelineFactory(Client client) {
        this.client = client;
    }

    @Override
    protected void initChannel(Channel ch) throws Exception {
        // Create a default pipeline implementation.
        ChannelPipeline pipeline = ch.pipeline();

        // Decoder
        pipeline.addLast("decoder", new MessageDecoder());
        // Encoder
        pipeline.addLast("encoder", new MessageEncoder());

        boolean isNettyAuth = (Boolean) this.client.getStormConf().get(Config.STORM_MESSAGING_NETTY_AUTHENTICATION);
        if (isNettyAuth) {
            // Authenticate: Removed after authentication completes
            pipeline.addLast("saslClientHandler", new SaslStormClientHandler(
                    client));
        }
        // business logic.
        pipeline.addLast("handler", new StormClientHandler(client));
    }
}

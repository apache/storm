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

package org.apache.storm.pacemaker.codec;

import java.io.IOException;
import java.util.Map;
import org.apache.storm.messaging.netty.KerberosSaslClientHandler;
import org.apache.storm.messaging.netty.SaslStormClientHandler;
import org.apache.storm.pacemaker.PacemakerClient;
import org.apache.storm.pacemaker.PacemakerClientHandler;
import org.apache.storm.security.auth.ClientAuthUtils;
import org.apache.storm.shade.io.netty.channel.Channel;
import org.apache.storm.shade.io.netty.channel.ChannelInitializer;
import org.apache.storm.shade.io.netty.channel.ChannelPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftNettyClientCodec extends ChannelInitializer<Channel> {

    public static final String SASL_HANDLER = "sasl-handler";
    public static final String KERBEROS_HANDLER = "kerberos-handler";
    private static final Logger LOG = LoggerFactory
        .getLogger(ThriftNettyClientCodec.class);

    private final int thriftMessageMaxSize;
    private final PacemakerClient client;
    private final AuthMethod authMethod;
    private final Map<String, Object> topoConf;
    private final String host;

    public ThriftNettyClientCodec(PacemakerClient pacemakerClient, Map<String, Object> topoConf,
                                  AuthMethod authMethod, String host, int thriftMessageMaxSizeBytes) {
        client = pacemakerClient;
        this.authMethod = authMethod;
        this.topoConf = topoConf;
        this.host = host;
        thriftMessageMaxSize = thriftMessageMaxSizeBytes;
    }

    @Override
    protected void initChannel(Channel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast("encoder", new ThriftEncoder());
        pipeline.addLast("decoder", new ThriftDecoder(thriftMessageMaxSize));

        if (authMethod == AuthMethod.KERBEROS) {
            try {
                LOG.debug("Adding KerberosSaslClientHandler to pacemaker client pipeline.");
                pipeline.addLast(KERBEROS_HANDLER,
                                 new KerberosSaslClientHandler(client,
                                                               topoConf,
                                                               ClientAuthUtils.LOGIN_CONTEXT_PACEMAKER_CLIENT,
                                                               host));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else if (authMethod == AuthMethod.DIGEST) {
            try {
                LOG.debug("Adding SaslStormClientHandler to pacemaker client pipeline.");
                pipeline.addLast(SASL_HANDLER, new SaslStormClientHandler(client));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            // no work for AuthMethod.NONE
        }

        pipeline.addLast("PacemakerClientHandler", new PacemakerClientHandler(client));
    }

    public enum AuthMethod {
        DIGEST,
        KERBEROS,
        NONE
    }
}

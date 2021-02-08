
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
import java.util.ArrayList;
import java.util.Map;
import org.apache.storm.DaemonConfig;
import org.apache.storm.messaging.netty.ISaslServer;
import org.apache.storm.messaging.netty.IServer;
import org.apache.storm.messaging.netty.KerberosSaslServerHandler;
import org.apache.storm.messaging.netty.SaslStormServerHandler;
import org.apache.storm.messaging.netty.StormServerHandler;
import org.apache.storm.security.auth.ClientAuthUtils;
import org.apache.storm.shade.io.netty.channel.Channel;
import org.apache.storm.shade.io.netty.channel.ChannelInitializer;
import org.apache.storm.shade.io.netty.channel.ChannelPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftNettyServerCodec extends ChannelInitializer<Channel> {

    public static final String SASL_HANDLER = "sasl-handler";
    public static final String KERBEROS_HANDLER = "kerberos-handler";
    private static final Logger LOG = LoggerFactory
        .getLogger(ThriftNettyServerCodec.class);

    ;
    private final int thriftMessageMaxSizeBytes;
    private final IServer server;
    private final AuthMethod authMethod;
    private final Map<String, Object> topoConf;

    public ThriftNettyServerCodec(IServer server, Map<String, Object> topoConf,
                                  AuthMethod authMethod, int thriftMessageMaxSizeBytes) {
        this.server = server;
        this.authMethod = authMethod;
        this.topoConf = topoConf;
        this.thriftMessageMaxSizeBytes = thriftMessageMaxSizeBytes;
    }

    @Override
    protected void initChannel(Channel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast("encoder", new ThriftEncoder());
        pipeline.addLast("decoder", new ThriftDecoder(thriftMessageMaxSizeBytes));
        if (authMethod == AuthMethod.DIGEST) {
            try {
                LOG.debug("Adding SaslStormServerHandler to pacemaker server pipeline.");
                pipeline.addLast(SASL_HANDLER, new SaslStormServerHandler((ISaslServer) server));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else if (authMethod == AuthMethod.KERBEROS) {
            try {
                LOG.debug("Adding KerberosSaslServerHandler to pacemaker server pipeline.");
                ArrayList<String> authorizedUsers = new ArrayList<>(1);
                authorizedUsers.add((String) topoConf.get(DaemonConfig.NIMBUS_DAEMON_USER));
                pipeline.addLast(KERBEROS_HANDLER, new KerberosSaslServerHandler((ISaslServer) server,
                                                                                 topoConf,
                                                                                 ClientAuthUtils.LOGIN_CONTEXT_PACEMAKER_SERVER,
                                                                                 authorizedUsers));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else if (authMethod == AuthMethod.NONE) {
            LOG.debug("Not authenticating any clients. AuthMethod is NONE");
        }

        pipeline.addLast("handler", new StormServerHandler(server));
    }

    public enum AuthMethod {
        DIGEST,
        KERBEROS,
        NONE
    }
}

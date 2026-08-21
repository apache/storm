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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLEngine;
import org.apache.storm.Config;
import org.apache.storm.shade.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.storm.shade.io.netty.handler.ssl.SslContext;
import org.apache.storm.shade.io.netty.handler.ssl.SslContextBuilder;
import org.apache.storm.shade.io.netty.handler.ssl.SslHandler;
import org.apache.storm.shade.io.netty.handler.ssl.SslProvider;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.Test;

public class StormClientPipelineFactoryTest {
    private static final String DST_HOST = "storm-worker-1.example.com";
    private static final int DST_PORT = 6701;

    private SSLEngine initSslEngine(Map<String, Object> conf) throws Exception {
        return initSslEngine(conf, null);
    }

    private SSLEngine initSslEngine(Map<String, Object> conf, SslProvider sslProvider) throws Exception {
        SslContext sslContext = SslContextBuilder.forClient().sslProvider(sslProvider).build();
        StormClientPipelineFactory factory =
            new StormClientPipelineFactory(null, new AtomicBoolean[]{ new AtomicBoolean(false) }, conf, sslContext,
                                           DST_HOST, DST_PORT);
        EmbeddedChannel channel = new EmbeddedChannel(factory);
        try {
            SslHandler sslHandler = (SslHandler) channel.pipeline().get("ssl");
            assertNotNull(sslHandler, "no ssl handler was added to the pipeline");
            return sslHandler.engine();
        } finally {
            channel.close();
        }
    }

    @Test
    public void testSslHandlerVerifiesPeerIdentity() throws Exception {
        Map<String, Object> conf = Utils.readDefaultConfig();
        conf.put(Config.STORM_MESSAGING_NETTY_AUTHENTICATION, false);

        SSLEngine engine = initSslEngine(conf);
        assertEquals("HTTPS", engine.getSSLParameters().getEndpointIdentificationAlgorithm());
        assertEquals(DST_HOST, engine.getPeerHost());
        assertEquals(DST_PORT, engine.getPeerPort());
    }

    @Test
    public void testHostnameVerificationCanBeDisabled() throws Exception {
        Map<String, Object> conf = Utils.readDefaultConfig();
        conf.put(Config.STORM_MESSAGING_NETTY_AUTHENTICATION, false);
        conf.put(Config.STORM_MESSAGING_NETTY_TLS_HOSTNAME_VERIFICATION, false);

        SSLEngine engine = initSslEngine(conf);
        assertEquals("", engine.getSSLParameters().getEndpointIdentificationAlgorithm());
    }

    @Test
    public void testHostnameVerificationCanBeDisabledWithTheJdkSslProvider() throws Exception {
        Map<String, Object> conf = Utils.readDefaultConfig();
        conf.put(Config.STORM_MESSAGING_NETTY_AUTHENTICATION, false);
        conf.put(Config.STORM_MESSAGING_NETTY_TLS_HOSTNAME_VERIFICATION, false);

        // the JDK engine ignores a null algorithm and would keep on verifying
        SSLEngine engine = initSslEngine(conf, SslProvider.JDK);
        assertEquals("", engine.getSSLParameters().getEndpointIdentificationAlgorithm());
    }
}

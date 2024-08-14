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

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.storm.Config;
import org.apache.storm.security.auth.tls.ReloadableX509KeyManager;
import org.apache.storm.security.auth.tls.ReloadableX509TrustManager;
import org.apache.storm.shade.io.netty.handler.ssl.ClientAuth;
import org.apache.storm.shade.io.netty.handler.ssl.OpenSsl;
import org.apache.storm.shade.io.netty.handler.ssl.SslContext;
import org.apache.storm.shade.io.netty.handler.ssl.SslContextBuilder;
import org.apache.storm.shade.io.netty.handler.ssl.SslProvider;
import org.apache.storm.utils.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NettyTlsUtils {
    private static final Logger LOG = LoggerFactory.getLogger(NettyTlsUtils.class);

    public static SslContext createSslContext(Map<String, Object> topoConf, boolean forServer) {
        boolean enableTls = ObjectReader.getBoolean(topoConf.get(Config.STORM_MESSAGING_NETTY_TLS_ENABLE), false);
        if (!enableTls) {
            return null;
        }

        boolean requireOpenSsl = ObjectReader.getBoolean(topoConf.get(Config.STORM_MESSAGING_NETTY_TLS_REQUIRE_OPEN_SSL), false);
        if (requireOpenSsl) {
            OpenSsl.ensureAvailability();
        }

        final Set<String> ciphers;
        if (topoConf.containsKey(Config.STORM_MESSAGING_NETTY_TLS_CIPHERS)) {
            ciphers = new HashSet<>();
            ciphers.addAll(ObjectReader.getStrings(topoConf.get(Config.STORM_MESSAGING_NETTY_TLS_CIPHERS)));
        } else {
            // TLSv1.3 ciphers available with OpenSSL from testing
            ciphers = Collections.unmodifiableSet(new LinkedHashSet<String>(
                    Arrays.asList("TLS_AES_256_GCM_SHA384", "TLS_CHACHA20_POLY1305_SHA256", "TLS_AES_128_GCM_SHA256")));
        }

        String keystorePath = ObjectReader.getString(topoConf.get(Config.STORM_MESSAGING_NETTY_TLS_KEYSTORE_PATH));
        String keystorePassword = ObjectReader.getString(topoConf.get(Config.STORM_MESSAGING_NETTY_TLS_KEYSTORE_PASSWORD));
        String truststorePath = ObjectReader.getString(topoConf.get(Config.STORM_MESSAGING_NETTY_TLS_TRUSTSTORE_PATH));
        String truststorePassword = ObjectReader.getString(topoConf.get(Config.STORM_MESSAGING_NETTY_TLS_TRUSTSTORE_PASSWORD));

        String protocols = ObjectReader.getString(topoConf.get(Config.STORM_MESSAGING_NETTY_TLS_SSL_PROTOCOLS), "TLSv1.3");

        SslContext sslContext = null;
        try {
            SslContextBuilder builder;
            ReloadableX509KeyManager reloadableX509KeyManager = new ReloadableX509KeyManager(keystorePath, keystorePassword);
            if (forServer) {
                builder = SslContextBuilder.forServer(reloadableX509KeyManager);
            } else {
                builder = SslContextBuilder.forClient();
                builder.keyManager(reloadableX509KeyManager);
            }

            if (forServer) {
                builder.clientAuth(ClientAuth.REQUIRE);
            }

            builder.ciphers(ciphers)
                    .trustManager(new ReloadableX509TrustManager(truststorePath, truststorePassword))
                    .startTls(false)
                    .protocols(protocols);

            if (requireOpenSsl) {
                builder.sslProvider(SslProvider.OPENSSL);
            }
            sslContext = builder.build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return sslContext;
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package org.apache.storm.daemon.common;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReloadableSslContextFactory extends SslContextFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ReloadableSslContextFactory.class);

    private boolean enableSslReload;
    private FileWatcher keyStoreWatcher;
    private FileWatcher trustStoreWatcher;

    public ReloadableSslContextFactory() {
        this(false);
    }

    public ReloadableSslContextFactory(boolean enableSslReload) {
        this.enableSslReload = enableSslReload;
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        if (enableSslReload) {
            LOG.info("Enabling reloading of SSL credentials without server restart");

            String keyStorePathStr = getKeyStorePath();
            if (keyStorePathStr != null) {
                Path keyStorePath = Paths.get(URI.create(keyStorePathStr).getPath());
                FileWatcher.Callback keyStoreWatcherCallback = () ->
                    ReloadableSslContextFactory.this.reload((scf) -> LOG.info("Reloading SslContextFactory due to keystore change"));
                keyStoreWatcher = new FileWatcher(keyStorePath, keyStoreWatcherCallback);
                keyStoreWatcher.start();
            } else {
                LOG.warn("KeyStore is null; it won't be watched/reloaded");
            }

            String trustStorePathStr = getTrustStorePath();
            if (trustStorePathStr != null) {
                Path trustStorePath = Paths.get(URI.create(trustStorePathStr).getPath());
                FileWatcher.Callback trustStoreWatcherCallback = () ->
                    ReloadableSslContextFactory.this.reload((scf) -> LOG.info("Reloading SslContextFactory due to truststore change"));
                trustStoreWatcher = new FileWatcher(trustStorePath, trustStoreWatcherCallback);
                trustStoreWatcher.start();
            } else {
                LOG.warn("TrustStore is null; it won't be watched/reloaded");
            }
        }
    }

    @Override
    protected void doStop() throws Exception {
        if (keyStoreWatcher != null) {
            keyStoreWatcher.stop();
        }
        if (trustStoreWatcher != null) {
            trustStoreWatcher.stop();
        }
        super.doStop();
    }
}
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE
 * file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the
 * Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of
 * the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed
 * on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions
 * and limitations under the License.
 */

package org.apache.storm.security.auth.tls;

import java.io.File;
import java.io.FileInputStream;
import java.net.Socket;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.X509KeyManager;
import org.apache.storm.daemon.common.FileWatcher;
import org.apache.storm.shade.io.netty.buffer.ByteBufAllocator;
import org.apache.storm.shade.io.netty.handler.ssl.ApplicationProtocolNegotiator;
import org.apache.storm.shade.io.netty.handler.ssl.SslContext;
import org.apache.storm.utils.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReloadableX509KeyManager implements X509KeyManager {

    private static final Logger LOG = LoggerFactory.getLogger(ReloadableX509KeyManager.class);
    private static final String KEYSTORE_RUNTIME_FORMAT = "JKS";
    private static final String CERTIFICATE_ENTRY_FORMAT = "X.509";
    private volatile X509KeyManager keyManager;

    public ReloadableX509KeyManager(String keystorePath, String keystorePassword) throws Exception {
        keyManager = createKeyManager(getKeyStore(keystorePath, keystorePassword), keystorePassword);
        FileWatcher.Callback keyStoreWatcherCallback = () -> {
            reloadCert(keystorePath, keystorePassword);
        };
        FileWatcher keyStoreWatcher = new FileWatcher(Paths.get(keystorePath), keyStoreWatcherCallback);
        keyStoreWatcher.start();
    }

    public X509KeyManager createKeyManager(KeyStore keystore, String keystorePassword) throws Exception {
        // Load the keystore

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keystore, keystorePassword.toCharArray());

        X509KeyManager x509KeyManager = null;
        for (KeyManager tm : kmf.getKeyManagers()) {
            if (tm instanceof X509KeyManager) {
                x509KeyManager = (X509KeyManager) tm;
                break;
            }
        }

        if (x509KeyManager == null) {
            throw new IllegalStateException("No x509KeyManager found");
        }

        LOG.info(" createKeyManager x509KeyManager {} ", x509KeyManager);

        return x509KeyManager;
    }

    @Override
    public String[] getClientAliases(String s, Principal[] principals) {
        return keyManager.getClientAliases(s, principals);
    }

    @Override
    public String chooseClientAlias(String[] strings, Principal[] principals, Socket socket) {
        return keyManager.chooseClientAlias(strings, principals, socket);
    }

    @Override
    public String[] getServerAliases(String s, Principal[] principals) {
        return keyManager.getServerAliases(s, principals);
    }

    @Override
    public String chooseServerAlias(String s, Principal[] principals, Socket socket) {
        return keyManager.chooseServerAlias(s, principals, socket);
    }

    @Override
    public X509Certificate[] getCertificateChain(String s) {
        return keyManager.getCertificateChain(s);
    }

    @Override
    public PrivateKey getPrivateKey(String s) {
        return keyManager.getPrivateKey(s);
    }

    private synchronized void reloadCert(String keystorePath, String keystorePassword) {
        try {
            LOG.info("Reloading KeyManager");
            keyManager = createKeyManager(getKeyStore(keystorePath, keystorePassword), keystorePassword);
            LOG.info("Reloading KeyManager - Done");
        } catch (Exception e) {
            LOG.error("Error reloading KeyManager. Setting keyManager to null", e);
            keyManager = null;
            //on error set keyManager to null
        }
    }

    public KeyStore getKeyStore(String keystorePath, String keystorePassword) throws Exception {
        String type = SecurityUtils.inferKeyStoreTypeFromPath(keystorePath);
        if (type == null) {
            type = KEYSTORE_RUNTIME_FORMAT;
        }
        LOG.info("Creating keystore with keystorePath {} type {} ", keystorePath, type);
        KeyStore keystore = KeyStore.getInstance(type);
        try (FileInputStream keystoreStream = new FileInputStream(keystorePath)) {
            keystore.load(keystoreStream, keystorePassword.toCharArray());
        }
        return keystore;
    }
}
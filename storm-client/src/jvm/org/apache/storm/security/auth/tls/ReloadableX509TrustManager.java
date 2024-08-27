/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.security.auth.tls;

import java.io.FileInputStream;
import java.net.Socket;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.storm.daemon.common.FileWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReloadableX509TrustManager extends X509ExtendedTrustManager {

    private static final Logger LOG = LoggerFactory.getLogger(ReloadableX509TrustManager.class);
    private static final String KEYSTORE_RUNTIME_FORMAT = "JKS";
    private static final String CERTIFICATE_ENTRY_FORMAT = "X.509";
    private static final X509Certificate[] EMPTY = new X509Certificate[0];
    private volatile X509ExtendedTrustManager trustManager;

    public ReloadableX509TrustManager(String trustStorePath, String trustStorePassword) throws Exception {
        trustManager = createTrustManager(trustStorePath, trustStorePassword);
        FileWatcher.Callback keyStoreWatcherCallback = () ->
                reloadCert(trustStorePath, trustStorePassword);
        FileWatcher keyStoreWatcher = new FileWatcher(Paths.get(trustStorePath), keyStoreWatcherCallback);
        keyStoreWatcher.start();
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        trustManager.checkClientTrusted(chain, authType);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws CertificateException {
        trustManager.checkClientTrusted(x509Certificates, s, socket);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {
        trustManager.checkClientTrusted(x509Certificates, s, sslEngine);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        trustManager.checkServerTrusted(chain, authType);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws CertificateException {
        trustManager.checkServerTrusted(x509Certificates, s, socket);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {
        trustManager.checkServerTrusted(x509Certificates, s, sslEngine);
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return trustManager.getAcceptedIssuers();
    }


    public X509ExtendedTrustManager createTrustManager(String trustStorePath, String keystorePassword) throws Exception {

        LOG.info(" createTrustManager trustStorePath {}", trustStorePath);
        KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
        try (FileInputStream keystoreStream = new FileInputStream(trustStorePath)) {
            keystore.load(keystoreStream, keystorePassword.toCharArray());
        }

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(keystore);

        X509ExtendedTrustManager x509TrustManager = null;
        for (javax.net.ssl.TrustManager tm : tmf.getTrustManagers()) {
            if (tm instanceof X509TrustManager) {
                x509TrustManager = (X509ExtendedTrustManager) tm;
                break;
            }
        }

        if (x509TrustManager == null) {
            throw new IllegalStateException("No X509TrustManager found");
        }
        return x509TrustManager;
    }

    private synchronized void reloadCert(String trustStorePath, String keystorePassword) {
        try {
            LOG.info("Reloading TrustManager");
            trustManager = createTrustManager(trustStorePath, keystorePassword);
            LOG.info("Reloading TrustManager - done");
        } catch (Exception e) {
            LOG.error("Error reloading TrustManager", e);
            throw new RuntimeException(e);
        }
    }

}
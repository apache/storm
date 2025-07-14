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

package org.apache.storm.security.auth;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.UUID;

import org.apache.storm.Config;
import org.apache.storm.thrift.protocol.TBinaryProtocol;
import org.apache.storm.thrift.protocol.TProtocol;
import org.apache.storm.thrift.transport.TSSLTransportFactory;
import org.apache.storm.thrift.transport.TSocket;
import org.apache.storm.thrift.transport.TTransport;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.SecurityUtils;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftClient implements AutoCloseable {
    protected TProtocol protocol;

    protected boolean retryForever = false;
    private TTransport transport;
    private String host;
    private Integer port;
    private Integer timeout;
    private Map<String, Object> conf;
    private ThriftConnectionType type;
    private String asUser;
    private static final Logger LOG = LoggerFactory.getLogger(ThriftClient.class);

    public ThriftClient(Map<String, Object> topoConf, ThriftConnectionType type, String host) {
        this(topoConf, type, host, null, null, null);
    }

    public ThriftClient(Map<String, Object> topoConf, ThriftConnectionType type, String host, Integer port, Integer timeout) {
        this(topoConf, type, host, port, timeout, null);
    }

    public ThriftClient(Map<String, Object> topoConf, ThriftConnectionType type, String host, Integer port, Integer timeout,
                        String asUser) {
        //create a socket with server
        if (host == null) {
            throw new IllegalArgumentException("host is not set");
        }

        if (port == null) {
            port = type.getPort(topoConf);
        }

        if (timeout == null) {
            timeout = type.getSocketTimeOut(topoConf);
        }

        if (port <= 0 && !type.isFake()) {
            throw new IllegalArgumentException("invalid port: " + port);
        }

        this.host = host;
        this.port = port;
        this.timeout = timeout;
        conf = topoConf;
        this.type = type;
        this.asUser = asUser;
        if (!type.isFake()) {
            reconnect();
        }
    }

    public synchronized TTransport transport() {
        return transport;
    }

    /**
     * Get the private key using BouncyCastle library from a PKCS#1 format file.
     * @return The Private Key
     * @throws IOException The IOException
     */
    protected PrivateKey getPrivateKey() throws IOException {
        try (FileReader fileReader = new FileReader(type.getClientKeyPath(conf))) {
            PEMParser pemParser = new PEMParser(fileReader);
            JcaPEMKeyConverter converter = new JcaPEMKeyConverter();
            Object key = pemParser.readObject();
            PrivateKeyInfo privateKeyInfo = ((PEMKeyPair) key).getPrivateKeyInfo();
            return converter.getPrivateKey(privateKeyInfo);
        }
    }

    /**
     * Function to create a keystore and return the keystore file.
     * @param keyStorePass The keystore password.
     * @return The keystore file
     * @throws CertificateException CertificateException
     * @throws KeyStoreException KeyStoreException
     * @throws IOException IOException
     * @throws NoSuchAlgorithmException NoSuchAlgorithmException
     */
    protected File getKeyStoreFile(String keyStorePass) throws CertificateException,
            KeyStoreException, IOException, NoSuchAlgorithmException {
        CertificateFactory fact = CertificateFactory.getInstance("X.509");
        KeyStore ks = KeyStore.getInstance("JKS");
        ks.load(null, keyStorePass.toCharArray());

        PrivateKey privateKey = getPrivateKey();
        // store the key and cert
        X509Certificate cer = null;
        try (FileInputStream fileInputStream = new FileInputStream(type.getClientCertPath(conf))) {
            cer = (X509Certificate) fact.generateCertificate(fileInputStream);
        }
        String certAlias = cer.getSubjectX500Principal().getName();
        Certificate[] chain = new Certificate[] {cer};
        ks.setKeyEntry(certAlias, privateKey, keyStorePass.toCharArray(), chain);
        // save the key store to a file
        File file = File.createTempFile("tempKeyStoreForStormAuth", null);
        file.deleteOnExit(); // mark it for deletion
        try (FileOutputStream fileOutputStream = new FileOutputStream(file.getAbsolutePath())) {
            ks.store(fileOutputStream, keyStorePass.toCharArray());
        }
        return file;
    }

    public synchronized void reconnect() {
        close();
        TSocket socket = null;
        File file = null;
        try {
            LOG.debug("Thrift client connecting to host={} port={}", host, port);
            if (type.isTlsEnabled()) {
                LOG.debug("Tls is enabled");
                TSSLTransportFactory.TSSLTransportParameters params =
                        new TSSLTransportFactory.TSSLTransportParameters();
                if (type.getClientTrustStorePath(conf) != null && type.getClientTrustStorePassword(conf) != null) {
                    // override the keyStoreType, there is no direct setter method
                    params.setTrustStore(type.getClientTrustStorePath(conf), type.getClientTrustStorePassword(conf), null,
                            SecurityUtils.inferKeyStoreTypeFromPath(type.getClientTrustStorePath(conf)));
                } else {
                    throw new IllegalArgumentException("The client truststore is not configured properly");
                }

                if (type.isClientAuthRequired(conf)) {
                    if (type.getClientKeyPath(conf) != null && type.getClientCertPath(conf) != null) {
                        String keyStorePass = UUID.randomUUID().toString();
                        file = getKeyStoreFile(keyStorePass);
                        params.setKeyStore(file.getAbsolutePath(), keyStorePass, null,
                                SecurityUtils.inferKeyStoreTypeFromPath(file.getAbsolutePath()));
                    } else if (type.getClientKeyStorePath(conf) != null && type.getClientKeyStorePassword(conf) != null) {
                        // override the keyStoreType, there is no direct setter method
                        params.setKeyStore(type.getClientKeyStorePath(conf), type.getClientKeyStorePassword(conf), null,
                                SecurityUtils.inferKeyStoreTypeFromPath(type.getClientKeyStorePath(conf)));
                    } else {
                        throw new IllegalArgumentException("The client credentials are not configured properly");
                    }
                }
                socket = TSSLTransportFactory.getClientSocket(host, port, 0, params);
            } else {
                socket = new TSocket(host, port);
            }
            if (timeout != null) {
                socket.setTimeout(timeout);
            }

            //construct a transport plugin
            ITransportPlugin transportPlugin = ClientAuthUtils.getTransportPlugin(type, conf);

            //TODO get this from type instead of hardcoding to Nimbus.
            //establish client-server transport via plugin
            //do retries if the connect fails
            TBackoffConnect connectionRetry
                = new TBackoffConnect(
                ObjectReader.getInt(conf.get(Config.STORM_NIMBUS_RETRY_TIMES)),
                ObjectReader.getInt(conf.get(Config.STORM_NIMBUS_RETRY_INTERVAL)),
                ObjectReader.getInt(conf.get(Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING)),
                    retryForever);
            transport = connectionRetry.doConnectWithRetry(transportPlugin, socket, host, asUser);
        } catch (Exception ex) {
            // close the socket, which releases connection if it has created any.
            if (socket != null) {
                try {
                    socket.close();
                } catch (Exception e) {
                    //ignore
                }
            }
            if (file != null) {
                file.delete();
            }
            throw new RuntimeException(ex);
        }
        protocol = null;
        if (transport != null) {
            protocol = new TBinaryProtocol(transport);
        }
    }

    @Override
    public synchronized void close() {
        if (transport != null) {
            transport.close();
            transport = null;
            protocol = null;
        }
    }
}

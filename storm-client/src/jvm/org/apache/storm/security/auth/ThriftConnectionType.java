/*
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

import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.generated.WorkerTokenServiceType;
import org.apache.storm.utils.ObjectReader;

/**
 * The purpose for which the Thrift server is created.
 */
public enum ThriftConnectionType {
    NIMBUS(Config.NIMBUS_THRIFT_TRANSPORT_PLUGIN, Config.NIMBUS_THRIFT_PORT, Config.NIMBUS_QUEUE_SIZE,
           Config.NIMBUS_THRIFT_THREADS, Config.NIMBUS_THRIFT_MAX_BUFFER_SIZE, Config.STORM_THRIFT_SOCKET_TIMEOUT_MS,
           WorkerTokenServiceType.NIMBUS, true),
    NIMBUS_TLS(Config.NIMBUS_THRIFT_TLS_TRANSPORT_PLUGIN, Config.NIMBUS_THRIFT_TLS_PORT, Config.NIMBUS_QUEUE_SIZE,
            Config.NIMBUS_THRIFT_TLS_THREADS, Config.NIMBUS_THRIFT_TLS_MAX_BUFFER_SIZE, Config.STORM_THRIFT_TLS_SOCKET_TIMEOUT_MS,
            false, null, false,
            true, Config.NIMBUS_THRIFT_TLS_CLIENT_AUTH_REQUIRED,
            Config.NIMBUS_THRIFT_TLS_SERVER_KEYSTORE_PATH, Config.NIMBUS_THRIFT_TLS_SERVER_KEYSTORE_PASSWORD,
            Config.NIMBUS_THRIFT_TLS_SERVER_TRUSTSTORE_PATH, Config.NIMBUS_THRIFT_TLS_SERVER_TRUSTSTORE_PASSWORD,
            Config.NIMBUS_THRIFT_TLS_CLIENT_KEYSTORE_PATH, Config.NIMBUS_THRIFT_TLS_CLIENT_KEYSTORE_PASSWORD,
            Config.NIMBUS_THRIFT_TLS_CLIENT_TRUSTSTORE_PATH, Config.NIMBUS_THRIFT_TLS_CLIENT_TRUSTSTORE_PASSWORD,
            Config.NIMBUS_THRIFT_TLS_CLIENT_KEY_PATH, Config.NIMBUS_THRIFT_TLS_CLIENT_CERT_PATH),
    SUPERVISOR(Config.SUPERVISOR_THRIFT_TRANSPORT_PLUGIN, Config.SUPERVISOR_THRIFT_PORT, Config.SUPERVISOR_QUEUE_SIZE,
               Config.SUPERVISOR_THRIFT_THREADS, Config.SUPERVISOR_THRIFT_MAX_BUFFER_SIZE,
               Config.SUPERVISOR_THRIFT_SOCKET_TIMEOUT_MS, WorkerTokenServiceType.SUPERVISOR, false),
    SUPERVISOR_TLS(Config.SUPERVISOR_THRIFT_TRANSPORT_PLUGIN, Config.SUPERVISOR_THRIFT_PORT, Config.SUPERVISOR_QUEUE_SIZE,
            Config.SUPERVISOR_THRIFT_THREADS, Config.SUPERVISOR_THRIFT_MAX_BUFFER_SIZE, Config.SUPERVISOR_THRIFT_SOCKET_TIMEOUT_MS,
            false, null, false,
            true, Config.SUPERVISOR_THRIFT_TLS_CLIENT_AUTH_REQUIRED,
            Config.SUPERVISOR_THRIFT_TLS_SERVER_KEYSTORE_PATH, Config.SUPERVISOR_THRIFT_TLS_SERVER_KEYSTORE_PASSWORD,
            Config.SUPERVISOR_THRIFT_TLS_SERVER_TRUSTSTORE_PATH, Config.SUPERVISOR_THRIFT_TLS_SERVER_TRUSTSTORE_PASSWORD,
            Config.SUPERVISOR_THRIFT_TLS_CLIENT_KEYSTORE_PATH, Config.SUPERVISOR_THRIFT_TLS_CLIENT_KEYSTORE_PASSWORD,
            Config.SUPERVISOR_THRIFT_TLS_CLIENT_TRUSTSTORE_PATH, Config.SUPERVISOR_THRIFT_TLS_CLIENT_TRUSTSTORE_PASSWORD,
            Config.SUPERVISOR_THRIFT_TLS_CLIENT_KEY_PATH, Config.SUPERVISOR_THRIFT_TLS_CLIENT_CERT_PATH),
    //A DRPC token only works for the invocations transport, not for the basic thrift transport.
    DRPC(Config.DRPC_THRIFT_TRANSPORT_PLUGIN, Config.DRPC_PORT, Config.DRPC_QUEUE_SIZE,
         Config.DRPC_WORKER_THREADS, Config.DRPC_MAX_BUFFER_SIZE, null, null, false),
    DRPC_INVOCATIONS(Config.DRPC_INVOCATIONS_THRIFT_TRANSPORT_PLUGIN, Config.DRPC_INVOCATIONS_PORT, null,
                     Config.DRPC_INVOCATIONS_THREADS, Config.DRPC_MAX_BUFFER_SIZE, null, WorkerTokenServiceType.DRPC, false),
    LOCAL_FAKE;

    private final String transConf;
    private final String portConf;
    private final String queueConf;
    private final String threadsConf;
    private final String buffConf;
    private final String socketTimeoutConf;
    private final boolean isFake;
    private final WorkerTokenServiceType wtType;
    private final boolean impersonationAllowed;
    private final boolean tlsEnabled;
    private final String clientAuthRequiredConf;
    private final String serverKeyStorePathConf;
    private final String serverKeyStorePasswordConf;
    private final String serverTrustStorePathConf;
    private final String serverTrustStorePasswordConf;
    private final String clientKeyStorePathConf;
    private final String clientKeyStorePasswordConf;
    private final String clientTrustStorePathConf;
    private final String clientTrustStorePasswordConf;
    private final String clientKeyPathConf;
    private final String clientCertPathConf;

    ThriftConnectionType() {
        this(null, null, null, null, null, null, true, null, false);
    }

    ThriftConnectionType(String transConf, String portConf, String queueConf,
                         String threadsConf, String buffConf, String socketTimeoutConf,
                         WorkerTokenServiceType wtType, boolean impersonationAllowed) {
        this(transConf, portConf, queueConf, threadsConf, buffConf, socketTimeoutConf, false, wtType, impersonationAllowed);
    }

    ThriftConnectionType(String transConf, String portConf, String queueConf,
                         String threadsConf, String buffConf, String socketTimeoutConf, boolean isFake,
                         WorkerTokenServiceType wtType, boolean impersonationAllowed) {
        this(transConf, portConf, queueConf, threadsConf, buffConf, socketTimeoutConf, isFake, wtType, impersonationAllowed,
                false, null, null, null, null, null, null, null, null, null, null, null);
    }

    ThriftConnectionType(String transConf, String portConf, String queueConf,
                         String threadsConf, String buffConf, String socketTimeoutConf, boolean isFake,
                         WorkerTokenServiceType wtType, boolean impersonationAllowed,
                         boolean tlsEnabled, String clientAuthRequiredConf,
                         String serverKeyStorePathConf, String serverKeyStorePasswordConf,
                         String serverTrustStorePathConf, String serverTrustStorePasswordConf,
                         String clientKeyStorePathConf, String clientKeyStorePasswordConf,
                         String clientTrustStorePathConf, String clientTrustStorePasswordConf,
                         String clientKeyPathConf, String clientCertPathConf) {
        this.transConf = transConf;
        this.portConf = portConf;
        this.queueConf = queueConf;
        this.threadsConf = threadsConf;
        this.buffConf = buffConf;
        this.socketTimeoutConf = socketTimeoutConf;
        this.isFake = isFake;
        this.wtType = wtType;
        this.impersonationAllowed = impersonationAllowed;

        this.tlsEnabled = tlsEnabled;
        this.clientAuthRequiredConf = clientAuthRequiredConf;
        this.serverKeyStorePathConf = serverKeyStorePathConf;
        this.serverKeyStorePasswordConf = serverKeyStorePasswordConf;
        this.serverTrustStorePathConf = serverTrustStorePathConf;
        this.serverTrustStorePasswordConf = serverTrustStorePasswordConf;
        this.clientKeyStorePathConf = clientKeyStorePathConf;
        this.clientKeyStorePasswordConf = clientKeyStorePasswordConf;
        this.clientTrustStorePathConf = clientTrustStorePathConf;
        this.clientTrustStorePasswordConf = clientTrustStorePasswordConf;
        this.clientKeyPathConf = clientKeyPathConf;
        this.clientCertPathConf = clientCertPathConf;
    }

    public boolean isFake() {
        return isFake;
    }

    public String getTransportPlugin(Map<String, Object> conf) {
        String ret = (String) conf.get(transConf);
        if (ret == null) {
            ret = (String) conf.get(Config.STORM_THRIFT_TRANSPORT_PLUGIN);
        }
        return ret;
    }

    public int getPort(Map<String, Object> conf) {
        if (isFake) {
            return -1;
        }
        return ObjectReader.getInt(conf.get(portConf));
    }

    public Integer getQueueSize(Map<String, Object> conf) {
        if (queueConf == null) {
            return null;
        }
        return (Integer) conf.get(queueConf);
    }

    public int getNumThreads(Map<String, Object> conf) {
        if (isFake) {
            return 1;
        }
        return ObjectReader.getInt(conf.get(threadsConf));
    }

    public int getMaxBufferSize(Map<String, Object> conf) {
        if (isFake) {
            return 1;
        }
        return ObjectReader.getInt(conf.get(buffConf));
    }

    public Integer getSocketTimeOut(Map<String, Object> conf) {
        if (socketTimeoutConf == null) {
            return null;
        }
        return ObjectReader.getInt(conf.get(socketTimeoutConf));
    }

    /**
     * Get the corresponding worker token type for this thrift connection.
     */
    public WorkerTokenServiceType getWtType() {
        return wtType;
    }

    /**
     * Check if SASL impersonation is allowed for this transport type.
     *
     * @return true if it is else false.
     */
    public boolean isImpersonationAllowed() {
        return impersonationAllowed;
    }

    public boolean isTlsEnabled() {
        return tlsEnabled;
    }

    public boolean isClientAuthRequired(Map<String, Object> conf) {
        boolean clientAuthRequired = false;
        if (tlsEnabled) {
            clientAuthRequired = ObjectReader.getBoolean(conf.get(clientAuthRequiredConf), false);
        }
        return clientAuthRequired;
    }

    public String getServerKeyStorePath(Map<String, Object> conf) {
        return ObjectReader.getString(conf.get(serverKeyStorePathConf), null);
    }

    public String getServerKeyStorePassword(Map<String, Object> conf) {
        return ObjectReader.getString(conf.get(serverKeyStorePasswordConf), null);
    }

    public String getServerTrustStorePath(Map<String, Object> conf) {
        return ObjectReader.getString(conf.get(serverTrustStorePathConf), null);
    }

    public String getServerTrustStorePassword(Map<String, Object> conf) {
        return ObjectReader.getString(conf.get(serverTrustStorePasswordConf), null);
    }

    public String getClientKeyStorePath(Map<String, Object> conf) {
        return ObjectReader.getString(conf.get(clientKeyStorePathConf), null);
    }

    public String getClientKeyStorePassword(Map<String, Object> conf) {
        return ObjectReader.getString(conf.get(clientKeyStorePasswordConf), null);
    }

    public String getClientTrustStorePath(Map<String, Object> conf) {
        return ObjectReader.getString(conf.get(clientTrustStorePathConf), null);
    }

    public String getClientTrustStorePassword(Map<String, Object> conf) {
        return ObjectReader.getString(conf.get(clientTrustStorePasswordConf), null);
    }

    public String getClientKeyPath(Map<String, Object> conf) {
        return ObjectReader.getString(conf.get(clientKeyPathConf), null);
    }

    public String getClientCertPath(Map<String, Object> conf) {
        return ObjectReader.getString(conf.get(clientCertPathConf), null);
    }

}

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
package org.apache.storm.security.auth;

import org.apache.storm.utils.ObjectReader;
import org.apache.storm.Config;

import java.util.Map;

/**
 * The purpose for which the Thrift server is created.
 */
public enum ThriftConnectionType {
    NIMBUS(Config.NIMBUS_THRIFT_TRANSPORT_PLUGIN, Config.NIMBUS_THRIFT_PORT, Config.NIMBUS_QUEUE_SIZE,
         Config.NIMBUS_THRIFT_THREADS, Config.NIMBUS_THRIFT_MAX_BUFFER_SIZE, Config.STORM_THRIFT_SOCKET_TIMEOUT_MS),
    DRPC(Config.DRPC_THRIFT_TRANSPORT_PLUGIN, Config.DRPC_PORT, Config.DRPC_QUEUE_SIZE,
         Config.DRPC_WORKER_THREADS, Config.DRPC_MAX_BUFFER_SIZE, null),
    DRPC_INVOCATIONS(Config.DRPC_INVOCATIONS_THRIFT_TRANSPORT_PLUGIN, Config.DRPC_INVOCATIONS_PORT, null,
         Config.DRPC_INVOCATIONS_THREADS, Config.DRPC_MAX_BUFFER_SIZE, null),
    LOCAL_FAKE;

    private final String _transConf;
    private final String _portConf;
    private final String _qConf;
    private final String _threadsConf;
    private final String _buffConf;
    private final String _socketTimeoutConf;
    private final boolean _isFake;

    ThriftConnectionType() {
        this(null, null, null, null, null, null, true);
    }
    
    ThriftConnectionType(String transConf, String portConf, String qConf,
            String threadsConf, String buffConf, String socketTimeoutConf) {
        this(transConf, portConf, qConf, threadsConf, buffConf, socketTimeoutConf, false);
    }
    
    ThriftConnectionType(String transConf, String portConf, String qConf,
                         String threadsConf, String buffConf, String socketTimeoutConf, boolean isFake) {
        _transConf = transConf;
        _portConf = portConf;
        _qConf = qConf;
        _threadsConf = threadsConf;
        _buffConf = buffConf;
        _socketTimeoutConf = socketTimeoutConf;
        _isFake = isFake;
    }

    public boolean isFake() {
        return _isFake;
    }
    
    public String getTransportPlugin(Map<String, Object> conf) {
        String ret = (String)conf.get(_transConf);
        if (ret == null) {
            ret = (String)conf.get(Config.STORM_THRIFT_TRANSPORT_PLUGIN);
        }
        return ret;
    }

    public int getPort(Map<String, Object> conf) {
        if (_isFake) {
            return -1;
        }
        return ObjectReader.getInt(conf.get(_portConf));
    }

    public Integer getQueueSize(Map<String, Object> conf) {
        if (_qConf == null) {
            return null;
        }
        return (Integer)conf.get(_qConf);
    }

    public int getNumThreads(Map<String, Object> conf) {
        if (_isFake) {
            return 1;
        }
        return ObjectReader.getInt(conf.get(_threadsConf));
    }

    public int getMaxBufferSize(Map<String, Object> conf) {
        if (_isFake) {
            return 1;
        }
        return ObjectReader.getInt(conf.get(_buffConf));
    }

    public Integer getSocketTimeOut(Map<String, Object> conf) {
        if (_socketTimeoutConf == null) {
            return null;
        }
        return ObjectReader.getInt(conf.get(_socketTimeoutConf));
    }
}

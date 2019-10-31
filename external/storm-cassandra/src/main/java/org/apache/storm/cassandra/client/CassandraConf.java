/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.cassandra.client;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy.Builder;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.LoggingRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.google.common.base.Objects;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Utils;

/**
 * Configuration used by cassandra storm components.
 */
public class CassandraConf implements Serializable {

    public static final String CASSANDRA_USERNAME = "cassandra.username";
    public static final String CASSANDRA_PASSWORD = "cassandra.password";
    public static final String CASSANDRA_KEYSPACE = "cassandra.keyspace";
    public static final String CASSANDRA_CONSISTENCY_LEVEL = "cassandra.output.consistencyLevel";
    public static final String CASSANDRA_NODES = "cassandra.nodes";
    public static final String CASSANDRA_PORT = "cassandra.port";
    public static final String CASSANDRA_BATCH_SIZE_ROWS = "cassandra.batch.size.rows";
    public static final String CASSANDRA_RETRY_POLICY = "cassandra.retryPolicy";
    public static final String CASSANDRA_RECONNECT_POLICY_BASE_MS = "cassandra.reconnectionPolicy.baseDelayMs";
    public static final String CASSANDRA_RECONNECT_POLICY_MAX_MS = "cassandra.reconnectionPolicy.maxDelayMs";
    public static final String CASSANDRA_POOL_MAX_SIZE = "cassandra.pool.max.size";
    public static final String CASSANDRA_LOAD_BALANCING_POLICY = "cassandra.loadBalancingPolicy";
    public static final String CASSANDRA_DATACENTER_NAME = "cassandra.datacenter.name";
    public static final String CASSANDRA_MAX_REQUESTS_PER_CON_LOCAL = "cassandra.max.requests.per.con.local";
    public static final String CASSANDRA_MAX_REQUESTS_PER_CON_REMOTE = "cassandra.max.requests.per.con.remote";
    public static final String CASSANDRA_HEARTBEAT_INTERVAL_SEC = "cassandra.heartbeat.interval.sec";
    public static final String CASSANDRA_IDLE_TIMEOUT_SEC = "cassandra.idle.timeout.sec";
    public static final String CASSANDRA_SOCKET_READ_TIMEOUT_MS = "cassandra.socket.read.timeout.millis";
    public static final String CASSANDRA_SOCKET_CONNECT_TIMEOUT_MS = "cassandra.socket.connect.timeout.millis";

    /**
     * The authorized cassandra username.
     */
    private String username;
    /**
     * The authorized cassandra password.
     */
    private String password;
    /**
     * The cassandra keyspace.
     */
    private String keyspace;
    /**
     * List of contacts nodes.
     */
    private String[] nodes = { "localhost" };

    /**
     * The port used to connect to nodes.
     */
    private int port = 9092;

    /**
     * Consistency level used to write statements.
     */
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;
    /**
     * The maximal numbers of rows per batch.
     */
    private int batchSizeRows = 100;

    /**
     * The retry policy to use for the new cluster.
     */
    private String retryPolicyName;

    /**
     * The base delay in milliseconds to use for the reconnection policy.
     */
    private long reconnectionPolicyBaseMs;

    /**
     * The maximum delay to wait between two attempts.
     */
    private long reconnectionPolicyMaxMs;

    /**
     * The maximum queue for connection pool.
     */
    private int poolMaxQueueSize;

    private String loadBalancingPolicyName;

    private String datacenterName;

    private int maxRequestPerConnectionLocal;

    private int maxRequestPerConnectionRemote;

    private int heartbeatIntervalSeconds;

    private int idleTimeoutSeconds;

    /**
     * The timeout for read for socket options.
     */
    private long socketReadTimeoutMillis;

    /**
     * The timeout for connect for socket options.
     */
    private long socketConnectTimeoutMillis;

    /**
     * Creates a new {@link CassandraConf} instance.
     */
    public CassandraConf() {
        super();
    }

    /**
     * Creates a new {@link CassandraConf} instance.
     *
     * @param conf The storm configuration.
     */
    public CassandraConf(Map<String, Object> conf) {

        this.username = (String) Utils.get(conf, CASSANDRA_USERNAME, null);
        this.password = (String) Utils.get(conf, CASSANDRA_PASSWORD, null);
        this.keyspace = get(conf, CASSANDRA_KEYSPACE);
        this.consistencyLevel =
            ConsistencyLevel.valueOf((String) Utils.get(conf, CASSANDRA_CONSISTENCY_LEVEL, ConsistencyLevel.ONE.name()));
        this.nodes = ((String) Utils.get(conf, CASSANDRA_NODES, "localhost")).split(",");
        this.batchSizeRows = ObjectReader.getInt(conf.get(CASSANDRA_BATCH_SIZE_ROWS), 100);
        this.port = ObjectReader.getInt(conf.get(CASSANDRA_PORT), 9042);
        this.retryPolicyName = (String) Utils.get(conf, CASSANDRA_RETRY_POLICY, DefaultRetryPolicy.class.getSimpleName());
        this.reconnectionPolicyBaseMs = getLong(conf.get(CASSANDRA_RECONNECT_POLICY_BASE_MS), 100L);
        this.reconnectionPolicyMaxMs = getLong(conf.get(CASSANDRA_RECONNECT_POLICY_MAX_MS), TimeUnit.MINUTES.toMillis(1));
        this.poolMaxQueueSize = getInt(conf.get(CASSANDRA_POOL_MAX_SIZE), 256);
        this.loadBalancingPolicyName = (String) Utils.get(conf, CASSANDRA_LOAD_BALANCING_POLICY, TokenAwarePolicy.class.getSimpleName());
        this.datacenterName = (String) Utils.get(conf, CASSANDRA_DATACENTER_NAME, null);
        this.maxRequestPerConnectionLocal = getInt(conf.get(CASSANDRA_MAX_REQUESTS_PER_CON_LOCAL), 1024);
        this.maxRequestPerConnectionRemote = getInt(conf.get(CASSANDRA_MAX_REQUESTS_PER_CON_REMOTE), 256);
        this.heartbeatIntervalSeconds = getInt(conf.get(CASSANDRA_HEARTBEAT_INTERVAL_SEC), 30);
        this.idleTimeoutSeconds = getInt(conf.get(CASSANDRA_IDLE_TIMEOUT_SEC), 60);
        this.socketReadTimeoutMillis =
            getLong(conf.get(CASSANDRA_SOCKET_READ_TIMEOUT_MS), (long) SocketOptions.DEFAULT_READ_TIMEOUT_MILLIS);
        this.socketConnectTimeoutMillis =
            getLong(conf.get(CASSANDRA_SOCKET_CONNECT_TIMEOUT_MS), (long) SocketOptions.DEFAULT_CONNECT_TIMEOUT_MILLIS);
    }

    public static Integer getInt(Object o, Integer defaultValue) {
        if (null == o) {
            return defaultValue;
        }
        if (o instanceof Number) {
            return ((Number) o).intValue();
        } else if (o instanceof String) {
            return Integer.parseInt((String) o);
        }
        throw new IllegalArgumentException("Don't know how to convert " + o + " to int");
    }

    public static Long getLong(Object o, Long defaultValue) {
        if (null == o) {
            return defaultValue;
        }
        if (o instanceof Number) {
            return ((Number) o).longValue();
        } else if (o instanceof String) {
            return Long.parseLong((String) o);
        }
        throw new IllegalArgumentException("Don't know how to convert " + o + " to long");
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public String[] getNodes() {
        return nodes;
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public int getBatchSizeRows() {
        return batchSizeRows;
    }

    public int getPort() {
        return this.port;
    }

    public long getReconnectionPolicyBaseMs() {
        return reconnectionPolicyBaseMs;
    }

    public long getReconnectionPolicyMaxMs() {
        return reconnectionPolicyMaxMs;
    }

    public RetryPolicy getRetryPolicy() {
        if (this.retryPolicyName.equals(DowngradingConsistencyRetryPolicy.class.getSimpleName())) {
            return new LoggingRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE);
        }
        if (this.retryPolicyName.equals(FallthroughRetryPolicy.class.getSimpleName())) {
            return FallthroughRetryPolicy.INSTANCE;
        }
        if (this.retryPolicyName.equals(DefaultRetryPolicy.class.getSimpleName())) {
            return DefaultRetryPolicy.INSTANCE;
        }
        throw new IllegalArgumentException("Unknown cassandra retry policy " + this.retryPolicyName);
    }

    public LoadBalancingPolicy getLoadBalancingPolicy() {
        if (this.loadBalancingPolicyName.equals(TokenAwarePolicy.class.getSimpleName())) {
            return new TokenAwarePolicy(new RoundRobinPolicy());
        }
        if (this.loadBalancingPolicyName.equals(DCAwareRoundRobinPolicy.class.getSimpleName())) {
            Builder builder = DCAwareRoundRobinPolicy.builder();
            if (StringUtils.isNotBlank(datacenterName)) {
                builder = builder.withLocalDc(this.datacenterName);
            }
            return new TokenAwarePolicy(builder.build());
        }
        throw new IllegalArgumentException("Unknown cassandra load balancing policy " + this.loadBalancingPolicyName);
    }

    public int getPoolMaxQueueSize() {
        return poolMaxQueueSize;
    }

    public String getDatacenterName() {
        return datacenterName;
    }

    public int getMaxRequestPerConnectionLocal() {
        return maxRequestPerConnectionLocal;
    }

    public int getMaxRequestPerConnectionRemote() {
        return maxRequestPerConnectionRemote;
    }

    public int getHeartbeatIntervalSeconds() {
        return heartbeatIntervalSeconds;
    }

    public int getIdleTimeoutSeconds() {
        return idleTimeoutSeconds;
    }

    public long getSocketReadTimeoutMillis() {
        return socketReadTimeoutMillis;
    }

    public long getSocketConnectTimeoutMillis() {
        return socketConnectTimeoutMillis;
    }

    private <T> T get(Map<String, Object> conf, String key) {
        Object o = conf.get(key);
        if (o == null) {
            throw new IllegalArgumentException("No '" + key + "' value found in configuration!");
        }
        return (T) o;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                      .add("username", username)
                      .add("password", password)
                      .add("keyspace", keyspace)
                      .add("nodes", nodes)
                      .add("port", port)
                      .add("consistencyLevel", consistencyLevel)
                      .add("batchSizeRows", batchSizeRows)
                      .add("retryPolicyName", retryPolicyName)
                      .add("reconnectionPolicyBaseMs", reconnectionPolicyBaseMs)
                      .add("reconnectionPolicyMaxMs", reconnectionPolicyMaxMs)
                      .add("poolMaxQueueSize", poolMaxQueueSize)
                      .add("datacenterName", datacenterName)
                      .add("maxRequestPerConnectionLocal", maxRequestPerConnectionLocal)
                      .add("maxRequestPerConnectionRemote", maxRequestPerConnectionRemote)
                      .add("heartbeatIntervalSeconds", heartbeatIntervalSeconds)
                      .add("idleTimeoutSeconds", idleTimeoutSeconds)
                      .add("socketReadTimeoutMillis", socketReadTimeoutMillis)
                      .toString();
    }
}

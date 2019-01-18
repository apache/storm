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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.cassandra.context.BaseBeanFactory;

/**
 * Default interface to build cassandra Cluster from the a Storm Topology configuration.
 */
public class ClusterFactory extends BaseBeanFactory<Cluster> {

    /**
     * Creates a new Cluster based on the specified configuration.
     * @param topoConf the storm configuration.
     * @return a new a new {@link com.datastax.driver.core.Cluster} instance.
     */
    @Override
    protected Cluster make(Map<String, Object> topoConf) {
        CassandraConf cassandraConf = new CassandraConf(topoConf);

        Cluster.Builder cluster = Cluster.builder()
                                         .withoutJMXReporting()
                                         .withoutMetrics()
                                         .addContactPoints(cassandraConf.getNodes())
                                         .withPort(cassandraConf.getPort())
                                         .withRetryPolicy(cassandraConf.getRetryPolicy())
                                         .withReconnectionPolicy(new ExponentialReconnectionPolicy(
                                             cassandraConf.getReconnectionPolicyBaseMs(),
                                             cassandraConf.getReconnectionPolicyMaxMs()))
                                         .withLoadBalancingPolicy(cassandraConf.getLoadBalancingPolicy());
        cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis((int) cassandraConf.getSocketReadTimeoutMillis());
        cluster.getConfiguration().getSocketOptions().setConnectTimeoutMillis((int) cassandraConf.getSocketConnectTimeoutMillis());

        final String username = cassandraConf.getUsername();
        final String password = cassandraConf.getPassword();

        if (StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password)) {
            cluster.withAuthProvider(new PlainTextAuthProvider(username, password));
        }

        QueryOptions options = new QueryOptions()
            .setConsistencyLevel(cassandraConf.getConsistencyLevel());
        cluster.withQueryOptions(options);

        PoolingOptions poolOps = new PoolingOptions();
        poolOps.setMaxQueueSize(cassandraConf.getPoolMaxQueueSize());
        poolOps.setHeartbeatIntervalSeconds(cassandraConf.getHeartbeatIntervalSeconds());
        poolOps.setIdleTimeoutSeconds(cassandraConf.getIdleTimeoutSeconds());
        poolOps.setMaxRequestsPerConnection(HostDistance.LOCAL, cassandraConf.getMaxRequestPerConnectionLocal());
        poolOps.setMaxRequestsPerConnection(HostDistance.REMOTE, cassandraConf.getMaxRequestPerConnectionRemote());
        cluster.withPoolingOptions(poolOps);

        return cluster.build();
    }
}

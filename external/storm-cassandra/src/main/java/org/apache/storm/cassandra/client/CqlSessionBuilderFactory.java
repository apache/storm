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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.session.Session;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.storm.cassandra.context.BaseBeanFactory;

/**
 * Default interface to build cassandra Cluster from the a Storm Topology configuration.
 */
public class CqlSessionBuilderFactory extends BaseBeanFactory<CqlSessionBuilder> {

    /**
     * Creates a new Cluster based on the specified configuration.
     * @param topoConf the storm configuration.
     * @return a new a new {@link Session} instance.
     */
    @Override
    protected CqlSessionBuilder make(Map<String, Object> topoConf) {
        CassandraConf cassandraConf = new CassandraConf(topoConf);

        Collection<InetSocketAddress> contactPoints = new ArrayList<>();
        {
            // Cluster Hosts
            for (String host : cassandraConf.getNodes()) {
                InetAddress inetAddress;
                try {
                    inetAddress = InetAddress.getByName(host);
                } catch (Exception ex) {
                    System.err.printf("Cannot resolve address for host \"%s\"%n", host);
                    ex.printStackTrace();
                    continue;
                }
                InetSocketAddress inetSocketAddress = new InetSocketAddress(inetAddress, cassandraConf.getPort());
                contactPoints.add(inetSocketAddress);
            }
        }
        CqlSessionBuilder cqlSessionBuilder = CqlSession.builder().addContactPoints(contactPoints);
        //.withRetryPolicy(cassandraConf.getRetryPolicy())
        //.withLoadBalancingPolicy(cassandraConf.getLoadBalancingPolicy());

        {
            // Cluster Authentication
            final String username = cassandraConf.getUsername();
            final String password = cassandraConf.getPassword();

            if (StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password)) {
                cqlSessionBuilder.withAuthCredentials(username, password);
            }
        }

        //QueryOptions options = new QueryOptions()
        //    .setConsistencyLevel(cassandraConf.getConsistencyLevel());
        //cqlSessionBuilder.withQueryOptions(options);

        //PoolingOptions poolOps = new PoolingOptions();
        //poolOps.setMaxQueueSize(cassandraConf.getPoolMaxQueueSize());
        //poolOps.setHeartbeatIntervalSeconds(cassandraConf.getHeartbeatIntervalSeconds());
        //poolOps.setIdleTimeoutSeconds(cassandraConf.getIdleTimeoutSeconds());
        //poolOps.setMaxRequestsPerConnection(HostDistance.LOCAL, cassandraConf.getMaxRequestPerConnectionLocal());
        //poolOps.setMaxRequestsPerConnection(HostDistance.REMOTE, cassandraConf.getMaxRequestPerConnectionRemote());
        //cqlSessionBuilder.withPoolingOptions(poolOps);

        return cqlSessionBuilder;
    }
}

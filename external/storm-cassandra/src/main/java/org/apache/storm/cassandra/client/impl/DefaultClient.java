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

package org.apache.storm.cassandra.client.impl;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.storm.cassandra.client.SimpleClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple class to wrap cassandra {@link CqlSession} instance.
 */
public class DefaultClient implements SimpleClient, Closeable, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultClient.class);

    private String keyspace;
    private CqlSessionBuilder cqlSessionBuilder;

    private CqlSession cqlSession;

    /**
     * Create a new {@link DefaultClient} instance.
     *
     * @param cqlSessionBuilder a cassandra session builder.
     */
    public DefaultClient(CqlSessionBuilder cqlSessionBuilder, String keyspace) {
        Preconditions.checkNotNull(cqlSessionBuilder, "CqlSessionBuilder cannot be 'null");
        this.cqlSessionBuilder = cqlSessionBuilder;
        this.keyspace = keyspace;

    }

    public Set<Node> getAllNodes() {
        Metadata metadata = getMetadata();
        return new HashSet<>(metadata.getNodes().values());
    }

    public Metadata getMetadata() {
        return cqlSession.getMetadata();
    }


    private String getExecutorName() {
        Thread thread = Thread.currentThread();
        return thread.getName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized CqlSession connect() {
        if (isDisconnected()) {
            LOG.info("Connecting to cluster: {}", cqlSession.getName());
            for (Node node : getAllNodes()) {
                LOG.info("Datacenter: {}; Host: {}; Rack: {}", node.getDatacenter(),
                        node.getListenAddress().get().getHostName(), node.getRack());
            }

            LOG.info("Connect to cluster using keyspace %s", keyspace);
            cqlSession = cqlSessionBuilder.build();
        } else {
            LOG.warn("{} - Already connected to cluster: {}", getExecutorName(), cqlSession.getName());
        }

        if (cqlSession.isClosed()) {
            LOG.warn("Session has been closed - create new one!");
            this.cqlSession = cqlSessionBuilder.build();
        }
        return cqlSession;
    }

    /**
     * Checks whether the client is already connected to the cluster.
     */
    protected boolean isDisconnected() {
        return cqlSession == null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        if (!cqlSession.isClosed()) {
            LOG.info("Try to close connection to cluster: {}", cqlSession.getMetadata().getClusterName());
            cqlSession.close();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClose() {
        return this.cqlSession.isClosed();
    }
}

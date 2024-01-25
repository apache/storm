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

package org.apache.storm.utils;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import org.apache.storm.shade.org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperServerCnxnFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperServerCnxnFactory.class);
    int port;
    NIOServerCnxnFactory factory;

    public ZookeeperServerCnxnFactory(int port, int maxClientCnxns) {
        //port range
        int max;
        if (port <= 0) {
            this.port = 2000;
            max = 65535;
        } else {
            this.port = port;
            max = port;
        }

        factory = new NIOServerCnxnFactory();

        //look for available port
        for (; this.port <= max; this.port++) {
            try {
                factory.configure(new InetSocketAddress(this.port), maxClientCnxns);
                LOG.debug("Zookeeper server successfully binded at port " + this.port);
                break;
            } catch (BindException e1) {
                //ignore
            } catch (IOException e2) {
                this.port = 0;
                factory = null;
                e2.printStackTrace();
                throw new RuntimeException(e2.getMessage());
            }
        }

        if (this.port > max) {
            this.port = 0;
            factory = null;
            LOG.error("Failed to find a port for Zookeeper");
            throw new RuntimeException("No port is available to launch an inprocess zookeeper.");
        }
    }

    public int port() {
        return port;
    }

    public NIOServerCnxnFactory factory() {
        return factory;
    }
}

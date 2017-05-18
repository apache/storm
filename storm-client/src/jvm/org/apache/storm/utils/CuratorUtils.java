/*
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

package org.apache.storm.utils;

import org.apache.storm.Config;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.ensemble.exhibitor.DefaultExhibitorRestClient;
import org.apache.curator.ensemble.exhibitor.ExhibitorEnsembleProvider;
import org.apache.curator.ensemble.exhibitor.Exhibitors;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CuratorUtils {
    public static final Logger LOG = LoggerFactory.getLogger(CuratorUtils.class);

    public static CuratorFramework newCurator(Map<String, Object> conf, List<String> servers, Object port, String root) {
        return newCurator(conf, servers, port, root, null);
    }

    public static CuratorFramework newCurator(Map<String, Object> conf, List<String> servers, Object port, ZookeeperAuthInfo auth) {
        return newCurator(conf, servers, port, "", auth);
    }

    public static CuratorFramework newCurator(Map<String, Object> conf, List<String> servers, Object port, String root, ZookeeperAuthInfo auth) {
        List<String> serverPorts = new ArrayList<String>();
        for (String zkServer : servers) {
            serverPorts.add(zkServer + ":" + ObjectReader.getInt(port));
        }
        String zkStr = StringUtils.join(serverPorts, ",") + root;
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();

        setupBuilder(builder, zkStr, conf, auth);

        return builder.build();
    }

    protected static void setupBuilder(CuratorFrameworkFactory.Builder builder, final String zkStr, Map<String, Object> conf, ZookeeperAuthInfo auth)
    {
        List<String> exhibitorServers = ObjectReader.getStrings(conf.get(Config.STORM_EXHIBITOR_SERVERS));
        if (!exhibitorServers.isEmpty()) {
            // use exhibitor servers
            builder.ensembleProvider(new ExhibitorEnsembleProvider(
                new Exhibitors(exhibitorServers, ObjectReader.getInt(conf.get(Config.STORM_EXHIBITOR_PORT)),
                    new Exhibitors.BackupConnectionStringProvider() {
                        @Override
                        public String getBackupConnectionString() throws Exception {
                            // use zk servers as backup if they exist
                            return zkStr;
                        }}),
                new DefaultExhibitorRestClient(),
                ObjectReader.getString(conf.get(Config.STORM_EXHIBITOR_URIPATH)),
                ObjectReader.getInt(conf.get(Config.STORM_EXHIBITOR_POLL)),
                new StormBoundedExponentialBackoffRetry(
                    ObjectReader.getInt(conf.get(Config.STORM_EXHIBITOR_RETRY_INTERVAL)),
                    ObjectReader.getInt(conf.get(Config.STORM_EXHIBITOR_RETRY_INTERVAL_CEILING)),
                    ObjectReader.getInt(conf.get(Config.STORM_EXHIBITOR_RETRY_TIMES)))));
        } else {
            builder.connectString(zkStr);
        }
        builder
            .connectionTimeoutMs(ObjectReader.getInt(conf.get(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT)))
            .sessionTimeoutMs(ObjectReader.getInt(conf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT)))
            .retryPolicy(new StormBoundedExponentialBackoffRetry(
                    ObjectReader.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL)),
                    ObjectReader.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL_CEILING)),
                    ObjectReader.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_TIMES))));

        if (auth != null && auth.scheme != null && auth.payload != null) {
            builder.authorization(auth.scheme, auth.payload);
        }
    }

    public static void testSetupBuilder(CuratorFrameworkFactory.Builder
                                                builder, String zkStr, Map<String, Object> conf, ZookeeperAuthInfo auth)
    {
        setupBuilder(builder, zkStr, conf, auth);
    }

    public static CuratorFramework newCuratorStarted(Map<String, Object> conf, List<String> servers, Object port, String root, ZookeeperAuthInfo auth) {
        CuratorFramework ret = newCurator(conf, servers, port, root, auth);
        LOG.info("Starting Utils Curator...");
        ret.start();
        return ret;
    }

    public static CuratorFramework newCuratorStarted(Map<String, Object> conf, List<String> servers, Object port, ZookeeperAuthInfo auth) {
        CuratorFramework ret = newCurator(conf, servers, port, auth);
        LOG.info("Starting Utils Curator (2)...");
        ret.start();
        return ret;
    }
}

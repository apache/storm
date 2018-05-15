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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.shade.org.apache.curator.ensemble.exhibitor.ExhibitorEnsembleProvider;
import org.apache.storm.shade.org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.storm.shade.org.apache.curator.framework.AuthInfo;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFrameworkFactory;
import org.junit.Assert;
import org.junit.Test;

public class CuratorUtilsTest {
    @Test
    public void newCuratorUsesExponentialBackoffTest() throws InterruptedException {
        final int expectedInterval = 2400;
        final int expectedRetries = 10;
        final int expectedCeiling = 3000;

        Map<String, Object> config = Utils.readDefaultConfig();
        config.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, expectedInterval);
        config.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, expectedRetries);
        config.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL_CEILING, expectedCeiling);

        CuratorFramework curator = CuratorUtils.newCurator(config, Arrays.asList("bogus_server"), 42, "",
                                                           DaemonType.WORKER.getDefaultZkAcls(config));
        StormBoundedExponentialBackoffRetry policy =
            (StormBoundedExponentialBackoffRetry) curator.getZookeeperClient().getRetryPolicy();
        Assert.assertEquals(policy.getBaseSleepTimeMs(), expectedInterval);
        Assert.assertEquals(policy.getN(), expectedRetries);
        Assert.assertEquals(policy.getSleepTimeMs(10, 0), expectedCeiling);
    }

    @Test
    public void givenExhibitorServersBuilderUsesExhibitorProviderTest() {
        CuratorFrameworkFactory.Builder builder = setupBuilder(true /*with exhibitor*/);
        Assert.assertEquals(builder.getEnsembleProvider().getConnectionString(), "");
        Assert.assertEquals(builder.getEnsembleProvider().getClass(), ExhibitorEnsembleProvider.class);
    }

    @Test
    public void givenNoExhibitorServersBuilderUsesFixedProviderTest() {
        CuratorFrameworkFactory.Builder builder = setupBuilder(false /*without exhibitor*/);
        Assert.assertEquals(builder.getEnsembleProvider().getConnectionString(), "zk_connection_string");
        Assert.assertEquals(builder.getEnsembleProvider().getClass(), FixedEnsembleProvider.class);
    }

    @Test
    public void givenSchemeAndPayloadBuilderUsesAuthTest() {
        CuratorFrameworkFactory.Builder builder = setupBuilder(false /*without exhibitor*/, true /*with auth*/);
        List<AuthInfo> authInfos = builder.getAuthInfos();
        AuthInfo authInfo = authInfos.get(0);
        Assert.assertEquals(authInfo.getScheme(), "scheme");
        Assert.assertArrayEquals(authInfo.getAuth(), "abc".getBytes());
    }


    private CuratorFrameworkFactory.Builder setupBuilder(boolean withExhibitor) {
        return setupBuilder(withExhibitor, false /*without Auth*/);
    }

    private CuratorFrameworkFactory.Builder setupBuilder(boolean withExhibitor, boolean withAuth) {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        Map<String, Object> conf = new HashMap<String, Object>();
        if (withExhibitor) {
            conf.put(Config.STORM_EXHIBITOR_SERVERS, "foo");
            conf.put(Config.STORM_EXHIBITOR_PORT, 0);
            conf.put(Config.STORM_EXHIBITOR_URIPATH, "/exhibitor");
            conf.put(Config.STORM_EXHIBITOR_POLL, 0);
            conf.put(Config.STORM_EXHIBITOR_RETRY_INTERVAL, 0);
            conf.put(Config.STORM_EXHIBITOR_RETRY_INTERVAL_CEILING, 0);
            conf.put(Config.STORM_EXHIBITOR_RETRY_TIMES, 0);
        }
        conf.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, 0);
        conf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 0);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, 0);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL_CEILING, 0);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, 0);
        String zkStr = new String("zk_connection_string");
        ZookeeperAuthInfo auth = null;
        if (withAuth) {
            auth = new ZookeeperAuthInfo("scheme", "abc".getBytes());
        }
        CuratorUtils.testSetupBuilder(builder, zkStr, conf, auth);
        return builder;
    }
}

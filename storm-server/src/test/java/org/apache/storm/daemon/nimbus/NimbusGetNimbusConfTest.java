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

package org.apache.storm.daemon.nimbus;

import java.util.HashMap;
import java.util.Map;

import net.minidev.json.JSONValue;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.LocalCluster;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NimbusGetNimbusConfTest {

    private static final String MASKED = "*****";

    @SuppressWarnings("unchecked")
    private static Map<String, Object> parse(String json) {
        return (Map<String, Object>) JSONValue.parse(json);
    }

    @Test
    public void getNimbusConfMasksCredentialsAndKeepsOtherValues() throws Exception {
        Map<String, Object> daemonConf = new HashMap<>();
        daemonConf.put(DaemonConfig.NIMBUS_AUTHORIZER, "org.apache.storm.security.auth.authorizer.NoopAuthorizer");
        daemonConf.put(DaemonConfig.SUPERVISOR_AUTHORIZER, "org.apache.storm.security.auth.authorizer.NoopAuthorizer");
        daemonConf.put(Config.STORM_ZOOKEEPER_AUTH_PAYLOAD, "zk-user:zk-secret");
        daemonConf.put(Config.NIMBUS_THRIFT_TLS_SERVER_KEYSTORE_PASSWORD, "nimbus-keystore-secret");
        daemonConf.put(DaemonConfig.UI_HTTPS_KEYSTORE_PASSWORD, "ui-keystore-secret");
        daemonConf.put(Config.STORM_ZOOKEEPER_SSL_KEYSTORE_PASSWORD, "zk-ssl-keystore-secret");
        daemonConf.put("storm.daemon.metrics.reporter.plugin.prometheus.basic_auth_password", "plugin-secret");
        daemonConf.put(Config.NIMBUS_THRIFT_TLS_SERVER_KEYSTORE_PATH, "/etc/storm/nimbus.jks");

        try (LocalCluster cluster = new LocalCluster.Builder().withDaemonConf(daemonConf).build()) {
            Map<String, Object> served = parse(cluster.getNimbus().getNimbusConf());

            assertEquals(MASKED, served.get(Config.STORM_ZOOKEEPER_AUTH_PAYLOAD),
                "the cluster ZooKeeper auth payload should be masked");
            assertEquals(MASKED, served.get(Config.NIMBUS_THRIFT_TLS_SERVER_KEYSTORE_PASSWORD),
                "thrift TLS store passwords should be masked");
            assertEquals(MASKED, served.get(DaemonConfig.UI_HTTPS_KEYSTORE_PASSWORD),
                "UI keystore passwords should be masked");

            assertEquals(MASKED, served.get(Config.STORM_ZOOKEEPER_SSL_KEYSTORE_PASSWORD),
                "ZooKeeper TLS store passwords should be masked");
            assertEquals(MASKED, served.get("storm.daemon.metrics.reporter.plugin.prometheus.basic_auth_password"),
                "credential keys that only a plugin declares should be masked too");

            assertEquals("/etc/storm/nimbus.jks", served.get(Config.NIMBUS_THRIFT_TLS_SERVER_KEYSTORE_PATH),
                "non-credential values should be served unchanged");
        }
    }
}

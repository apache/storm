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

package org.apache.storm.cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.security.auth.DefaultPrincipalToLocal;
import org.apache.storm.shade.org.apache.zookeeper.ZooDefs;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.shade.org.apache.zookeeper.data.Id;
import org.apache.storm.utils.ConfigUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class DaemonTypeTest {

    @Test
    public void getDefaultZkAclsDefaultConf() {
        Map<String, Object> conf = ConfigUtils.readStormConfig();
        assertNull(DaemonType.UNKNOWN.getDefaultZkAcls(conf));
        assertNull(DaemonType.PACEMAKER.getDefaultZkAcls(conf));
        assertNull(DaemonType.SUPERVISOR.getDefaultZkAcls(conf));
        assertNull(DaemonType.NIMBUS.getDefaultZkAcls(conf));
        assertNull(DaemonType.WORKER.getDefaultZkAcls(conf));
    }

    @Test
    public void getDefaultZkAclsSecureServerConf() {
        Map<String, Object> conf = ConfigUtils.readStormConfig();
        conf.put(Config.STORM_ZOOKEEPER_AUTH_SCHEME, "digest");
        conf.put(Config.STORM_ZOOKEEPER_AUTH_PAYLOAD, "storm:thisisapoorpassword");
        conf.put(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN, DefaultPrincipalToLocal.class.getName());
        conf.put(Config.NIMBUS_THRIFT_PORT, 6666);

        assertNull(DaemonType.UNKNOWN.getDefaultZkAcls(conf));
        assertNull(DaemonType.PACEMAKER.getDefaultZkAcls(conf));
        assertEquals(DaemonType.NIMBUS_SUPERVISOR_ZK_ACLS, DaemonType.SUPERVISOR.getDefaultZkAcls(conf));
        assertEquals(DaemonType.NIMBUS_SUPERVISOR_ZK_ACLS, DaemonType.NIMBUS.getDefaultZkAcls(conf));
        assertNull(DaemonType.WORKER.getDefaultZkAcls(conf));
    }

    @Test
    public void getDefaultZkAclsSecureWorkerConf() {
        Map<String, Object> conf = ConfigUtils.readStormConfig();
        conf.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME, "digest");
        conf.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD, "storm:thisisapoorpassword");
        conf.put(Config.STORM_ZOOKEEPER_SUPERACL, "sasl:nimbus");
        conf.put(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN, DefaultPrincipalToLocal.class.getName());
        conf.put(Config.NIMBUS_THRIFT_PORT, 6666);

        assertNull(DaemonType.UNKNOWN.getDefaultZkAcls(conf));
        assertNull(DaemonType.PACEMAKER.getDefaultZkAcls(conf));
        assertNull(DaemonType.SUPERVISOR.getDefaultZkAcls(conf));
        assertNull(DaemonType.NIMBUS.getDefaultZkAcls(conf));
        List<ACL> expected = new ArrayList<>(ZooDefs.Ids.CREATOR_ALL_ACL);
        expected.add(new ACL(ZooDefs.Perms.ALL, new Id("sasl", "nimbus")));
        assertEquals(expected, DaemonType.WORKER.getDefaultZkAcls(conf));
    }
}
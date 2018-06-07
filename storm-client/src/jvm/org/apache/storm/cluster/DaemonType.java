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
import org.apache.storm.generated.WorkerTokenServiceType;
import org.apache.storm.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.storm.shade.org.apache.zookeeper.ZooDefs;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type of process/daemon that this server is running as.  This is used with the IStormClusterState implementation to know how to
 * properly secure data stored in it.
 */
public enum DaemonType {
    SUPERVISOR {
        @Override
        public List<ACL> getDefaultZkAcls(Map<String, Object> conf) {
            return getDefaultNimbusSupervisorZkAcls(conf);
        }
    },
    NIMBUS {
        @Override
        public List<ACL> getDefaultZkAcls(Map<String, Object> conf) {
            return getDefaultNimbusSupervisorZkAcls(conf);
        }

        @Override
        public List<ACL> getZkSecretAcls(WorkerTokenServiceType type, Map<String, Object> conf) {
            if (!Utils.isZkAuthenticationConfiguredStormServer(conf)) {
                //This is here only for testing.
                LOG.error("Will Store Worker Token Keys in ZK without ACLs.  If you are not running tests STOP NOW!");
                return null;
            }
            switch (type) {
                case NIMBUS:
                    //Fall through on purpose
                case SUPERVISOR:
                    return ZooDefs.Ids.CREATOR_ALL_ACL;
                case DRPC:
                    List<ACL> ret = new ArrayList<>(ZooDefs.Ids.CREATOR_ALL_ACL);
                    String drpcAcl = (String) conf.get(Config.STORM_ZOOKEEPER_DRPC_ACL);
                    if (drpcAcl != null) {
                        ret.add(new ACL(ZooDefs.Perms.READ,
                                        Utils.parseZkId(drpcAcl, Config.STORM_ZOOKEEPER_DRPC_ACL)));
                    } //else we assume it is the same as teh SUPER_ACL which is covered by CREATOR_ALL
                    return ret;
                default:
                    throw new IllegalStateException("WorkerTokens for " + type + " are not currently supported.");
            }
        }
    },
    WORKER {
        @Override
        public List<ACL> getDefaultZkAcls(Map<String, Object> conf) {
            return Utils.getWorkerACL(conf);
        }
    },
    PACEMAKER,
    UNKNOWN;

    @VisibleForTesting
    public static final List<ACL> NIMBUS_SUPERVISOR_ZK_ACLS = ZooDefs.Ids.CREATOR_ALL_ACL;
    private static final Logger LOG = LoggerFactory.getLogger(DaemonType.class);

    private static List<ACL> getDefaultNimbusSupervisorZkAcls(Map<String, Object> conf) {
        if (Utils.isZkAuthenticationConfiguredStormServer(conf)) {
            return NIMBUS_SUPERVISOR_ZK_ACLS;
        }
        return null;
    }

    /**
     * Get the default ZK ACLs that should be used for a given daemon type.
     *
     * @param conf the config used to help get the correct ACLs.
     * @return the list of acls. null means no acls.
     */
    public List<ACL> getDefaultZkAcls(Map<String, Object> conf) {
        return null;
    }

    /**
     * Get the ACLs to store a secret for a given service.
     *
     * @param type the type of service the secret is for.
     * @param conf the cluster configuration.
     * @return the ACLs
     */
    public List<ACL> getZkSecretAcls(WorkerTokenServiceType type, Map<String, Object> conf) {
        throw new IllegalArgumentException(name() + " does not support storing secrets.");
    }
}

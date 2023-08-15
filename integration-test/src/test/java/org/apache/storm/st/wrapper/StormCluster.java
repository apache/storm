/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.st.wrapper;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.st.utils.AssertUtil;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StormCluster {
    private static final Logger log = LoggerFactory.getLogger(StormCluster.class);
    private final Nimbus.Iface client;

    public StormCluster() {
        Map<String, Object> conf = getConfig();
        this.client = NimbusClient.getConfiguredClient(conf).getClient();
    }

    public static Map<String, Object> getConfig() {
        return Utils.readStormConfig();
    }

    public static boolean isSecure() {
        final String thriftConfig = "" + getConfig().get("storm.thrift.transport");
        final String thriftConfigInSecCluster = "org.apache.storm.security.auth.kerberos.KerberosSaslTransportPlugin";
        return thriftConfigInSecCluster.equals(thriftConfig.trim());
    }

    public List<TopologySummary> getSummaries() throws TException {
        final ClusterSummary clusterInfo = client.getClusterInfo();
        log.info("Cluster info: " + clusterInfo);
        return clusterInfo.get_topologies();
    }

    public List<TopologySummary> getActive() throws TException {
        return getTopologiesWithStatus("active");
    }

    public List<TopologySummary> getKilled() throws TException {
        return getTopologiesWithStatus("killed");
    }

    private List<TopologySummary> getTopologiesWithStatus(final String expectedStatus) throws TException {
        Collection<TopologySummary> topologySummaries = getSummaries();
        Collection<TopologySummary> filteredSummary = Collections2.filter(topologySummaries, new Predicate<TopologySummary>() {
            @Override
            public boolean apply(@Nullable TopologySummary input) {
                return input != null && input.get_status().toLowerCase().equals(expectedStatus.toLowerCase());
            }
        });
        return new ArrayList<>(filteredSummary);
    }

    public void killOrThrow(String topologyName) throws Exception {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() < start + TimeUnit.SECONDS.toMillis(60)) {
            try {
                KillOptions killOptions = new KillOptions();
                killOptions.set_wait_secs(0);
                client.killTopologyWithOpts(topologyName, killOptions);
                log.info("Topology killed: " + topologyName);
                return;
            } catch (TException e) {
                log.warn("Couldn't kill topology: " + topologyName + ", going to retry soon. Exception: " + ExceptionUtils.getFullStackTrace(e));
                Thread.sleep(TimeUnit.SECONDS.toMillis(2));
            }
        }
        throw new RuntimeException("Failed to kill topology " + topologyName + ". Subsequent tests may fail because worker slots are occupied");
    }

    public TopologySummary getOneActive() throws TException {
        List<TopologySummary> topoSummaries = getActive();
        AssertUtil.nonEmpty(topoSummaries, "Expecting one active topology.");
        assertEquals(topoSummaries.size(), 1, "Expected one topology to be running, found: " + topoSummaries);
        return topoSummaries.get(0);
    }

    public TopologyInfo getInfo(TopologySummary topologySummary) throws TException {
        return client.getTopologyInfo(topologySummary.get_id());
    }

    public Nimbus.Iface getNimbusClient() {
        return client;
    }

    public void killActiveTopologies() throws Exception {
        List<TopologySummary> activeTopologies = getActive();
        for (TopologySummary activeTopology : activeTopologies) {
            killOrThrow(activeTopology.get_name());
        }

        AssertUtil.empty(getActive());
    }
}

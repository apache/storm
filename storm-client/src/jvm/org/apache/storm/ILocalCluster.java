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

package org.apache.storm;

import java.util.List;
import java.util.Map;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.Credentials;
import org.apache.storm.generated.GetInfoOptions;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.RebalanceOptions;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.SubmitOptions;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.thrift.TException;

/**
 * This is here mostly for backwards compatibility.
 */
public interface ILocalCluster extends AutoCloseable {
    /**
     * Submit a topology to be run in local mode.
     *
     * @param topologyName the name of the topology to use
     * @param conf         the config for the topology
     * @param topology     the topology itself.
     * @return an AutoCloseable that will kill the topology.
     * @throws TException on any error from nimbus
     */
    ILocalTopology submitTopology(String topologyName, Map<String, Object> conf, StormTopology topology) throws TException;

    /**
     * Submit a topology to be run in local mode.
     *
     * @param topologyName the name of the topology to use
     * @param conf         the config for the topology
     * @param topology     the topology itself.
     * @param submitOpts   options for topology
     * @return an AutoCloseable that will kill the topology.
     * @throws TException on any error from nimbus
     */
    ILocalTopology submitTopologyWithOpts(String topologyName, Map<String, Object> conf, StormTopology topology,
                                          SubmitOptions submitOpts) throws TException;

    /**
     * Upload new credentials to a topology.
     *
     * @param topologyName the name of the topology
     * @param creds        the new credentails for the topology.
     * @throws TException on any error from nimbus
     */
    void uploadNewCredentials(String topologyName, Credentials creds) throws TException;

    /**
     * Kill a topology (if you are not using ILocalTopology).
     *
     * @param topologyName the name of the topology
     * @throws TException on any error from nimbus
     */
    void killTopology(String topologyName) throws TException;

    /**
     * Kill a topology (if you are not using ILocalTopology).
     *
     * @param topologyName the name of the topology
     * @param options      for how to kill the topology
     * @throws TException on any error from nimbus
     */
    void killTopologyWithOpts(String name, KillOptions options) throws TException;

    /**
     * Activate a topology.
     *
     * @param topologyName the name of the topology to activate
     * @throws TException on any error from nimbus
     */
    void activate(String topologyName) throws TException;

    /**
     * Deactivate a topology.
     *
     * @param topologyName the name of the topology to deactivate
     * @throws TException on any error from nimbus
     */
    void deactivate(String topologyName) throws TException;

    /**
     * Rebalance a topology.
     *
     * @param name    the name of the topology
     * @param options options for rebalanceing the topology.
     * @throws TException on any error from nimbus
     */
    void rebalance(String name, RebalanceOptions options) throws TException;

    /**
     * Shut down the cluster.
     *
     * @deprecated use {@link #close()} instead.
     */
    @Deprecated
    void shutdown();

    /**
     * The config of a topology as a JSON string.
     *
     * @param id the id of the topology (not the name)
     * @return The config of a topology as a JSON string
     * @throws TException on any error from nimbus
     */
    String getTopologyConf(String id) throws TException;

    /**
     * Get the compiled storm topology.
     *
     * @param id the id of the topology (not the name)
     * @return the compiled storm topology
     * @throws TException on any error from nimbus
     */
    StormTopology getTopology(String id) throws TException;

    /**
     * Get cluster information.
     * @return a summary of the current state of the cluster
     * @throws TException on any error from nimbus
     */
    ClusterSummary getClusterInfo() throws TException;

    List<TopologySummary> getTopologySummaries() throws TException;

    TopologySummary getTopologySummaryByName(String name) throws TException;

    TopologySummary getTopologySummary(String id) throws TException;

    /**
     * Get the state of a topology.
     *
     * @param id the id of the topology (not the name)
     * @return the state of a topology
     * @throws TException on any error from nimbus
     */
    TopologyInfo getTopologyInfo(String id) throws TException;

    /**
     * Get the state of a topology.
     *
     * @param name the name of the topology (not the id)
     * @return the state of a topology
     * @throws TException on any error from nimbus
     */
    TopologyInfo getTopologyInfoByName(String name) throws TException;

    /**
     * Get the state of a topology.
     *
     * @param id the id of the topology (not the name)
     * @param options This is to choose number of Error(s) in TopologyInfo.
     * @return the state of a topology
     * @throws TException on any error from nimbus
     */
    TopologyInfo getTopologyInfoWithOpts(String id, GetInfoOptions options) throws TException;

    /**
     * Get the state of a topology.
     *
     * @param name the name of the topology (not the id)
     * @param options This is GetInfoOptions to choose Error(s) in on TopologyInfo.
     * @return the state of a topology
     * @throws TException on any error from nimbus
     */
    TopologyInfo getTopologyInfoByNameWithOpts(String name, GetInfoOptions options) throws TException;

    /**
     * This is intended for internal testing only.
     *
     * @return an internal class that holds the state of the cluster.
     */
    IStormClusterState getClusterState();

    /**
     * Advance the cluster time when the cluster is using SimulatedTime. This is intended for internal testing only.
     *
     * @param secs the number of seconds to advance time
     */
    void advanceClusterTime(int secs) throws InterruptedException;

    /**
     * Advance the cluster time when the cluster is using SimulatedTime. This is intended for internal testing only.
     *
     * @param secs  the number of seconds to advance time
     * @param steps the number of steps we should take when advancing simulated time
     */
    void advanceClusterTime(int secs, int step) throws InterruptedException;

    /**
     * If the cluster is tracked get the id for the tracked cluster. This is intended for internal testing only.
     *
     * @return the id of the tracked cluster
     */
    String getTrackedId();

    /**
     * Close this class to kill the topology. This is here mostly for backwards compatibility.
     */
    interface ILocalTopology extends AutoCloseable {

    }
}

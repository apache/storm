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

package org.apache.storm;

import java.util.Map;
import org.apache.storm.generated.SubmitOptions;
import org.apache.storm.testing.TrackedTopology;
import org.apache.storm.thrift.TException;

/**
 * This is here mostly for backwards compatibility. Please see {@link org.apache.storm.LocalCluster} for more details on testing a Storm
 * Topology.
 */
public interface ILocalClusterTrackedTopologyAware extends ILocalCluster {

    /**
     * Submit a tracked topology to be run in local mode.
     *
     * @param topologyName the name of the topology to use
     * @param conf         the config for the topology
     * @param topology     the topology itself.
     * @return an AutoCloseable that will kill the topology.
     *
     * @throws TException on any error from nimbus
     */
    ILocalTopology submitTopology(String topologyName, Map<String, Object> conf, TrackedTopology topology) throws TException;

    /**
     * Submit a tracked topology to be run in local mode.
     *
     * @param topologyName the name of the topology to use
     * @param conf         the config for the topology
     * @param topology     the topology itself.
     * @param submitOpts   options for topology
     * @return an AutoCloseable that will kill the topology.
     *
     * @throws TException on any error from nimbus
     */
    ILocalTopology submitTopologyWithOpts(String topologyName, Map<String, Object> conf, TrackedTopology topology,
                                          SubmitOptions submitOpts) throws TException;

}

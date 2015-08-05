/**
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
package com.alibaba.jstorm.callback.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.callback.BaseCallback;
import com.alibaba.jstorm.cluster.StormBase;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.daemon.nimbus.NimbusUtils;

/**
 * Remove topology /ZK-DIR/topology data
 * 
 * remove this ZK node will trigger watch on this topology
 * 
 * And Monitor thread every 10 seconds will clean these disappear topology
 * 
 */
public class RemoveTransitionCallback extends BaseCallback {

    private static Logger LOG = LoggerFactory
            .getLogger(RemoveTransitionCallback.class);

    private NimbusData data;
    private String topologyid;

    public RemoveTransitionCallback(NimbusData data, String topologyid) {
        this.data = data;
        this.topologyid = topologyid;
    }

    @Override
    public <T> Object execute(T... args) {
        LOG.info("Begin to remove topology: " + topologyid);
        try {

            StormBase stormBase =
                    data.getStormClusterState().storm_base(topologyid, null);
            if (stormBase == null) {
                LOG.info("Topology " + topologyid + " has been removed ");
                return null;
            }
            data.getStormClusterState().remove_storm(topologyid);
            NimbusUtils.removeTopologyTaskTimeout(data, topologyid);
            LOG.info("Successfully removed ZK items topology: " + topologyid);

        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.warn("Failed to remove StormBase " + topologyid + " from ZK", e);
        }
        return null;
    }

}

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

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.callback.BaseCallback;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.cluster.StormStatus;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.schedule.Assignment.AssignmentType;

/**
 * Update user configuration
 * 
 * @author Basti.lj
 */
public class UpdateConfTransitionCallback extends BaseCallback {

    private static Logger LOG = LoggerFactory
            .getLogger(DelayStatusTransitionCallback.class);

    public static final int DEFAULT_DELAY_SECONDS = 30;

    private NimbusData data;
    private String topologyId;
    private StormStatus currentStatus;

    public UpdateConfTransitionCallback(NimbusData data, String topologyId,
            StormStatus currentStatus) {
        this.data = data;
        this.topologyId = topologyId;
        this.currentStatus = currentStatus;
    }

    @Override
    public <T> Object execute(T... args) {
        StormClusterState clusterState = data.getStormClusterState();
        try {
            Map userConf = (Map) args[0];
            Map topoConf =
                    StormConfig.read_nimbus_topology_conf(data.getConf(),
                            topologyId);
            topoConf.putAll(userConf);
            StormConfig.write_nimbus_topology_conf(data.getConf(), topologyId, topoConf);
            
            Assignment assignment =
                    clusterState.assignment_info(topologyId, null);
            assignment.setAssignmentType(AssignmentType.Config);
            assignment.updateTimeStamp();
            clusterState.set_assignment(topologyId, assignment);
            LOG.info("Successfully update new config to ZK for " + topologyId);
        } catch (Exception e) {
            LOG.error("Failed to update user configuartion.", e);
        }
        return currentStatus;
    }

}

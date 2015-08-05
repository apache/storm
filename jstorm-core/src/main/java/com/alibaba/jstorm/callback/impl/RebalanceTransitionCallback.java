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

import com.alibaba.jstorm.cluster.StormStatus;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.daemon.nimbus.StatusType;

/**
 * The action when nimbus receive rebalance command. Rebalance command is only
 * valid when current status is active
 * 
 * 1. change current topology status as rebalancing 2. do_rebalance action after
 * 2 * TIMEOUT seconds
 * 
 * @author Lixin/Longda
 * 
 */
public class RebalanceTransitionCallback extends DelayStatusTransitionCallback {

    public RebalanceTransitionCallback(NimbusData data, String topologyid,
            StormStatus status) {
        super(data, topologyid, status, StatusType.rebalancing,
                StatusType.do_rebalance);
    }

}

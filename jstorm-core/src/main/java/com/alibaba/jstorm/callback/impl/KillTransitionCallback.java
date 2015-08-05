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

import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.daemon.nimbus.StatusType;

/**
 * The action when nimbus receive killed command.
 * 
 * 1. change current topology status as killed 2. one TIMEOUT seconds later, do
 * remove action, which remove topology from ZK
 * 
 * @author Longda
 * 
 */
public class KillTransitionCallback extends DelayStatusTransitionCallback {

    public KillTransitionCallback(NimbusData data, String topologyid) {
        super(data, topologyid, null, StatusType.killed, StatusType.remove);
    }

}

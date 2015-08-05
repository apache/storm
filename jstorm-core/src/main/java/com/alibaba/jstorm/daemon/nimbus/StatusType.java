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
package com.alibaba.jstorm.daemon.nimbus;

/**
 * topology status:
 * 
 * 1. Status: this status will be stored in ZK
 * killed/inactive/active/rebalancing 2. action:
 * 
 * monitor -- every Config.NIMBUS_MONITOR_FREQ_SECS seconds will trigger this
 * only valid when current status is active inactivate -- client will trigger
 * this action, only valid when current status is active activate -- client will
 * trigger this action only valid when current status is inactive startup --
 * when nimbus startup, it will trigger this action only valid when current
 * status is killed/rebalancing kill -- client kill topology will trigger this
 * action, only valid when current status is active/inactive/killed remove -- 30
 * seconds after client submit kill command, it will do this action, only valid
 * when current status is killed rebalance -- client submit rebalance command,
 * only valid when current status is active/deactive do_rebalance -- 30 seconds
 * after client submit rebalance command, it will do this action, only valid
 * when current status is rebalance
 * 
 * 
 * 
 */

public enum StatusType {

    // status
    active("active"), inactive("inactive"), rebalancing("rebalancing"), killed(
            "killed"),

    // actions
    activate("activate"), inactivate("inactivate"), monitor("monitor"), startup(
            "startup"), kill("kill"), remove("remove"), rebalance("rebalance"), do_rebalance(
            "do-rebalance"), done_rebalance("done-rebalance"), update_conf("update-config");

    private String status;

    StatusType(String status) {
        this.status = status;
    }

    public String getStatus() {
        return status;
    }
}

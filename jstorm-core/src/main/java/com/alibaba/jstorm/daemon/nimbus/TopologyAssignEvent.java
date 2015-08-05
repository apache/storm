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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.alibaba.jstorm.cluster.StormStatus;

public class TopologyAssignEvent {

    // unit is minutes
    private static final int DEFAULT_WAIT_TIME = 2;
    private String topologyId;
    private String topologyName; // if this field has been set, it is create
    private String group;
    // topology
    private boolean isScratch;
    private boolean isReassign;
    private StormStatus oldStatus; // if this field has been set, it is
                                   // rebalance
    private CountDownLatch latch = new CountDownLatch(1);
    private boolean isSuccess = false;
    private String errorMsg;

    public String getTopologyId() {
        return topologyId;
    }

    public void setTopologyId(String topologyId) {
        this.topologyId = topologyId;
    }

    public boolean isScratch() {
        return isScratch;
    }

    public void setScratch(boolean isScratch) {
        this.isScratch = isScratch;
    }

    public boolean isReassign() {
        return isReassign;
    }

    public void setReassign(boolean isReassign) {
        this.isReassign = isReassign;
    }

    public StormStatus getOldStatus() {
        return oldStatus;
    }

    public void setOldStatus(StormStatus oldStatus) {
        this.oldStatus = oldStatus;
    }

    public String getTopologyName() {
        return topologyName;
    }

    public void setTopologyName(String topologyName) {
        this.topologyName = topologyName;
    }

    public boolean waitFinish() {
        try {
            latch.await(DEFAULT_WAIT_TIME, TimeUnit.MINUTES);
        } catch (InterruptedException e) {

        }
        return isSuccess;
    }

    public boolean isFinish() {
        return latch.getCount() == 0;
    }

    public void done() {
        isSuccess = true;
        latch.countDown();
    }

    public void fail(String errorMsg) {
        isSuccess = false;
        this.errorMsg = errorMsg;
        latch.countDown();
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

}

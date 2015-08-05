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
package com.alibaba.jstorm.cluster;

import java.io.Serializable;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.alibaba.jstorm.daemon.nimbus.StatusType;

/**
 * author: lixin/longda
 * 
 * Dedicate Topology status
 * 
 * Topology status: active/inactive/killed/rebalancing killTimeSecs: when status
 * isn't killed, it is -1 and useless. when status is killed, do kill operation
 * after killTimeSecs seconds when status is rebalancing, do rebalancing opation
 * after delaySecs seconds restore oldStatus as current status
 */
public class StormStatus implements Serializable {

    private static final long serialVersionUID = -2276901070967900100L;
    private StatusType type;
    @Deprecated
    private int killTimeSecs;
    private int delaySecs;
    private StormStatus oldStatus = null;

    public StormStatus(StatusType type) {
        this(0, type);
    }

    public StormStatus(int delaySecs, StatusType type) {
        this(type, delaySecs, null);
    }

    public StormStatus(StatusType type, int delaySecs, StormStatus oldStatus) {
        this.type = type;
        this.delaySecs = delaySecs;
        this.killTimeSecs = delaySecs;
        this.oldStatus = oldStatus;
    }

    public StatusType getStatusType() {
        return type;
    }

    public void setStatusType(StatusType type) {
        this.type = type;
    }

    @Deprecated
    public Integer getKillTimeSecs() {
        return killTimeSecs;
    }

    @Deprecated
    public void setKillTimeSecs(int killTimeSecs) {
        this.killTimeSecs = killTimeSecs;
    }

    public Integer getDelaySecs() {
        return delaySecs;
    }

    public void setDelaySecs(int delaySecs) {
        this.delaySecs = delaySecs;
    }

    public StormStatus getOldStatus() {
        return oldStatus;
    }

    public void setOldStatus(StormStatus oldStatus) {
        this.oldStatus = oldStatus;
    }

    @Override
    public boolean equals(Object base) {
        if ((base instanceof StormStatus) == false) {
            return false;
        }

        StormStatus check = (StormStatus) base;
        if (check.getStatusType().equals(getStatusType())
                && check.getKillTimeSecs() == getKillTimeSecs()
                && check.getDelaySecs().equals(getDelaySecs())) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return this.getStatusType().hashCode()
                + this.getKillTimeSecs().hashCode()
                + this.getDelaySecs().hashCode();
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this,
                ToStringStyle.SHORT_PREFIX_STYLE);
    }

}

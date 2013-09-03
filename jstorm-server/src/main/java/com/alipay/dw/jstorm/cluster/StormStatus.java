package com.alipay.dw.jstorm.cluster;

import java.io.Serializable;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.alipay.dw.jstorm.daemon.nimbus.StatusType;

/**
 * author: lixin
 * 
 * Dedicate Topology status
 * 
 * Topology status: active/inactive/killed/rebalancing
 * killTimeSecs: when status isn't killed, it is -1 and useless.
 * when status is killed, do kill operation after killTimeSecs seconds
 * when status is rebalancing, do rebalancing opation after delaySecs seconds
 * restore oldStatus as current status
 */
public class StormStatus implements Serializable {
    
    private static final long serialVersionUID = -2276901070967900100L;
    private StatusType        type;
    private int               killTimeSecs;
    private int               delaySecs;
    private StormStatus       oldStatus        = null;
    
    public StormStatus(int killTimeSecs, StatusType type, StormStatus oldStatus) {
        this.type = type;
        this.killTimeSecs = killTimeSecs;
        this.oldStatus = oldStatus;
    }
    
    public StormStatus(int killTimeSecs, StatusType type) {
        this.type = type;
        this.killTimeSecs = killTimeSecs;
    }
    
    public StormStatus(StatusType type, int delaySecs, StormStatus oldStatus) {
        this.type = type;
        this.delaySecs = delaySecs;
        this.oldStatus = oldStatus;
    }
    
    public StormStatus(StatusType type) {
        this.type = type;
        this.killTimeSecs = -1;
        this.delaySecs = -1;
    }
    
    public StatusType getStatusType() {
        return type;
    }
    
    public void setStatusType(StatusType type) {
        this.type = type;
    }
    
    public Integer getKillTimeSecs() {
        return killTimeSecs;
    }
    
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

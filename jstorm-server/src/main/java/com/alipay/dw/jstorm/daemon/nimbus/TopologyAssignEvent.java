package com.alipay.dw.jstorm.daemon.nimbus;

import com.alipay.dw.jstorm.cluster.StormStatus;

public class TopologyAssignEvent {
    
    private String      topologyId;
    private boolean     isScratch;
    private String      topologyName; // if this field has been set, it is create topology
    private StormStatus oldStatus;   // if this field has been set, it is rebalance
                                      
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
    
}

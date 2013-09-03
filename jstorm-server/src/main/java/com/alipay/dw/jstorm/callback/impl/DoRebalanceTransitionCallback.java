package com.alipay.dw.jstorm.callback.impl;

import org.apache.log4j.Logger;

import com.alipay.dw.jstorm.callback.BaseCallback;
import com.alipay.dw.jstorm.cluster.StormStatus;
import com.alipay.dw.jstorm.daemon.nimbus.NimbusData;
import com.alipay.dw.jstorm.daemon.nimbus.TopologyAssign;
import com.alipay.dw.jstorm.daemon.nimbus.TopologyAssignEvent;

/**
 * Do real rebalance action.
 * 
 * After nimbus receive one rebalance command, it will do as following:
 * 1. set topology status as rebalancing
 * 2. delay 2 * timeout seconds
 * 3. do this callback
 * 
 * @author Xin.Li/Longda
 * 
 */
public class DoRebalanceTransitionCallback extends BaseCallback {
    
    private static Logger LOG = Logger.getLogger(DoRebalanceTransitionCallback.class);
    
    private String        topologyid;
    private StormStatus   oldStatus;
    
    public DoRebalanceTransitionCallback(NimbusData data, String topologyid,
            StormStatus status) {
        //this.data = data;
        this.topologyid = topologyid;
        this.oldStatus = status;
    }
    
    @Override
    public <T> Object execute(T... args) {
        try {
            TopologyAssignEvent event = new TopologyAssignEvent();
            
            event.setTopologyId(topologyid);
            event.setScratch(true);
            event.setOldStatus(oldStatus);
            
            TopologyAssign.push(event);
            
        } catch (Exception e) {
            LOG.error("do-rebalance error!", e);
        }
        // FIXME Why oldStatus?
        return oldStatus.getOldStatus();
    }
    
}

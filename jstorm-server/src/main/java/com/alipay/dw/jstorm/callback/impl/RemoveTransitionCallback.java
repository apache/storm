package com.alipay.dw.jstorm.callback.impl;

import org.apache.log4j.Logger;

import com.alipay.dw.jstorm.callback.BaseCallback;
import com.alipay.dw.jstorm.daemon.nimbus.NimbusData;

/**
 * Remove topology /ZK-DIR/topology data
 * 
 * remove this ZK node will trigger watch on this topology
 * 
 * And Monitor thread every 10 seconds will clean these disappear topology
 * 
 */
public class RemoveTransitionCallback extends BaseCallback {
    
    private static Logger LOG = Logger.getLogger(RemoveTransitionCallback.class);
    
    private NimbusData    data;
    private String        topologyid;
    
    public RemoveTransitionCallback(NimbusData data, String topologyid) {
        this.data = data;
        this.topologyid = topologyid;
    }
    
    @Override
    public <T> Object execute(T... args) {
        LOG.info("Killing topology: " + topologyid);
        try {
            data.getStormClusterState().remove_storm(topologyid);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.warn("Failed to remove StormBase " + topologyid + " from ZK", e);
        }
        return null;
    }
    
}

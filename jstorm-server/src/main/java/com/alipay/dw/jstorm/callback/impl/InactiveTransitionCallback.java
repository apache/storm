package com.alipay.dw.jstorm.callback.impl;

import com.alipay.dw.jstorm.callback.BaseCallback;
import com.alipay.dw.jstorm.cluster.StormStatus;
import com.alipay.dw.jstorm.daemon.nimbus.StatusType;

/**
 * 
 * set Topology status as inactive
 * 
 * Here just return inactive status
 * Later, it will set inactive status to ZK
 */
public class InactiveTransitionCallback extends BaseCallback {
    
    @Override
    public <T> Object execute(T... args) {
        
        return new StormStatus(StatusType.inactive);
    }
    
}

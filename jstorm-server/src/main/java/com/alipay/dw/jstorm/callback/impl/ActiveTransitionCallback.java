package com.alipay.dw.jstorm.callback.impl;

import com.alipay.dw.jstorm.callback.BaseCallback;
import com.alipay.dw.jstorm.cluster.StormStatus;
import com.alipay.dw.jstorm.daemon.nimbus.StatusType;

/**
 * Set the topology status as Active
 * 
 */
public class ActiveTransitionCallback extends BaseCallback {
    
    @Override
    public <T> Object execute(T... args) {
        
        return new StormStatus(StatusType.active);
    }
    
}

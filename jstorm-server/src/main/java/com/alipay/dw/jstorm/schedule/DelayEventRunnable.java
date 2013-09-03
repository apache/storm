package com.alipay.dw.jstorm.schedule;

import com.alipay.dw.jstorm.daemon.nimbus.NimbusData;
import com.alipay.dw.jstorm.daemon.nimbus.NimbusUtils;
import com.alipay.dw.jstorm.daemon.nimbus.StatusType;

public class DelayEventRunnable implements Runnable {
    
    private NimbusData data;
    private String     topologyid;
    private StatusType status;
    
    public DelayEventRunnable(NimbusData data, String topologyid,
            StatusType status) {
        this.data = data;
        this.topologyid = topologyid;
        this.status = status;
    }
    
    @Override
    public void run() {
        NimbusUtils.transition(data, topologyid, false, status);
    }
    
}

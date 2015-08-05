package com.alibaba.jstorm.callback.impl;

import com.alibaba.jstorm.callback.BaseCallback;
import com.alibaba.jstorm.cluster.StormStatus;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.daemon.nimbus.StatusType;

public class DoneRebalanceTransitionCallback extends BaseCallback {

    private NimbusData data;
    private String topologyId;

    public DoneRebalanceTransitionCallback(NimbusData data, String topologyId) {
        this.data = data;
        this.topologyId = topologyId;
    }

    @Override
    public <T> Object execute(T... args) {
        return new StormStatus(StatusType.active);
    }
}
package com.alipay.dw.jstorm.task;

import backtype.storm.daemon.Shutdownable;

import com.alipay.dw.jstorm.cluster.DaemonCommon;

public interface ShutdownableDameon extends Shutdownable, DaemonCommon,
        Runnable {
    
}

package com.alipay.dw.jstorm.daemon.worker;

import com.alipay.dw.jstorm.callback.RunnableCallback;
import com.alipay.dw.jstorm.common.JStormUtils;

public class WorkerHaltRunable extends RunnableCallback {
    
    @Override
    public void run() {
        JStormUtils.halt_process(1, "Task died");
    }
    
}

package com.alipay.dw.jstorm.callback;

import com.alipay.dw.jstorm.common.JStormUtils;

/**
 * Killer callback
 * 
 * @author yannian
 * 
 */

public class AsyncLoopDefaultKill extends RunnableCallback {
    
    @Override
    public <T> Object execute(T... args) {
        Exception e = (Exception) args[0];
        JStormUtils.halt_process(1, "Async loop died!");
        return e;
    }
    
    @Override
    public void run() {
        JStormUtils.halt_process(1, "Async loop died!");
    }
}

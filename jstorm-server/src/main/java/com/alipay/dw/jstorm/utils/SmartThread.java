package com.alipay.dw.jstorm.utils;

/**
 * 
 * @author yannian
 * 
 */
public interface SmartThread {
    public void start();
    
    public void join() throws InterruptedException;;
    
    public void interrupt();
    
    public Boolean isSleeping();
}

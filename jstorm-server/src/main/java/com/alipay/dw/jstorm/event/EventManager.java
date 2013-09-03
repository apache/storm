package com.alipay.dw.jstorm.event;

import com.alipay.dw.jstorm.callback.RunnableCallback;

public interface EventManager {
    public void add(RunnableCallback event_fn);
    
    public boolean waiting();
    
    public void shutdown();
}

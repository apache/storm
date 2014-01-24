package com.alibaba.jstorm.event;

import com.alibaba.jstorm.callback.RunnableCallback;

public interface EventManager {
	public void add(RunnableCallback event_fn);

	public boolean waiting();

	public void shutdown();
}

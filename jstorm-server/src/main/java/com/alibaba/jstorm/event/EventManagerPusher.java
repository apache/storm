package com.alibaba.jstorm.event;

import java.util.concurrent.atomic.AtomicBoolean;

import com.alibaba.jstorm.callback.RunnableCallback;

/**
 * put event to eventManager queue
 */
public class EventManagerPusher extends RunnableCallback {

	private EventManager eventManager;

	private RunnableCallback event;

	private AtomicBoolean active;

	private Integer result;

	private int frequence;

	public EventManagerPusher(EventManager eventManager,
			RunnableCallback event, AtomicBoolean active, int frequence) {
		this.eventManager = eventManager;
		this.event = event;
		this.active = active;
		this.result = null;
		this.frequence = frequence;
	}

	@Override
	public Object getResult() {
		return result;
	}

	@Override
	public void run() {
		eventManager.add(event);
		if (active.get()) {
			this.result = frequence;
		} else {
			this.result = -1;

		}
	}

}

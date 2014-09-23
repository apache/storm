package com.alibaba.jstorm.event;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.jstorm.callback.RunnableCallback;

import backtype.storm.utils.Time;

/**
 * Event Manager, drop one event from queue, then execute the event.
 */
public class EventManagerImp implements EventManager {
	private AtomicInteger added = new AtomicInteger();
	private AtomicInteger processed = new AtomicInteger();
	private AtomicBoolean isrunning = new AtomicBoolean(true);
	private Thread runningThread;
	private LinkedBlockingQueue<RunnableCallback> queue = new LinkedBlockingQueue<RunnableCallback>();

	public EventManagerImp(boolean _daemon) {

		Runnable runner = new EventManagerImpExecute(this);

		this.runningThread = new Thread(runner);
		this.runningThread.setDaemon(_daemon);
		this.runningThread.start();
	}

	public boolean isRunning() {
		return isrunning.get();
	}

	public RunnableCallback poll() throws InterruptedException {
		RunnableCallback event = queue.poll();
		return event;
	}
	
	public RunnableCallback take() throws InterruptedException {
		RunnableCallback event = queue.take();
		return event;
	}

	public void proccessinc() {
		processed.incrementAndGet();
	}

	@Override
	public void add(RunnableCallback event_fn) {
		if (!this.isRunning()) {
			// throw new RuntimeException(
			// "Cannot add events to a shutdown event manager");

			return;
		}
		added.incrementAndGet();
		queue.add(event_fn);
	}

	@Override
	public boolean waiting() {
		return (processed.get() == added.get());

		// return Time.isThreadWaiting(runningThread) ||
		// (processed.get() == added.get());

	}

	@Override
	public void shutdown() {
		isrunning.set(false);
		runningThread.interrupt();
		try {
			runningThread.join();
		} catch (InterruptedException e) {
		}

	}
}

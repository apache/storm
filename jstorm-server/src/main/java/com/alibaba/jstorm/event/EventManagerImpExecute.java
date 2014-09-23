package com.alibaba.jstorm.event;

import org.apache.log4j.Logger;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.utils.JStormUtils;

public class EventManagerImpExecute implements Runnable {
	private static Logger LOG = Logger.getLogger(EventManagerImpExecute.class);

	public EventManagerImpExecute(EventManagerImp manager) {
		this.manager = manager;
	}

	EventManagerImp manager;
	Exception error = null;

	@Override
	public void run() {
		try {
			while (manager.isRunning()) {
				RunnableCallback r = null;
				try {
					r = manager.take();
				} catch (InterruptedException e) {
					// LOG.info("Failed to get ArgsRunable from EventManager queue");
				}

				if (r == null) {
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {

					}
					continue;
				}

				r.run();
				Exception e = r.error();
				if (e != null) {
					throw e;
				}
				manager.proccessinc();

			}

		} catch (InterruptedException e) {
			error = e;
			LOG.error("Event Manager interrupted", e);
		} catch (Exception e) {
			LOG.error("Error when processing event ", e);
			JStormUtils.halt_process(20, "Error when processing an event");
		}

	}

}

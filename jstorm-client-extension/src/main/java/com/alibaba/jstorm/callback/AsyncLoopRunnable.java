package com.alibaba.jstorm.callback;

import org.apache.log4j.Logger;

import com.alibaba.jstorm.utils.JStormUtils;

/**
 * AsyncLoopThread 's runnable
 * 
 * The class wrapper RunnableCallback fn, if occur exception, run killfn
 * 
 * @author yannian
 * 
 */
public class AsyncLoopRunnable implements Runnable {
	private static Logger LOG = Logger.getLogger(AsyncLoopRunnable.class);

	private RunnableCallback fn;
	private RunnableCallback killfn;

	public AsyncLoopRunnable(RunnableCallback fn, RunnableCallback killfn) {
		this.fn = fn;
		this.killfn = killfn;
	}

	private boolean needQuit(Object rtn) {
		if (rtn != null) {
			long sleepTime = Long.parseLong(String.valueOf(rtn));
			if (sleepTime < 0) {
				return true;
			}else if (sleepTime > 0) {
				JStormUtils.sleepMs(sleepTime * 1000);
			} 
		}
		return false;
	}

	@Override
	public void run() {

		try {
			while (true) {
				Exception e = null;

				try {
					if (fn == null) {
						LOG.warn("fn==null");
						throw new RuntimeException("AsyncLoopRunnable no core function ");
					}

					fn.run();

					e = fn.error();

				} catch (Exception ex) {
					e = ex;
				}
				if (e != null) {
					fn.shutdown();
					throw e;
				}
				Object rtn = fn.getResult();
				if (this.needQuit(rtn)) {
					return;
				}

			}
		} catch (InterruptedException e) {
			LOG.info("Async loop interrupted!");
		} catch (Throwable e) {
			Object rtn = fn.getResult();
			if (this.needQuit(rtn)) {
				return;
			}else {
				LOG.error("Async loop died!", e);
				killfn.execute(e);
			}
		}

	}

}

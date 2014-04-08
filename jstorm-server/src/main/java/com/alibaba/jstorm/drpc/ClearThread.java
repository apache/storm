package com.alibaba.jstorm.drpc;

import java.util.Map.Entry;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.generated.DRPCExecutionException;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeUtils;

public class ClearThread extends RunnableCallback {
	private static final Logger LOG = Logger.getLogger(ClearThread.class);
			
	private final int REQUEST_TIMEOUT_SECS;
	private static final int TIMEOUT_CHECK_SECS = 5;
	
	private Drpc   drpcService;
	private AtomicBoolean active;
	
	public ClearThread(Drpc drpc) {
		drpcService = drpc;
		active = drpc.getIsActive();
		
		REQUEST_TIMEOUT_SECS = JStormUtils.parseInt(
				drpcService.getConf().get(Config.DRPC_REQUEST_TIMEOUT_SECS), 60);
		LOG.info("Drpc timeout seconds is " + REQUEST_TIMEOUT_SECS);
	}

	@Override
	public void run() {

		for (Entry<String, Integer> e : drpcService.getIdtoStart().entrySet()) {
			if (TimeUtils.time_delta(e.getValue()) > REQUEST_TIMEOUT_SECS) {
				String id = e.getKey();
				
				drpcService.getIdtoResult().put(id, new DRPCExecutionException(
						"Request timed out"));
				Semaphore s = drpcService.getIdtoSem().get(id);
				if (s != null) {
					s.release();
				}
				drpcService.cleanup(id);
				LOG.info("Clear request " + id);
			}
		}
		
		JStormUtils.sleepMs(10);

	}

	public Object getResult() {
		if (active.get() == true ) {
			return TIMEOUT_CHECK_SECS;
		}else {
			LOG.info("Quit Drpc clear thread ");
			return -1;
		}
		
	}

}

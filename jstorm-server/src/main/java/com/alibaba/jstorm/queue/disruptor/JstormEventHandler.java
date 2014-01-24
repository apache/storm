package com.alibaba.jstorm.queue.disruptor;

import org.apache.log4j.Logger;

import com.lmax.disruptor.EventHandler;

public class JstormEventHandler implements EventHandler {

	Logger logger = Logger.getLogger(JstormEventHandler.class);

	private int count;

	public JstormEventHandler(int count) {
		this.count = count;
	}

	@Override
	public void onEvent(Object event, long sequence, boolean endOfBatch)
			throws Exception {
		long msgId = Long.parseLong(((JstormEvent) event).getMsgId());
		// if (msgId % size ==0) {
		// logger.warn("consumer msgId=" + msgId + ", seq=" + sequence);
		// }
		if (msgId == count - 1) {
			logger.warn("end..." + System.currentTimeMillis());
		}
	}

}

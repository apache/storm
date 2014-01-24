package com.alibaba.jstorm.queue.disruptor;

import org.apache.log4j.Logger;

import com.lmax.disruptor.RingBuffer;

public class JstormProducer implements Runnable {

	Logger logger = Logger.getLogger(JstormProducer.class);

	private RingBuffer<JstormEvent> ringBuffer;
	private int size;

	public JstormProducer(RingBuffer<JstormEvent> ringBuffer, int size) {
		this.ringBuffer = ringBuffer;
		this.size = size;
	}

	@Override
	public void run() {
		logger.warn("producer start..." + System.currentTimeMillis());

		// while (true) {
		// long seqId = ringBuffer.next();
		//
		// ringBuffer.get(seqId).setMsgId(String.valueOf(seqId));
		// ringBuffer.publish(seqId);
		//
		// try {
		// double random = Math.random();
		// Thread.sleep((long)(random * 1000));
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// }
		// }

		for (int i = 0; i < size; i++) {
			long seqId = ringBuffer.next();

			ringBuffer.get(seqId).setMsgId(String.valueOf(seqId));
			ringBuffer.publish(seqId);
		}
	}

}

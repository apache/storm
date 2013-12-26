package com.alibaba.jstorm.message.queue;


import org.apache.log4j.Logger;
import org.junit.Test;

import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.queue.disruptor.JstormEvent;
import com.alibaba.jstorm.queue.disruptor.JstormEventHandler;
import com.alibaba.jstorm.queue.disruptor.JstormProducer;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.SingleThreadedClaimStrategy;

public class DisruptorTest {
	
	private int count = 100000000;
	private int buffer_size = 8 * 1024;
	
	private Logger logger = Logger.getLogger(DisruptorTest.class);
	
//	@Test
//	public void test_1P1C() {
//		RingBuffer<JstormEvent> ringBuffer = new RingBuffer<JstormEvent>(
//				JstormEvent.EVENT_FACTORY, new SingleThreadedClaimStrategy(buffer_size),
//				new BlockingWaitStrategy());
//		SequenceBarrier seqBarrier = ringBuffer.newBarrier();
//		JstormEventHandler handler = new JstormEventHandler(count);
//		BatchEventProcessor<JstormEvent> batchHandler = new BatchEventProcessor<JstormEvent>(
//				ringBuffer, seqBarrier, handler);
//		ringBuffer.setGatingSequences(batchHandler.getSequence());
//		
//		//consumer
//		Thread consumer = new Thread(batchHandler);
//		consumer.start();
//		
//		//producer
//		Thread producer = new Thread(new JstormProducer(ringBuffer, count));
//		producer.start();
//		
//		try {
//			producer.join();
//			consumer.join();
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//	}
//	
//	@Test
//	public void test_queue() {
//		final DisruptorQueue queue = new DisruptorQueue(new SingleThreadedClaimStrategy(buffer_size), 
//				new BlockingWaitStrategy());
//		
//		logger.warn("start..." + System.currentTimeMillis());
//		//Consumer
//		Thread consumer = new Thread(new Runnable(){
//
//			@Override
//			public void run() {
//				while (true) {
//					queue.consumeBatchWhenAvailable(new JstormEventHandler(count));
//				}
//				
//			}
//			
//		});
//		consumer.start();
//		
//		//Producer
//		for (int i = 0; i < count; i++) {
//			JstormEvent event = new JstormEvent();
//			event.setMsgId(String.valueOf(i));
//			queue.publish(event);
//		}
//		
//		try {
//			consumer.join();
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		} 
//	}
	
	
}

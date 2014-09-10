package com.alibaba.jstorm.message.queue;

import org.apache.log4j.Logger;
import org.junit.Test;

import backtype.storm.utils.DisruptorQueue;
import clojure.lang.Compiler.NewExpr;

import com.alibaba.jstorm.queue.disruptor.JstormEvent;
import com.alibaba.jstorm.queue.disruptor.JstormEventHandler;
import com.alibaba.jstorm.queue.disruptor.JstormProducer;
import com.alibaba.jstorm.utils.JStormUtils;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.SingleThreadedClaimStrategy;

public class DisruptorTest {

	private int count = 100000000;
	private int buffer_size = 8 * 1024;

	private Logger logger = Logger.getLogger(DisruptorTest.class);

	//@Test
	public void testMultipleConsume() {
		final DisruptorQueue disruptorQueue = new DisruptorQueue(
				new SingleThreadedClaimStrategy(1024), new BlockingWaitStrategy());
		
//		new Thread(new Runnable() {
//			
//			@Override
//			public void run() {
//				System.out.println("Begin to produce item");
//				JStormUtils.sleepMs(1000);
//				
//				for (int i = 0; i < 1000000; i++) {
//					disruptorQueue.publish(Integer.valueOf(i));
//				}
//				
//				System.out.println("Finish produce item");
//			}
//		}).start();
//		
//		
//		new Thread(new Runnable() {
//			
//			@Override
//			public void run() {
//				while(true) {
//					disruptorQueue.consumeBatchWhenAvailable(new EventHandler<Object>() {
//
//						@Override
//						public void onEvent(Object event, long sequence,
//								boolean endOfBatch) throws Exception {
//							
//							System.out.println("Consumer 1:" + (Integer)event);
//						}
//						
//					});
//				}
//				
//			}
//		}).start();
//		
//		new Thread(new Runnable() {
//			
//			@Override
//			public void run() {
//				while(true) {
//					disruptorQueue.consumeBatchWhenAvailable(new EventHandler<Object>() {
//
//						@Override
//						public void onEvent(Object event, long sequence,
//								boolean endOfBatch) throws Exception {
//							
//							System.out.println("Consumer 2:" + (Integer)event);
//						}
//						
//					});
//				}
//				
//			}
//		}).start();
//		
//		JStormUtils.sleepMs(100000);
	}

}

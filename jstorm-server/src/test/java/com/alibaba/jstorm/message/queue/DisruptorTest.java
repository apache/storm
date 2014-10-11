package com.alibaba.jstorm.message.queue;

import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.Test;

import backtype.storm.utils.DisruptorQueue;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.MultiThreadedClaimStrategy;
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

	
	private final static int TIMEOUT = 5; // MS
    private final static int PRODUCER_NUM = 4;

    @Test
    public void testConsumerHang() throws InterruptedException {
    	 System.out.println("Begin testConsumerHang");
        final AtomicBoolean messageConsumed = new AtomicBoolean(false);

        // Set queue length to 1, so that the RingBuffer can be easily full
        // to trigger consumer blocking
        DisruptorQueue queue = createQueue("consumerHang", 1);
        // if no queue.consumerStarted(), this test will fail
        queue.consumerStarted();
        Runnable producer = new Producer(queue, "msg");
        Runnable consumer = new Consumer(queue, new EventHandler<Object>() {
        	long count = 0;
            @Override
            public void onEvent(Object obj, long sequence, boolean endOfBatch)
                    throws Exception {
            	System.out.println("Consume " + count++);
                messageConsumed.set(true);
            }
        });

        run(producer, consumer);
        Assert.assertTrue("disruptor message is never consumed due to consumer thread hangs",
                messageConsumed.get());
        
        System.out.println("End testConsumerHang");
    }
    
    @Test
    public void testMessageDisorder() throws InterruptedException {

    	 System.out.println("begin testMessageDisorder");
        // Set queue length to bigger enough
        DisruptorQueue queue = createQueue("messageOrder", 16);

        queue.publish("1");

        Runnable producer = new Producer(queue, "2");

        final Object[] result = new Object[1];
        Runnable consumer = new Consumer(queue, new EventHandler<Object>() {
            private boolean head = true;

            @Override
            public void onEvent(Object obj, long sequence, boolean endOfBatch)
                    throws Exception {
                if (head) {
                    head = false;
                    result[0] = obj;
                }
            }
        });

        run(producer, consumer);
        Assert.assertEquals("We expect to receive first published message first, but received " + result[0],
                "1", result[0]);
        System.out.println("End testMessageDisorder");
    }

    


    private void run(Runnable producer, Runnable consumer)
            throws InterruptedException {

        Thread[] producerThreads = new Thread[PRODUCER_NUM];
        for (int i = 0; i < PRODUCER_NUM; i++) {
            producerThreads[i] = new Thread(producer);
            producerThreads[i].start();
        }

        Thread consumerThread = new Thread(consumer);
        consumerThread.start();

        for (int i = 0; i < PRODUCER_NUM; i++) {
            producerThreads[i].interrupt();
            producerThreads[i].join(TIMEOUT);
        }
        consumerThread.interrupt();
        consumerThread.join(TIMEOUT);
    }

    private class Producer implements Runnable {
        private String msg;
        private DisruptorQueue queue;

        Producer(DisruptorQueue queue, String msg) {
            this.msg = msg;
            this.queue = queue;
        }

        @Override
        public void run() {
        	long count = 0;
            try {
                while (true) {
                	System.out.println(Thread.currentThread().getId() + " Publish one :" + count++);
                    queue.publish(msg, false);
                }
            } catch (InsufficientCapacityException e) {
            	System.out.println(Thread.currentThread().getId() + " InsufficientCapacityException " + count);
                return;
            }
        }
    }

    private class Consumer implements Runnable {
        private EventHandler handler;
        private DisruptorQueue queue;

        Consumer(DisruptorQueue queue, EventHandler handler) {
            this.handler = handler;
            this.queue = queue;
        }

        @Override
        public void run() {
            queue.consumerStarted();
            try {
                while(true) {
                    queue.consumeBatchWhenAvailable(handler);
                }
            }catch(RuntimeException e) {
                //break
            }
        }
    }

    private static DisruptorQueue createQueue(String name, int queueSize) {
        return new DisruptorQueue(new MultiThreadedClaimStrategy(queueSize), new BlockingWaitStrategy());
    }
}



package com.alibaba.jstorm.util.queue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.Test;

import backtype.storm.utils.DisruptorQueue;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.dsl.ProducerType;

public class DisruptorTest {

	static {
		DisruptorQueue.setUseSleep(true);
		DisruptorQueue.setLimited(true);
	}

	private int count = 100000000;
	private int buffer_size = 8 * 1024;

	private Logger logger = Logger.getLogger(DisruptorTest.class);

	@Test
	public void testMultipleConsume() {
		final DisruptorQueue disruptorQueue = createQueue("test",
				ProducerType.MULTI, 1024);

		// new Thread(new Runnable() {
		//
		// @Override
		// public void run() {
		// System.out.println("Begin to produce item");
		// JStormUtils.sleepMs(1000);
		//
		// for (int i = 0; i < 1000000; i++) {
		// disruptorQueue.publish(Integer.valueOf(i));
		// }
		//
		// System.out.println("Finish produce item");
		// }
		// }).start();
		//
		//
		// new Thread(new Runnable() {
		//
		// @Override
		// public void run() {
		// while(true) {
		// disruptorQueue.consumeBatchWhenAvailable(new EventHandler<Object>() {
		//
		// @Override
		// public void onEvent(Object event, long sequence,
		// boolean endOfBatch) throws Exception {
		//
		// System.out.println("Consumer 1:" + (Integer)event);
		// }
		//
		// });
		// }
		//
		// }
		// }).start();
		//
		// new Thread(new Runnable() {
		//
		// @Override
		// public void run() {
		// while(true) {
		// disruptorQueue.consumeBatchWhenAvailable(new EventHandler<Object>() {
		//
		// @Override
		// public void onEvent(Object event, long sequence,
		// boolean endOfBatch) throws Exception {
		//
		// System.out.println("Consumer 2:" + (Integer)event);
		// }
		//
		// });
		// }
		//
		// }
		// }).start();
		//
		// JStormUtils.sleepMs(100000);
	}

	private final static int TIMEOUT = 5; // MS
	private final static int PRODUCER_NUM = 4;

	@Test
	public void testLaterStartConsumer() throws InterruptedException {
		System.out
				.println("!!!!!!!!!!!!!!!Begin testLaterStartConsumer!!!!!!!!!!");
		final AtomicBoolean messageConsumed = new AtomicBoolean(false);

		// Set queue length to 1, so that the RingBuffer can be easily full
		// to trigger consumer blocking
		DisruptorQueue queue = createQueue("consumerHang", ProducerType.MULTI,
				2);
		push(queue, 1);
		Runnable producer = new Producer(queue);
		Runnable consumer = new Consumer(queue, new EventHandler<Object>() {
			long count = 0;

			@Override
			public void onEvent(Object obj, long sequence, boolean endOfBatch)
					throws Exception {

				messageConsumed.set(true);
				System.out.println("Consume " + count++);
			}
		});

		run(producer, 0, 0, consumer, 50);
		Assert.assertTrue(
				"disruptor message is never consumed due to consumer thread hangs",
				messageConsumed.get());

		System.out
				.println("!!!!!!!!!!!!!!!!End testLaterStartConsumer!!!!!!!!!!");
	}

	@Test
	public void testBeforeStartConsumer() throws InterruptedException {
		System.out
				.println("!!!!!!!!!!!!Begin testBeforeStartConsumer!!!!!!!!!");
		final AtomicBoolean messageConsumed = new AtomicBoolean(false);

		// Set queue length to 1, so that the RingBuffer can be easily full
		// to trigger consumer blocking
		DisruptorQueue queue = createQueue("consumerHang", ProducerType.MULTI,
				2);
		queue.consumerStarted();
		push(queue, 1);
		Runnable producer = new Producer(queue);
		Runnable consumer = new Consumer(queue, new EventHandler<Object>() {
			long count = 0;

			@Override
			public void onEvent(Object obj, long sequence, boolean endOfBatch)
					throws Exception {

				messageConsumed.set(true);
				System.out.println("Consume " + count++);
			}
		});

		run(producer, 0, 0, consumer, 50);
		Assert.assertTrue(
				"disruptor message is never consumed due to consumer thread hangs",
				messageConsumed.get());

		System.out
				.println("!!!!!!!!!!!!!End testBeforeStartConsumer!!!!!!!!!!");
	}

	@Test
	public void testSingleProducer() throws InterruptedException {
		System.out
				.println("!!!!!!!!!!!!!!Begin testSingleProducer!!!!!!!!!!!!!!");
		final AtomicBoolean messageConsumed = new AtomicBoolean(false);

		// Set queue length to 1, so that the RingBuffer can be easily full
		// to trigger consumer blocking
		DisruptorQueue queue = createQueue("consumerHang", ProducerType.SINGLE,
				1);
		push(queue, 1);
		Runnable producer = new Producer(queue);
		Runnable consumer = new Consumer(queue, new EventHandler<Object>() {
			long count = 0;

			@Override
			public void onEvent(Object obj, long sequence, boolean endOfBatch)
					throws Exception {

				messageConsumed.set(true);
				System.out.println("Consume " + count++);
			}
		});

		run(producer, 0, 0, consumer, 50);
		Assert.assertTrue(
				"disruptor message is never consumed due to consumer thread hangs",
				messageConsumed.get());

		System.out
				.println("!!!!!!!!!!!!!!End testSingleProducer!!!!!!!!!!!!!!");
	}

	public static AtomicLong produceNum = new AtomicLong(0);
	public static AtomicLong consumerNum = new AtomicLong(0);

	public static EventHandlerTest handler = new EventHandlerTest();

	public static void resetNum() {
		produceNum.set(0);
		consumerNum.set(0);
		handler.reset();

	}

	@Test
	public void testMessageDisorder() throws InterruptedException {

		System.out
				.println("!!!!!!!!!!!!!!!!Begin testMessageDisorder!!!!!!!!!!");
		// Set queue length to bigger enough
		DisruptorQueue queue = createQueue("messageOrder", ProducerType.MULTI,
				128);

		queue.publish("1");

		Runnable producer = new Producer(queue);

		final Object[] result = new Object[1];
		Runnable consumer = new Consumer(queue, new EventHandler<Object>() {
			private boolean head = true;
			private Map<String, Long> lastIdMap = new HashMap<String, Long>();

			@Override
			public void onEvent(Object obj, long sequence, boolean endOfBatch)
					throws Exception {
				consumerNum.incrementAndGet();
				if (head) {
					head = false;
					result[0] = obj;
				} else {
					String event = (String) obj;
					String[] item = event.split("@");
					Long current = Long.valueOf(item[1]);
					Long last = lastIdMap.get(item[0]);
					if (last != null) {
						if (current <= last) {
							String msg = "Consume disorder of " + item[0]
									+ ", current" + current + ",last:" + last;
							System.err
									.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
							System.err.println(msg);
							System.err
									.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
							Assert.fail(msg);
						}
					}

					lastIdMap.put(item[0], current);

				}
			}
		});

		run(producer, PRODUCER_NUM, 1000, consumer, 30000);
		Assert.assertEquals(
				"We expect to receive first published message first, but received "
						+ result[0], "1", result[0]);
		produceNum.incrementAndGet();
		Assert.assertEquals("produce: " + produceNum.get() + ", consume:"
				+ consumerNum.get(), produceNum.get(), consumerNum.get());
		System.out.println("!!!!!!!!!!!!!!End testMessageDisorder!!!!!!!!!!!!");
	}

	@Test
	public void testPull() {
		// @@@ TODO
	}

	@Test
	public void testTake() {
		// @@@ TODO
	}

	public void push(DisruptorQueue queue, int num) {
		for (int i = 0; i < num; i++) {
			String msg = String.valueOf(Thread.currentThread().getId()) + "@"
					+ i;
			try {
				queue.publish(msg, false);
			} catch (InsufficientCapacityException e) {
				e.printStackTrace();
			}
			produceNum.incrementAndGet();
			System.out.println(Thread.currentThread().getId()
					+ " Publish one :" + i);
		}
	}

	@Test
	public void testConsumeBatchWhenAvailable() {
		System.out
				.println("!!!!!!!!!!!!!!!Begin testConsumeBatchWhenAvailable!!!!!!!!!!!!");

		resetNum();

		// Set queue length to bigger enough
		DisruptorQueue queue = createQueue("messageOrder", ProducerType.MULTI,
				128);

		push(queue, 128);

		queue.consumeBatchWhenAvailable(handler);

		Assert.assertEquals("produce: " + produceNum.get() + ", consume:"
				+ consumerNum.get(), produceNum.get(), consumerNum.get());
		System.out
				.println("!!!!!! finish testConsumeBatchWhenAvailable test 1");
		resetNum();

		queue.consumerStarted();

		push(queue, 128);

		queue.consumeBatchWhenAvailable(handler);

		Assert.assertEquals("produce: " + produceNum.get() + ", consume:"
				+ consumerNum.get(), produceNum.get(), consumerNum.get());
		System.out
				.println("!!!!!! finish testConsumeBatchWhenAvailable test 2");

		System.out
				.println("!!!!!!!!!!!!!!!Finsh testConsumeBatchWhenAvailable for MULTI!!!!!!!!!!!!");

		resetNum();
		// Set queue length to bigger enough
		DisruptorQueue queue2 = createQueue("messageOrder",
				ProducerType.SINGLE, 128);

		push(queue2, 128);

		queue2.consumeBatchWhenAvailable(handler);

		Assert.assertEquals("produce: " + produceNum.get() + ", consume:"
				+ consumerNum.get(), produceNum.get(), consumerNum.get());
		System.out
				.println("!!!!!! finish testConsumeBatchWhenAvailable test 3");
		resetNum();

		queue2.consumerStarted();

		push(queue2, 128);

		queue2.consumeBatchWhenAvailable(handler);

		Assert.assertEquals("produce: " + produceNum.get() + ", consume:"
				+ consumerNum.get(), produceNum.get(), consumerNum.get());
		System.out
				.println("!!!!!! finish testConsumeBatchWhenAvailable test 4");

		System.out
				.println("!!!!!!!!!!!!!!!Finsh testConsumeBatchWhenAvailable for single !!!!!!!!!!!!");
		System.out
				.println("!!!!!!!!!!!!!End testConsumeBatchWhenAvailable!!!!!!!!!!!");
	}

	@Test
	public void testTryConsume() {
		System.out.println("!!!!!!!!!!!!Begin testTryConsume!!!!!!!!!!!!!!!!");

		resetNum();
		// Set queue length to bigger enough
		DisruptorQueue queue = createQueue("messageOrder", ProducerType.MULTI,
				128);

		push(queue, 128);

		queue.consumeBatch(handler);

		Assert.assertEquals("produce: " + produceNum.get() + ", consume:"
				+ consumerNum.get(), produceNum.get(), consumerNum.get());
		System.out.println("!!!!!! finish testTryConsume test 1");
		resetNum();

		queue.consumerStarted();

		push(queue, 128);

		queue.consumeBatch(handler);

		Assert.assertEquals("produce: " + produceNum.get() + ", consume:"
				+ consumerNum.get(), produceNum.get(), consumerNum.get());
		System.out.println("!!!!!! finish testTryConsume test 2");

		resetNum();
		// Set queue length to bigger enough
		DisruptorQueue queue2 = createQueue("messageOrder",
				ProducerType.SINGLE, 128);

		push(queue2, 128);

		queue2.consumeBatch(handler);

		Assert.assertEquals("produce: " + produceNum.get() + ", consume:"
				+ consumerNum.get(), produceNum.get(), consumerNum.get());
		System.out.println("!!!!!! finish testTryConsume test 3");
		resetNum();

		queue2.consumerStarted();

		push(queue2, 128);

		queue2.consumeBatch(handler);

		Assert.assertEquals("produce: " + produceNum.get() + ", consume:"
				+ consumerNum.get(), produceNum.get(), consumerNum.get());
		System.out.println("!!!!!! finish testTryConsume test 4");

		System.out.println("!!!!!!!!!!!!!!!!!End testTryConsume!!!!!!!!!!!!!!");
	}

	private void run(Runnable producer, int producerNum, long produceMs,
			Runnable consumer, long waitMs) {
		try {

			resetNum();

			Thread[] producerThreads = new Thread[producerNum];
			for (int i = 0; i < producerNum; i++) {
				producerThreads[i] = new Thread(producer);
				producerThreads[i].start();
			}

			Thread consumerThread = new Thread(consumer);
			consumerThread.start();
			System.out.println("Please wait seconds" + produceMs / 1000);

			Thread.sleep(produceMs);

			for (int i = 0; i < producerNum; i++) {
				producerThreads[i].interrupt();
				producerThreads[i].stop();
				producerThreads[i].join(TIMEOUT);
			}

			Thread.sleep(waitMs);
			System.out.println("Please wait seconds" + waitMs / 1000);

			consumerThread.interrupt();
			consumerThread.stop();
			consumerThread.join(TIMEOUT);
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	private class Producer implements Runnable {
		private String msg;
		private DisruptorQueue queue;

		Producer(DisruptorQueue queue) {
			this.queue = queue;
		}

		@Override
		public void run() {
			long count = 0;
			try {
				while (true) {

					String msg = String.valueOf(Thread.currentThread().getId())
							+ "@" + count;
					queue.publish(msg, false);
					produceNum.incrementAndGet();
					System.out.println(msg);
					count++;
				}
			} catch (InsufficientCapacityException e) {
				System.out.println(Thread.currentThread().getId()
						+ " quit, insufficientCapacityException " + count);
				return;
			}catch (Exception e) {
				System.out.println(Thread.currentThread().getId()
						+ " quit, Exception " + count);
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
				while (true) {
					queue.consumeBatchWhenAvailable(handler);
				}
			} catch (Exception e) {
				// break
			}
		}
	}

	static class EventHandlerTest implements EventHandler<Object> {
		private Map<String, Long> lastIdMap = new HashMap<String, Long>();

		public void reset() {
			lastIdMap.clear();
		}

		@Override
		public void onEvent(Object obj, long sequence, boolean endOfBatch)
				throws Exception {

			String event = (String) obj;
			String[] item = event.split("@");
			Long current = Long.valueOf(item[1]);
			Long last = lastIdMap.get(item[0]);
			if (last != null) {
				if (current <= last) {
					String msg = "Consume disorder of " + item[0] + ", current"
							+ current + ",last:" + last;
					System.err.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
					System.err.println(msg + "," + event);
					System.err.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
					Assert.fail(msg);
				}
			}

			lastIdMap.put(item[0], current);
			consumerNum.incrementAndGet();
		}
	};

	private static DisruptorQueue createQueue(String name, ProducerType type,
			int queueSize) {


		return DisruptorQueue.mkInstance(name, type, queueSize,
				new BlockingWaitStrategy());
	}
}

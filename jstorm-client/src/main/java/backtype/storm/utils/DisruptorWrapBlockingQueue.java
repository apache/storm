package backtype.storm.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import backtype.storm.metric.api.IStatefulObject;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * 
 * A single consumer queue that uses the LMAX Disruptor. They key to the
 * performance is the ability to catch up to the producer by processing tuples
 * in batches.
 */
public class DisruptorWrapBlockingQueue extends DisruptorQueue {
	private static final Logger LOG = Logger
			.getLogger(DisruptorWrapBlockingQueue.class);

	private static final long QUEUE_CAPACITY = 512;
	private LinkedBlockingDeque<Object> queue;

	private String queueName;

	public DisruptorWrapBlockingQueue(String queueName,
			ProducerType producerType, int bufferSize, WaitStrategy wait) {
		this.queueName = queueName;
		queue = new LinkedBlockingDeque<Object>();
	}

	public String getName() {
		return queueName;
	}

	// poll method
	public void consumeBatch(EventHandler<Object> handler) {
		consumeBatchToCursor(0, handler);
	}

	public void haltWithInterrupt() {
	}

	public Object poll() {
		return queue.poll();
	}

	public Object take() {
		try {
			return queue.take();
		} catch (InterruptedException e) {
			return null;
		}
	}
	
	public void drainQueue(Object object, EventHandler<Object> handler) {
		while (object != null) {
			try {
				handler.onEvent(object, 0, false);
				object = queue.poll();
			} catch (InterruptedException e) {
				LOG.warn("Occur interrupt error, " + object);
				break;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	public void consumeBatchWhenAvailable(EventHandler<Object> handler) {
		Object object = queue.poll();
		if (object == null) {
			try {
				object = queue.take();
			} catch (InterruptedException e) {
				LOG.warn("Occur interrupt error, " + object);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		drainQueue(object, handler);

	}

	public void consumeBatchToCursor(long cursor, EventHandler<Object> handler) {
		Object object = queue.poll();
		drainQueue(object, handler);
	}

	/*
	 * Caches until consumerStarted is called, upon which the cache is flushed
	 * to the consumer
	 */
	public void publish(Object obj) {
		boolean isSuccess = queue.offer(obj);
		while (isSuccess == false) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
			}
			isSuccess = queue.offer(obj);
		}

	}

	public void tryPublish(Object obj) throws InsufficientCapacityException {
		boolean isSuccess = queue.offer(obj);
		if (isSuccess == false) {
			throw InsufficientCapacityException.INSTANCE;
		}

	}

	public void publish(Object obj, boolean block)
			throws InsufficientCapacityException {
		if (block == true) {
			publish(obj);
		} else {
			tryPublish(obj);
		}
	}

	public void consumerStarted() {
	}

	private void flushCache() {
	}

	public void clear() {
		queue.clear();
	}

	public long population() {
		return queue.size();
	}

	public long capacity() {
		long used = queue.size();
		if (used < QUEUE_CAPACITY) {
			return QUEUE_CAPACITY;
		} else {
			return used;
		}
	}

	public long writePos() {
		return 0;
	}

	public long readPos() {
		return queue.size();
	}

	public float pctFull() {
		long used = queue.size();
		if (used < QUEUE_CAPACITY) {
			return (1.0F * used / QUEUE_CAPACITY);
		} else {
			return 1.0f;
		}
	}

	@Override
	public Object getState() {
		Map state = new HashMap<String, Object>();
		// get readPos then writePos so it's never an under-estimate
		long rp = readPos();
		long wp = writePos();
		state.put("capacity", capacity());
		state.put("population", wp - rp);
		state.put("write_pos", wp);
		state.put("read_pos", rp);
		return state;
	}

	public static class ObjectEventFactory implements
			EventFactory<MutableObject> {
		@Override
		public MutableObject newInstance() {
			return new MutableObject();
		}
	}

}

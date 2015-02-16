package backtype.storm.utils;

import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import backtype.storm.metric.api.IStatefulObject;
import backtype.storm.utils.disruptor.AbstractSequencerExt;
import backtype.storm.utils.disruptor.RingBuffer;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * 
 * A single consumer queue that uses the LMAX Disruptor. They key to the
 * performance is the ability to catch up to the producer by processing tuples
 * in batches.
 */
public class DisruptorQueueImpl extends DisruptorQueue {
	private static final Logger LOG = Logger.getLogger(DisruptorQueueImpl.class);
	static boolean useSleep = true;
	public static void setUseSleep(boolean useSleep) {
		AbstractSequencerExt.setWaitSleep(useSleep);
	}
	
	private static final Object FLUSH_CACHE = new Object();
	private static final Object INTERRUPT = new Object();
	private static final String PREFIX = "disruptor-";

	private final String _queueName;
	private final RingBuffer<MutableObject> _buffer;
	private final Sequence _consumer;
	private final SequenceBarrier _barrier;

	// TODO: consider having a threadlocal cache of this variable to speed up
	// reads?
	volatile boolean consumerStartedFlag = false;

	private final HashMap<String, Object> state = new HashMap<String, Object>(4);
	private final ConcurrentLinkedQueue<Object> _cache = new ConcurrentLinkedQueue<Object>();
	private final ReentrantReadWriteLock cacheLock = new ReentrantReadWriteLock();
	private final Lock readLock = cacheLock.readLock();
	private final Lock writeLock = cacheLock.writeLock();

	public DisruptorQueueImpl(String queueName, ProducerType producerType,
			int bufferSize, WaitStrategy wait) {
		this._queueName = PREFIX + queueName;
		_buffer = RingBuffer.create(producerType, new ObjectEventFactory(),
				bufferSize, wait);
		_consumer = new Sequence();
		_barrier = _buffer.newBarrier();
		_buffer.addGatingSequences(_consumer);
		if (producerType == ProducerType.SINGLE) {
			consumerStartedFlag = true;
		} else {
			// make sure we flush the pending messages in cache first
			if (bufferSize < 2) {
				throw new RuntimeException("QueueSize must >= 2");
			}
			try {
				publishDirect(FLUSH_CACHE, true);
			} catch (InsufficientCapacityException e) {
				throw new RuntimeException("This code should be unreachable!",
						e);
			}
		}
	}

	public String getName() {
		return _queueName;
	}

	public void consumeBatch(EventHandler<Object> handler) {
		consumeBatchToCursor(_barrier.getCursor(), handler);
	}

	public void haltWithInterrupt() {
		publish(INTERRUPT);
	}

	public Object poll() {
		// @@@
		// should use _cache.isEmpty, but it is slow
		// I will change the logic later
		if (consumerStartedFlag == false) {
			return _cache.poll();
		}

		final long nextSequence = _consumer.get() + 1;
		if (nextSequence <= _barrier.getCursor()) {
			MutableObject mo = _buffer.get(nextSequence);
			_consumer.set(nextSequence);
			Object ret = mo.o;
			mo.setObject(null);
			return ret;
		}
		return null;
	}

	public Object take() {
		// @@@
		// should use _cache.isEmpty, but it is slow
		// I will change the logic later
		if (consumerStartedFlag == false) {
			return _cache.poll();
		}

		final long nextSequence = _consumer.get() + 1;
		// final long availableSequence;
		try {
			_barrier.waitFor(nextSequence);
		} catch (AlertException e) {
			LOG.error(e.getCause(), e);
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			LOG.error("InterruptedException " + e.getCause());
			// throw new RuntimeException(e);
			return null;
		} catch (TimeoutException e) {
			LOG.error(e.getCause(), e);
			return null;
		}
		MutableObject mo = _buffer.get(nextSequence);
		_consumer.set(nextSequence);
		Object ret = mo.o;
		mo.setObject(null);
		return ret;
	}

	public void consumeBatchWhenAvailable(EventHandler<Object> handler) {
		try {
			final long nextSequence = _consumer.get() + 1;
			final long availableSequence = _barrier.waitFor(nextSequence);
			if (availableSequence >= nextSequence) {
				consumeBatchToCursor(availableSequence, handler);
			}
		} catch (AlertException e) {
			LOG.error(e.getCause(), e);
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			LOG.error("InterruptedException " + e.getCause());
			return;
		}catch (TimeoutException e) {
			LOG.error(e.getCause(), e);
			return ;
		}
	}

	public void consumeBatchToCursor(long cursor, EventHandler<Object> handler){
		for (long curr = _consumer.get() + 1; curr <= cursor; curr++) {
			try {
				MutableObject mo = _buffer.get(curr);
				Object o = mo.o;
				mo.setObject(null);
				if (o == FLUSH_CACHE) {
					Object c = null;
					while (true) {
						c = _cache.poll();
						if (c == null)
							break;
						else
							handler.onEvent(c, curr, true);
					}
				} else if (o == INTERRUPT) {
					throw new InterruptedException(
							"Disruptor processing interrupted");
				} else {
					handler.onEvent(o, curr, curr == cursor);
				}
			} catch (InterruptedException e) {
				// throw new RuntimeException(e);
				LOG.error(e.getCause());
				return;
			} catch (Exception e) {
				LOG.error(e.getCause(), e);
				throw new RuntimeException(e);
			}
		}
		// TODO: only set this if the consumer cursor has changed?
		_consumer.set(cursor);
	}

	/*
	 * Caches until consumerStarted is called, upon which the cache is flushed
	 * to the consumer
	 */
	public void publish(Object obj) {
		try {
			publish(obj, true);
		} catch (InsufficientCapacityException ex) {
			throw new RuntimeException("This code should be unreachable!");
		}
	}

	public void tryPublish(Object obj) throws InsufficientCapacityException {
		publish(obj, false);
	}

	public void publish(Object obj, boolean block)
			throws InsufficientCapacityException {

		boolean publishNow = consumerStartedFlag;

		if (!publishNow) {
			readLock.lock();
			try {
				publishNow = consumerStartedFlag;
				if (!publishNow) {
					_cache.add(obj);
				}
			} finally {
				readLock.unlock();
			}
		}

		if (publishNow) {
			publishDirect(obj, block);
		}
	}

	protected void publishDirect(Object obj, boolean block)
			throws InsufficientCapacityException {
		final long id;
		if (block) {
			id = _buffer.next();
		} else {
			id = _buffer.tryNext(1);
		}
		final MutableObject m = _buffer.get(id);
		m.setObject(obj);
		_buffer.publish(id);
	}

	public void consumerStarted() {

		writeLock.lock();
		consumerStartedFlag = true;
		
		writeLock.unlock();
	}

	public void clear() {
		while (population() != 0L) {
			poll();
		}
	}

	public long population() {
		return (writePos() - readPos());
	}

	public long capacity() {
		return _buffer.getBufferSize();
	}

	public long writePos() {
		return _buffer.getCursor();
	}

	public long readPos() {
		return _consumer.get();
	}

	public float pctFull() {
		return (1.0F * population() / capacity());
	}

	@Override
	public Object getState() {
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

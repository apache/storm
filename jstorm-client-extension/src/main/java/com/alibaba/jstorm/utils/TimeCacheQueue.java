package com.alibaba.jstorm.utils;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.Logger;

/**
 * Expires keys that have not been updated in the configured number of seconds.
 * The algorithm used will take between expirationSecs and expirationSecs * (1 +
 * 1 / (numBuckets-1)) to actually expire the message.
 * 
 * get, put, remove, containsKey, and size take O(numBuckets) time to run.
 * 
 * The advantage of this design is that the expiration thread only locks the
 * object for O(1) time, meaning the object is essentially always available for
 * poll/offer
 */
public class TimeCacheQueue<K> {
	// this default ensures things expire at most 50% past the expiration time
	public static final int DEFAULT_NUM_BUCKETS = 3;

	public static interface ExpiredCallback<K> {
		public void expire(K entry);
	}

	public static class DefaultExpiredCallback<K> implements ExpiredCallback<K> {
		protected static final Logger LOG = Logger
				.getLogger(TimeCacheQueue.DefaultExpiredCallback.class);

		protected String queueName;

		public DefaultExpiredCallback(String queueName) {
			this.queueName = queueName;
		}

		public void expire(K entry) {
			LOG.info("TimeCacheQueue " + queueName + " entry:" + entry
					+ ", timeout");
		}
	}

	protected LinkedList<LinkedBlockingDeque<K>> _buckets;

	protected final Object _lock = new Object();
	protected Thread _cleaner;
	protected ExpiredCallback _callback;

	public TimeCacheQueue(int expirationSecs, int numBuckets,
			ExpiredCallback<K> callback) {
		if (numBuckets < 2) {
			throw new IllegalArgumentException("numBuckets must be >= 2");
		}
		_buckets = new LinkedList<LinkedBlockingDeque<K>>();
		for (int i = 0; i < numBuckets; i++) {
			_buckets.add(new LinkedBlockingDeque<K>());
		}

		_callback = callback;
		final long expirationMillis = expirationSecs * 1000L;
		final long sleepTime = expirationMillis / (numBuckets - 1);
		_cleaner = new Thread(new Runnable() {
			public void run() {
				try {
					while (true) {
						LinkedBlockingDeque<K> dead = null;

						Thread.sleep(sleepTime);

						synchronized (_lock) {
							dead = _buckets.removeLast();
							_buckets.addFirst(new LinkedBlockingDeque<K>());
						}
						if (_callback != null) {
							for (K entry : dead) {
								_callback.expire(entry);
							}
						}
					}
				} catch (InterruptedException ex) {

				}
			}
		});
		_cleaner.setDaemon(true);
		_cleaner.start();
	}

	public TimeCacheQueue(int expirationSecs, ExpiredCallback<K> callback) {
		this(expirationSecs, DEFAULT_NUM_BUCKETS, callback);
	}

	public TimeCacheQueue(int expirationSecs) {
		this(expirationSecs, DEFAULT_NUM_BUCKETS, null);
	}

	public TimeCacheQueue(int expirationSecs, int numBuckets) {
		this(expirationSecs, numBuckets, null);
	}

	public boolean containsKey(K entry) {
		synchronized (_lock) {
			for (LinkedBlockingDeque<K> bucket : _buckets) {
				if (bucket.contains(entry)) {
					return true;
				}
			}
			return false;
		}
	}

	public K poll() {
		synchronized (_lock) {
			Iterator<LinkedBlockingDeque<K>> itor = _buckets
					.descendingIterator();
			while (itor.hasNext()) {
				LinkedBlockingDeque<K> bucket = itor.next();
				K entry = bucket.poll();
				if (entry != null) {
					return entry;
				}
			}

			return null;
		}
	}

	public void offer(K entry) {
		synchronized (_lock) {
			LinkedBlockingDeque<K> bucket = _buckets.getFirst();

			bucket.offer(entry);
		}
	}

	public void remove(K entry) {
		synchronized (_lock) {
			for (LinkedBlockingDeque<K> bucket : _buckets) {
				if (bucket.contains(entry)) {
					bucket.remove(entry);
					return;
				}
			}
			return;
		}
	}

	public int size() {
		synchronized (_lock) {
			int size = 0;
			for (LinkedBlockingDeque<K> bucket : _buckets) {
				size += bucket.size();
			}
			return size;
		}
	}

	@Override
	protected void finalize() throws Throwable {
		try {
			_cleaner.interrupt();
		} finally {
			super.finalize();
		}
	}

}

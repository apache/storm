package com.alibaba.jstorm.utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Expires keys that have not been updated in the configured number of seconds.
 * The algorithm used will take between expirationSecs and expirationSecs * (1 +
 * 1 / (numBuckets-1)) to actually expire the message.
 * 
 * get, put, remove, containsKey, and size take O(numBuckets) time to run.
 * 
 * 
 */
public class TimeCacheMap<K, V> implements TimeOutMap<K, V> {
	// this default ensures things expire at most 50% past the expiration time
	private static final int DEFAULT_NUM_BUCKETS = 3;

	private LinkedList<HashMap<K, V>> _buckets;

	private final Object _lock = new Object();
	private Thread _cleaner;
	private ExpiredCallback _callback;

	public TimeCacheMap(int expirationSecs, int numBuckets,
			ExpiredCallback<K, V> callback) {
		if (numBuckets < 2) {
			throw new IllegalArgumentException("numBuckets must be >= 2");
		}
		_buckets = new LinkedList<HashMap<K, V>>();
		for (int i = 0; i < numBuckets; i++) {
			_buckets.add(new HashMap<K, V>());
		}

		_callback = callback;
		final long expirationMillis = expirationSecs * 1000L;
		final long sleepTime = expirationMillis / (numBuckets - 1);
		_cleaner = new Thread(new Runnable() {
			public void run() {

				while (true) {
					Map<K, V> dead = null;
					JStormUtils.sleepMs(sleepTime);
					synchronized (_lock) {
						dead = _buckets.removeLast();
						_buckets.addFirst(new HashMap<K, V>());
					}
					if (_callback != null) {
						for (Entry<K, V> entry : dead.entrySet()) {
							_callback.expire(entry.getKey(), entry.getValue());
						}
					}
				}
			}
		});
		_cleaner.setDaemon(true);
		_cleaner.start();
	}

	public TimeCacheMap(int expirationSecs, ExpiredCallback<K, V> callback) {
		this(expirationSecs, DEFAULT_NUM_BUCKETS, callback);
	}

	public TimeCacheMap(int expirationSecs) {
		this(expirationSecs, DEFAULT_NUM_BUCKETS);
	}

	public TimeCacheMap(int expirationSecs, int numBuckets) {
		this(expirationSecs, numBuckets, null);
	}

	@Override
	public boolean containsKey(K key) {
		synchronized (_lock) {
			for (HashMap<K, V> bucket : _buckets) {
				if (bucket.containsKey(key)) {
					return true;
				}
			}
			return false;
		}
	}

	@Override
	public V get(K key) {
		synchronized (_lock) {
			for (HashMap<K, V> bucket : _buckets) {
				if (bucket.containsKey(key)) {
					return bucket.get(key);
				}
			}
			return null;
		}
	}
	
	@Override
	public void putHead(K key, V value) {
		synchronized (_lock) {
			_buckets.getFirst().put(key, value);
		}
	}

	@Override
	public void put(K key, V value) {
		synchronized (_lock) {
			Iterator<HashMap<K, V>> it = _buckets.iterator();
			HashMap<K, V> bucket = it.next();
			bucket.put(key, value);
			while (it.hasNext()) {
				bucket = it.next();
				bucket.remove(key);
			}
		}
	}

	@Override
	public Object remove(K key) {
		synchronized (_lock) {
			for (HashMap<K, V> bucket : _buckets) {
				if (bucket.containsKey(key)) {
					return bucket.remove(key);
				}
			}
			return null;
		}
	}

	@Override
	public int size() {
		synchronized (_lock) {
			int size = 0;
			for (HashMap<K, V> bucket : _buckets) {
				size += bucket.size();
			}
			return size;
		}
	}

	public void cleanup() {
		_cleaner.interrupt();
	}

	
}

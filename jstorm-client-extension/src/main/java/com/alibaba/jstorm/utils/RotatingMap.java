package com.alibaba.jstorm.utils;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

/**
 * Expires keys that have not been updated in the configured number of seconds.
 * The algorithm used will take between expirationSecs and
 * expirationSecs * (1 + 1 / (numBuckets-1)) to actually expire the message.
 *
 * get, put, remove, containsKey, and size take O(numBuckets) time to run.
 *
 * The advantage of this design is that the expiration thread only locks the object
 * for O(1) time, meaning the object is essentially always available for gets/puts.
 */
public class RotatingMap<K, V> {
    //this default ensures things expire at most 50% past the expiration time
    private static final int DEFAULT_NUM_BUCKETS = 3;

    public static interface ExpiredCallback<K, V> {
        public void expire(K key, V val);
    }

    private LinkedList<Map<K, V>> _buckets;

    private ExpiredCallback _callback;
    
    public RotatingMap(int numBuckets, ExpiredCallback<K, V> callback) {
        if(numBuckets<2) {
            throw new IllegalArgumentException("numBuckets must be >= 2");
        }
        _buckets = new LinkedList<Map<K, V>>();
        for(int i=0; i<numBuckets; i++) {
            _buckets.add(new ConcurrentHashMap<K, V>());
        }

        _callback = callback;
    }

    public RotatingMap(ExpiredCallback<K, V> callback) {
        this(DEFAULT_NUM_BUCKETS, callback);
    }

    public RotatingMap(int numBuckets) {
        this(numBuckets, null);
    }   
    
    public Map<K, V> rotate() {
        Map<K, V> dead = _buckets.removeLast();
        _buckets.addFirst(new ConcurrentHashMap<K, V>());
        if(_callback!=null) {
            for(Entry<K, V> entry: dead.entrySet()) {
                _callback.expire(entry.getKey(), entry.getValue());
            }
        }
        return dead;
    }

    public boolean containsKey(K key) {
        for(Map<K, V> bucket: _buckets) {
            if(bucket.containsKey(key)) {
                return true;
            }
        }
        return false;
    }

    public V get(K key) {
        for(Map<K, V> bucket: _buckets) {
            if(bucket.containsKey(key)) {
                return bucket.get(key);
            }
        }
        return null;
    }

    public void put(K key, V value) {
        Iterator<Map<K, V>> it = _buckets.iterator();
        Map<K, V> bucket = it.next();
        bucket.put(key, value);
        while(it.hasNext()) {
            bucket = it.next();
            bucket.remove(key);
        }
    }
    
    /**
     * In order to improving performance and avoid competition, 
     * just put entry to list head, 
     * 
     * Please make sure no duplicate key
     */
    public void putHead(K key, V value) {
        Map<K, V> bucket = _buckets.getFirst();
        bucket.put(key, value);
    }
    
    /**
     * Remove item from Rotate
     * 
     * On the side of performance, scanning from header is faster
     * On the side of logic, it should scan from the end to first.
     * 
     * @param key
     * @return
     */
    public Object remove(K key) {
        for(Map<K, V> bucket: _buckets) {
            Object value = bucket.remove(key);
            if(value != null) {
                return value;
            }
        }
        return null;
    }

    public int size() {
        int size = 0;
        for(Map<K, V> bucket: _buckets) {
            size+=bucket.size();
        }
        return size;
    }    
}

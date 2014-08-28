package com.alibaba.jstorm.utils;

public interface ExpiredCallback<K, V> {
	public void expire(K key, V val);
}

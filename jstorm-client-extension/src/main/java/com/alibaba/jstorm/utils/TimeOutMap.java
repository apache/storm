package com.alibaba.jstorm.utils;


public interface TimeOutMap<K, V> {
	
	public boolean containsKey(K key);

	public V get(K key);
	
	public void putHead(K key, V value);

	public void put(K key, V value);

	public Object remove(K key);

	public int size() ;

}

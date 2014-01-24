package com.alibaba.jstorm.resource;

public interface ResourcePool<K> {

	/**
	 * Check the alloc is available or not
	 * 
	 * @param slot
	 * @param context
	 * @return
	 */
	public boolean isAvailable(K alloc, Object context);

	/**
	 * Alloc one resource, return one resource, if unavailable return null
	 * 
	 * @param context
	 * @return
	 */
	public K alloc(Object context);

	public void free(K alloc, Object context);

	public int getTotalNum();

	public int getLeftNum();

	public int getUsedNum();

	public ResourceType getType();
}

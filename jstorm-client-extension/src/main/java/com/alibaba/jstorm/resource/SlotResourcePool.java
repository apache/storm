package com.alibaba.jstorm.resource;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.log4j.Logger;

public class SlotResourcePool<K> implements ResourcePool<K>, Serializable {
	private static final Logger LOG = Logger.getLogger(SlotResourcePool.class);

	/**  */
	private static final long serialVersionUID = -2998826654837064079L;

	private final ResourceType type;
	private final Map<K, Boolean> slots;

	public SlotResourcePool(final ResourceType type, Set<K> slotSet) {
		this.type = type;

		slots = new TreeMap();
		for (K slot : slotSet) {
			slots.put(slot, Boolean.valueOf(false));
		}

		LOG.info("Successfully init " + this.getClass().getSimpleName() + ":"
				+ this.toString());
	}

	@Override
	public ResourceType getType() {
		return type;
	}

	/**
	 * Alloc one slot, assigned by the SlotResourcePool
	 * 
	 * @param context
	 * @return
	 */
	@Override
	public K alloc(Object context) {

		synchronized (type) {
			for (Entry<K, Boolean> entry : slots.entrySet()) {
				if (entry.getValue() == true) {
					continue;
				}

				K slot = entry.getKey();

				slots.put(slot, Boolean.valueOf(true));

				LOG.debug(context + " alloc " + type + ":" + slot);

				return slot;
			}
		}

		return null;
	}

	/**
	 * Assign the slot, if the slot is available, return the slot if
	 * unavailable, return null
	 * 
	 * @param slot
	 * @param context
	 * @return
	 */
	public K alloc(K slot, Object context) {
		if (slot == null) {
			return null;
		}
		synchronized (type) {
			Boolean value = slots.get(slot);
			if (value == null) {
				LOG.info(type + " no this slot " + slot);
				return null;
			}

			if (value == true) {
				LOG.debug(type + " slot " + slot + " is assigned");
				return null;
			}

			slots.put(slot, Boolean.valueOf(true));
			LOG.debug(type + " assign " + slot + " to " + context);
			return slot;
		}

	}

	/**
	 * Check the slot is available or not
	 * 
	 * @param slot
	 * @param context
	 * @return
	 */
	@Override
	public boolean isAvailable(K slot, Object context) {
		if (slot == null) {
			return true;
		}

		synchronized (type) {
			Boolean value = slots.get(slot);
			if (value == null) {
				return false;
			}

			if (value == true) {
				LOG.debug(type + " slot " + slot + " is assigned," + context);
				return false;
			}

			return true;
		}
	}

	@Override
	public void free(K slot, Object context) {

		if (slot == null) {
			return;
		}

		LOG.debug(context + " free " + type + ":" + slot);

		synchronized (type) {
			Boolean value = slots.get(slot);
			if (value == null) {
				return;
			}
			slots.put(slot, Boolean.valueOf(false));
		}

		return;
	}

	@Override
	public int getLeftNum() {
		int freeSlotNum = 0;
		synchronized (type) {
			for (Entry<K, Boolean> entry : slots.entrySet()) {
				if (entry.getValue() == true) {
					continue;
				}

				freeSlotNum++;
			}
		}

		return freeSlotNum;
	}

	@Override
	public int getUsedNum() {
		return slots.size() - getLeftNum();
	}

	@Override
	public int getTotalNum() {
		return slots.size();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof SlotResourcePool<?> == false) {
			return false;
		}

		SlotResourcePool<K> otherPool = (SlotResourcePool<K>) obj;
		return otherPool.getType().equals(type)
				&& otherPool.slots.equals(slots);
	}

	@Override
	public int hashCode() {
		return type.hashCode() + slots.hashCode();
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
	}

}

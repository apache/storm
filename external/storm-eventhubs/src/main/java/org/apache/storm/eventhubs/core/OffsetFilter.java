package org.apache.storm.eventhubs.core;

public class OffsetFilter implements IEventFilter {
	String offset = null;

	public OffsetFilter(String offset) {
		this.offset = offset;
	}

	public String getOffset() {
		return offset;
	}

	@Override
	public String toString() {
		if (offset != null)
			return offset;

		return null;
	}
}

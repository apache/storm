package org.apache.storm.eventhubs.core;

import java.time.Instant;

public class TimestampFilter implements IEventFilter{	
	Instant time = null;
	
	public TimestampFilter(Instant time) {
		this.time = time;		
	}
		
	public Instant getTime() {
		return time;
	}

	@Override
	public String toString() {
		if (time != null)
			return Long.toString(time.toEpochMilli());

		return null;
	}
}

package org.apache.storm.eventhubs.spout;

import java.util.HashMap;

import com.microsoft.azure.eventhubs.EventData;

public class EventDataMock extends EventData {
	private static final long serialVersionUID = -1362022940535977850L;
	private SystemProperties sysprops;

	public EventDataMock(byte[] data, HashMap<String, Object> map) {
		super(data);
		this.sysprops = new SystemProperties(map);
		
		System.out.println("OFF: " + sysprops.getOffset());
		System.out.println("SEQ: " + sysprops.getSequenceNumber());
	}

	@Override
	public SystemProperties getSystemProperties() {
		return sysprops;
	}
}

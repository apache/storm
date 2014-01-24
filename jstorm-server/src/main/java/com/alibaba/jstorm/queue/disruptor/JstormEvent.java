package com.alibaba.jstorm.queue.disruptor;

import com.lmax.disruptor.EventFactory;

public class JstormEvent {

	private String msgId;

	public String getMsgId() {
		return msgId;
	}

	public void setMsgId(String msgId) {
		this.msgId = msgId;
	}

	public final static EventFactory<JstormEvent> EVENT_FACTORY = new EventFactory<JstormEvent>() {
		public JstormEvent newInstance() {
			return new JstormEvent();
		}
	};

}

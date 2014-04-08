package com.alibaba.jstorm.message.jeroMq;

import org.jeromq.ZMQ;

import backtype.storm.messaging.IConnection;

public class JZMQSendConnection implements IConnection {
	
	private ZMQ.Socket socket;
	private ZMQ.Poller outItems;
	private boolean closed = false;
	
	public JZMQSendConnection(ZMQ.Socket socket) {
		this.socket = socket;
	}

	@Override
	public byte[] recv(int flags) {
		throw new UnsupportedOperationException(
				"Client connection should not receive any messages");
	}

	@Override
	public void send(int taskId, byte[] message) {
//		ZMQ.Poller outItems;
//        outItems = context.poller();
//        outItems.register (sender, ZMQ.Poller.POLLOUT);
		socket.send(message);
		
	}

	@Override
	public void close() {
		socket.close();
		closed = true;
	}

	@Override
	public boolean isClosed() {
		return closed;
	}

}

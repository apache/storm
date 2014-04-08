package com.alibaba.jstorm.message.jeroMq;

import org.jeromq.ZMQ;

import backtype.storm.messaging.IConnection;

public class JZMQRecvConnection implements IConnection {

	private ZMQ.Socket socket;
	private boolean closed = false;
	
	public JZMQRecvConnection(ZMQ.Socket socket) {
		this.socket = socket;
	}
	
	@Override
	public byte[] recv(int flags) {
		return socket.recv(flags);
	}

	@Override
	public void send(int taskId, byte[] message) {
		throw new UnsupportedOperationException(
				"Server connection should not send any messages");
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

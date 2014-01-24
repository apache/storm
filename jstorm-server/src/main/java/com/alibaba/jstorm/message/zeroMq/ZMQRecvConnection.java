package com.alibaba.jstorm.message.zeroMq;

import org.zeromq.ZMQ.Socket;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;

/**
 * 
 * @author longda
 * 
 */
public class ZMQRecvConnection implements IConnection {
	private Socket socket;
	private boolean closed = false;

	public ZMQRecvConnection(Socket _socket) {
		socket = _socket;
	}

	@Override
	public byte[] recv(int flags) {
		return ZeroMq.recv(socket, flags);
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

	@Override
	public void send(int taskId, byte[] payload) {
		throw new UnsupportedOperationException(
				"Server connection should not send any messages");
	}

}

package com.alibaba.jstorm.message.zeroMq;

import org.zeromq.ZMQ.Socket;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;

/**
 * 
 * @author longda
 *
 */
public class ZMQSendConnection implements IConnection {
    private org.zeromq.ZMQ.Socket socket;
    private boolean closed = false;
    
    public ZMQSendConnection(Socket _socket) {
        socket = _socket;
    }
    
	@Override
	public void send(int taskId, byte[] message) {
		ZeroMq.send(socket, message);
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
	public byte[] recv(int flags) {
		throw new UnsupportedOperationException("Client connection should not receive any messages");
	}
    
}

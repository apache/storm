package com.alipay.dw.jstorm.zeroMq;

import org.zeromq.ZMQ.Socket;

/**
 * 
 * @author longda
 *
 */
public class ZMQSendConnection implements ISendConnection {
    private org.zeromq.ZMQ.Socket socket;
    private boolean closed = false;
    
    public ZMQSendConnection(Socket _socket) {
        socket = _socket;
    }
    
    @Override
    public void send(byte[] message) {
        
        ZeroMq.send(socket, message);
    }
    
    @Override
    public void close() {
        socket.close();
        closed = true;
    }

    @Override
    public boolean isClosed() {
        // TODO Auto-generated method stub
        return closed;
    }
    
}

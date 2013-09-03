package com.alipay.dw.jstorm.zeroMq;

import org.zeromq.ZMQ.Socket;

/**
 * 
 * @author longda
 *
 */
public class ZMQRecvConnection implements IRecvConnection {
    private Socket socket;
    private boolean closed = false;
    
    public ZMQRecvConnection(Socket _socket) {
        socket = _socket;
    }
    
    @Override
    public byte[] recv() {
        return ZeroMq.recv(socket);
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

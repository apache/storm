package com.alipay.dw.jstorm.zeroMq;

import java.util.concurrent.LinkedBlockingQueue;

//import org.zeromq.ZMQ.Socket;

/**
 * 
 * @author longda
 *
 */
public class QueueConnection implements IRecvConnection, ISendConnection {
    private LinkedBlockingQueue<byte[]> queue;
    private boolean                     closed = false;
    
    public QueueConnection() {
        queue = new LinkedBlockingQueue<byte[]>();
    }
    
    /**
     * QueueConnection 's recv is blocked type
     */
    @Override
    public byte[] recv() {
       
        try {
            return queue.take();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            return null;
        }
    }
    
    @Override
    public void send(byte[] message) {
        
        queue.offer(message);
    }
    
    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean isClosed() {
        // TODO Auto-generated method stub
        return closed;
    }
}

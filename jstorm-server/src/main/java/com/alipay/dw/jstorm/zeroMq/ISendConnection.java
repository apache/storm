package com.alipay.dw.jstorm.zeroMq;

/**
 * 
 * @author longda
 *
 */
public interface ISendConnection {
    
    public void send(byte[] message);
    
    public void close();
    
    public boolean isClosed();
}

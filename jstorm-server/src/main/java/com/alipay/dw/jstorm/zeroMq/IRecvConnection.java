package com.alipay.dw.jstorm.zeroMq;

/**
 * 
 * @author longda
 *
 */
public interface IRecvConnection {
    public byte[] recv();
    
    public void close();
    
    public boolean isClosed();
}

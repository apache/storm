package com.alipay.dw.jstorm.task.comm;

import org.apache.log4j.Logger;
import org.zeromq.ZMQ.Socket;

import backtype.storm.daemon.Shutdownable;

import com.alipay.dw.jstorm.callback.AsyncLoopThread;
import com.alipay.dw.jstorm.zeroMq.ISendConnection;
import com.alipay.dw.jstorm.zeroMq.MQContext;
import com.alipay.dw.jstorm.zeroMq.PacketPair;

/**
 * Shutdown VirtualPort Interface
 * 
 * @author yannian/Longda
 * 
 */
public class VirtualPortShutdown implements Shutdownable {
    private static final Logger LOG = Logger.getLogger(VirtualPortShutdown.class);
    
    protected MQContext         mqContext;
    protected Socket            kill_socket;
    protected AsyncLoopThread   vthread;
    protected int               port;
    
    public VirtualPortShutdown(MQContext mqContext, AsyncLoopThread vthread,
            int port) {
        this.mqContext = mqContext;
        this.vthread = vthread;
        this.port = port;
    }
    
    @Override
    public void shutdown() {
        
        ISendConnection sendConn = mqContext.connect(true, "localhost", port);
        sendConn.send(PacketPair.mk_packet((short)-1, new byte[0]));
        
        LOG.info("Waiting for virtual port at url " + port + " to die");
        
        try {
            vthread.join();
        } catch (InterruptedException e) {
            
        }
        
        LOG.info("Shutdown virtual port at url: " + port);
    }
}

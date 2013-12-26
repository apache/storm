package com.alibaba.jstorm.task.comm;

import org.apache.log4j.Logger;
import org.zeromq.ZMQ.Socket;

import backtype.storm.daemon.Shutdownable;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.serialization.KryoTupleSerializer;

import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.message.zeroMq.PacketPair;
import com.esotericsoftware.kryo.KryoSerializable;

/**
 * Shutdown VirtualPort Interface
 * 
 * @author yannian/Longda
 * 
 */
public class VirtualPortShutdown implements Shutdownable {
    private static final Logger LOG = Logger.getLogger(VirtualPortShutdown.class);
    
    protected String            topologyId;
    protected IContext          context;
    protected Socket            kill_socket;
    protected AsyncLoopThread   vthread;
    protected int               port;
    
    public VirtualPortShutdown(String topologyId, IContext context, 
    		AsyncLoopThread vthread, int port) {
    	this.topologyId = topologyId;
        this.context = context;
        this.vthread = vthread;
        this.port = port;
    }
    
    @Override
    public void shutdown() {
        
        IConnection sendConn = context.connect(topologyId, "localhost", port, true);
        sendConn.send(-1, KryoTupleSerializer.serialize(-1));
        
        LOG.info("Waiting for virtual port at url " + port + " to die");
        
        try {
            vthread.join();
        } catch (InterruptedException e) {
            
        }
        
        LOG.info("Shutdown virtual port at url: " + port);
    }
}

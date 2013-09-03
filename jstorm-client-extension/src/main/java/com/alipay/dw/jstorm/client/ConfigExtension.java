package com.alipay.dw.jstorm.client;

public class ConfigExtension {
    /**
     * if this configure has been set, 
     * the spout or bolt will log all receive tuples 
     * 
     * topology.debug will log all send tuples
     */
    public static final String TOPOLOGY_DEBUG_RECV_TUPLE = "topology.debug.recv.tuple";
    
    /**
     * delay spout to run nextTuple 
     */
    public static final String SPOUT_DELAY_RUN = "spout.delay.run";
    
    /**
     * max cache zeroMq message number, if there are more messages in the 
     * sending queue, some message will be directly throw
     */
    public static final String ZMQ_MAX_QUEUE_MSG = "zmq.max.queue.msg";
    
    public static final int   DEFAULT_ZMQ_MAX_QUEUE_MSG = 100000;
    
    
}

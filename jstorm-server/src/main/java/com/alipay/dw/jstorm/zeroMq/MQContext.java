package com.alipay.dw.jstorm.zeroMq;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.alipay.dw.jstorm.task.TaskStatus;

/**
 * zeroMQ context
 * 
 * @author yannian/Longda
 * 
 */
public class MQContext {
    private final static Logger           LOG         = Logger.getLogger(MQContext.class);
    private org.zeromq.ZMQ.Context        context;
    private int                           linger_ms;
    private boolean                       ipc;
    private boolean                       virtportZmq      = false;
    private int                           maxQueueMsg;
    private Map<Integer, QueueConnection> queueConnections = new HashMap<Integer, QueueConnection>();
    
    public static MQContext mk_zmq_context(int num_threads, int linger,
            boolean local, boolean virtportZmq, int maxQueueMsg) {
        Context context = ZeroMq.context(num_threads);
        return new MQContext(context, linger, local, virtportZmq, maxQueueMsg);
    }
    
    public MQContext(org.zeromq.ZMQ.Context _context, int _linger_ms,
            boolean _ipc, boolean virtportZmq, int maxQueueMsg) {
        context = _context;
        linger_ms = _linger_ms;
        ipc = _ipc;
        this.virtportZmq = virtportZmq;
        this.maxQueueMsg = maxQueueMsg;
    }
    
    /**
     * Create receive data connection
     * 
     * @param distributeZmq
     *            -- receive other worker's data
     * @param virtportZmq
     *            true -- receive current worker's data through zmq
     * @param port
     * @return
     */
    public IRecvConnection bind(boolean distributeZmq, int port) {
        if (distributeZmq || virtportZmq) {
            return zmq_bind(distributeZmq, port);
        } else {
            return queue_bind(port);
        }
    }
    
    public IRecvConnection zmq_bind(boolean distributeZmq, int port) {
        String url = null;
        if (distributeZmq) {
            if (ipc) {
                url = "ipc://" + port + ".ipc";
            } else {
                url = "tcp://*:" + port;
            }
        } else {
            //virtportZmq will be true
            url = "inproc://" + port;
        }
        Socket socket = ZeroMq.socket(context, ZeroMq.pull);
        
        ZeroMq.bind(socket, url);
        ZeroMq.set_hwm(socket, maxQueueMsg);
        
        //ZeroMq.subscribe(socket);
        
        LOG.info("Create zmq receiver " + url);
        return new ZMQRecvConnection(socket);
    }
    
    public IRecvConnection queue_bind(int port) {
        //        String url = "inproc://" + port;
        //        Socket socket = ZeroMq.socket(context, ZeroMq.pull);
        //        ZeroMq.bind(socket, url);
        //        
        //        return new ZMQSendConnection(socket);
        return queue_connect(port);
    }
    
    /**
     * Create connection to send data
     */
    public ISendConnection connect(boolean distributeZmq, String host, int port) {
        
        if (distributeZmq || virtportZmq) {
            return zmq_connect(distributeZmq, host, port);
        } else {
            return queue_connect(port);
        }
        
    }
    
    public ISendConnection zmq_connect(boolean distributeZmq, String host,
            int port) {
        String url = null;
        
        if (distributeZmq) {
            if (ipc) {
                url = "ipc://" + port + ".ipc";
            } else {
                url = "tcp://" + host + ":" + port;
            }
        } else {
            //virtportZmq will be true
            url = "inproc://" + port;
        }
        
        Socket socket = ZeroMq.socket(context, ZeroMq.push);
        socket = ZeroMq.set_linger(socket, linger_ms);
        socket = ZeroMq.connect(socket, url);
        ZeroMq.set_hwm(socket, maxQueueMsg);
        
        LOG.info("Create zmq sender " + url);
        return new ZMQSendConnection(socket);
    }
    
    public QueueConnection queue_connect(int port) {
        QueueConnection queueConnection = null;
        synchronized (this) {
            queueConnection = queueConnections.get(port);
            if (queueConnection == null) {
                queueConnection = new QueueConnection();
                
                queueConnections.put(port, queueConnection);
                
                LOG.info("Create internal queue connect " + port);
            }
        }
        
        return queueConnection;
    }
    
    public void term() {
        context.term();
    }
    
    public Context getContext() {
        return context;
    }
    
}

package com.alipay.dw.jstorm.daemon.worker;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.alipay.dw.jstorm.callback.RunnableCallback;
import com.alipay.dw.jstorm.common.JStormUtils;
import com.alipay.dw.jstorm.common.NodePort;
import com.alipay.dw.jstorm.utils.RunCounter;
import com.alipay.dw.jstorm.zeroMq.ISendConnection;

/**
 * 
 * Tuple sender
 * 
 * @author yannian
 * 
 */
public class DrainerRunable extends RunnableCallback {
    private final static Logger                          LOG = Logger.getLogger(DrainerRunable.class);
    
    private LinkedBlockingQueue<TransferData>            transferQueue;
    private ConcurrentHashMap<NodePort, ISendConnection> nodeportSocket;
    private ConcurrentHashMap<Integer, NodePort>         taskNodeport;
    private RunCounter drainerCounter = new RunCounter("DrainerRunable", DrainerRunable.class);
    
    public DrainerRunable(WorkerData workerData) {
        this.transferQueue = workerData.getTransferQueue();
        this.nodeportSocket = workerData.getNodeportSocket();
        this.taskNodeport = workerData.getTaskNodeport();
    }
    
//    @Override
//    public void run() {
//        try {
//            TransferData felem = transferQueue.take();
//            if (felem == null) {
//                return;
//            }
//            
//            ArrayList<TransferData> drainer = new ArrayList<TransferData>();
//            drainer.add(felem);
//            
//            transferQueue.drainTo(drainer);
//            for (TransferData o : drainer) {
//                int taskId = o.getTaskid();
//                byte[] tuple = o.getData();
//                
//                NodePort nodePort = taskNodeport.get(taskId);
//                if (nodePort == null) {
//                    String errormsg = "can`t not found IConnection";
//                    LOG.warn("DrainerRunable warn", new Exception(errormsg));
//                    continue;
//                }
//                ISendConnection conn = nodeportSocket.get(nodePort);
//                if (conn == null) {
//                    String errormsg = "can`t not found nodePort";
//                    LOG.warn("DrainerRunable warn", new Exception(errormsg));
//                    continue;
//                }
//                
//                conn.send(PacketPair.mk_packet(taskId, tuple));
//            }
//            drainer.clear();
//            
//        } catch (Exception e) {
//            LOG.error("DrainerRunable send error", e);
//        }
//    }
    
    @Override
    public void run() {
        try {
            while(true) {
                TransferData felem = transferQueue.take();
                if (felem == null) {
                    return;
                }
                
                long before = System.currentTimeMillis();
                
                int taskId = felem.getTaskid();
                byte[] tuple = felem.getData();
                
                NodePort nodePort = taskNodeport.get(taskId);
                if (nodePort == null) {
                    String errormsg = "can`t not found IConnection";
                    LOG.warn("DrainerRunable warn", new Exception(errormsg));
                    continue;
                }
                ISendConnection conn = nodeportSocket.get(nodePort);
                if (conn == null) {
                    String errormsg = "can`t not found nodePort";
                    LOG.warn("DrainerRunable warn", new Exception(errormsg));
                    continue;
                }
                
                if (conn.isClosed() == true) {
                    // if connection has been closed, just skip the package 
                    continue;
                }
                
                conn.send(felem.getData());
                
                long after = System.currentTimeMillis();
                drainerCounter.count(after - before);
            }
            
        } catch (Exception e) {
            LOG.error("DrainerRunable send error", e);
        }
    }
    
    @Override
    public Object getResult() {
        return 0;
    }
    
}

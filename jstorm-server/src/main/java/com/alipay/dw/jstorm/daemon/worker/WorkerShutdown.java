package com.alipay.dw.jstorm.daemon.worker;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import backtype.storm.daemon.Shutdownable;

import com.alipay.dw.jstorm.callback.AsyncLoopThread;
import com.alipay.dw.jstorm.cluster.ClusterState;
import com.alipay.dw.jstorm.cluster.StormClusterState;
import com.alipay.dw.jstorm.common.NodePort;
import com.alipay.dw.jstorm.task.ShutdownableDameon;
import com.alipay.dw.jstorm.task.TaskShutdownDameon;
import com.alipay.dw.jstorm.zeroMq.ISendConnection;
import com.alipay.dw.jstorm.zeroMq.MQContext;

/**
 * Shutdown worker
 * 
 * @author yannian/Longda
 * 
 */
public class WorkerShutdown implements ShutdownableDameon {
    private static Logger                                LOG = Logger.getLogger(WorkerShutdown.class);
    
    private List<TaskShutdownDameon>                     shutdowntasks;
    private AtomicBoolean                                active;
    private ConcurrentHashMap<NodePort, ISendConnection> nodeportSocket;
    private Shutdownable                                 virtualPortShutdown;
    private MQContext                                    mq_context;
    private AsyncLoopThread[]                            threads;
    private StormClusterState                            zkCluster;
    private ClusterState                                 cluster_state;
    
    //active nodeportSocket mqContext zkCluster  zkClusterstate
    public WorkerShutdown(WorkerData workerData,
            List<TaskShutdownDameon> _shutdowntasks,
            Shutdownable _virtual_port_shutdown, AsyncLoopThread[] _threads) {
        
        this.shutdowntasks = _shutdowntasks;
        this.virtualPortShutdown = _virtual_port_shutdown;
        this.threads = _threads;
        
        this.active = workerData.getActive();
        this.nodeportSocket = workerData.getNodeportSocket();
        this.mq_context = workerData.getMqContext();
        this.zkCluster = workerData.getZkCluster();
        this.cluster_state = workerData.getZkClusterstate();
        
        Runtime.getRuntime().addShutdownHook(new Thread(this));
    }
    
    @Override
    public void shutdown() {
        active.set(false);
        
        // shutdown tasks 
        for (ShutdownableDameon task : shutdowntasks) {
            task.shutdown();
        }
        
        // send data to close connection
        for (NodePort k : nodeportSocket.keySet()) {
            ISendConnection value = nodeportSocket.get(k);
            value.close();
        }
        
        virtualPortShutdown.shutdown();
        mq_context.term();
        
        // shutdown worker's demon thread
        // refreshconn, refreshzk, hb, drainer 
        for (AsyncLoopThread t : threads) {
            t.interrupt();
            try {
                t.join();
            } catch (InterruptedException e) {
                LOG.error("join thread", e);
            }
        }
        
        // close ZK client
        try {
            zkCluster.disconnect();
            cluster_state.close();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.info("Shutdown error,", e);
        }
        
    }
    
    public void join() throws InterruptedException {
        for (TaskShutdownDameon task : shutdowntasks) {
            task.join();
        }
        for (AsyncLoopThread t : threads) {
            t.join();
        }
        
    }
    
    public boolean waiting() {
        Boolean isExistsWait = false;
        for (ShutdownableDameon task : shutdowntasks) {
            if (task.waiting()) {
                isExistsWait = true;
                break;
            }
        }
        for (AsyncLoopThread thr : threads) {
            if (thr.isSleeping()) {
                isExistsWait = true;
                break;
            }
        }
        return isExistsWait;
    }
    
    @Override
    public void run() {
        // TODO Auto-generated method stub
        shutdown();
    }
    
}

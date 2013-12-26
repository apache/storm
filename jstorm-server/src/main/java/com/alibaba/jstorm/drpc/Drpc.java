package com.alibaba.jstorm.drpc;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import backtype.storm.Config;
import backtype.storm.daemon.Shutdownable;
import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.generated.DRPCRequest;
import backtype.storm.generated.DistributedRPC;
import backtype.storm.generated.DistributedRPCInvocations;

import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeUtils;

/**
 * Drpc 
 * 
 * @author yannian
 * 
 */
public class Drpc implements DistributedRPC.Iface,
        DistributedRPCInvocations.Iface, Shutdownable {
    
    private static final Logger LOG = Logger.getLogger(Drpc.class);
    
    public static void main(String[] args) throws TTransportException {
        Map conf = StormConfig.read_storm_config();
        final Drpc service = new Drpc();
        int port = JStormUtils.parseInt(conf.get(Config.DRPC_PORT));
        TNonblockingServerSocket socket = new TNonblockingServerSocket(port);
        THsHaServer.Args targs = new THsHaServer.Args(socket);
        targs.workerThreads(64);
        targs.protocolFactory(new TBinaryProtocol.Factory());
        targs.processor(new DistributedRPC.Processor<DistributedRPC.Iface>(
                service));
        
        final THsHaServer handler_server = new THsHaServer(targs);
        
        int portinvoke = JStormUtils.parseInt(conf
                .get(Config.DRPC_INVOCATIONS_PORT));
        TNonblockingServerSocket socketInvoke = new TNonblockingServerSocket(
                portinvoke);
        THsHaServer.Args targsInvoke = new THsHaServer.Args(socketInvoke);
        targsInvoke.workerThreads(64);
        targsInvoke.protocolFactory(new TBinaryProtocol.Factory());
        targsInvoke
                .processor(new DistributedRPCInvocations.Processor<DistributedRPCInvocations.Iface>(
                        service));
        
        final THsHaServer invoke_server = new THsHaServer(targsInvoke);
        
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                service.shutdown();
                handler_server.stop();
                invoke_server.stop();
            }
            
        });
        
        LOG.info("Starting Distributed RPC servers...");
        new Thread(new Runnable() {
            @Override
            public void run() {
                invoke_server.serve();
            }
        }).start();
        handler_server.serve();
    }
    
    private AtomicInteger                                                 ctr           = new AtomicInteger(
                                                                                                0);
    private ConcurrentHashMap<String, Semaphore>                          idtoSem       = new ConcurrentHashMap<String, Semaphore>();
    private ConcurrentHashMap<String, Object>                             idtoResult    = new ConcurrentHashMap<String, Object>();
    private ConcurrentHashMap<String, Integer>                            idtoStart     = new ConcurrentHashMap<String, Integer>();
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<DRPCRequest>> requestQueues = new ConcurrentHashMap<String, ConcurrentLinkedQueue<DRPCRequest>>();
    
    private void cleanup(String id) {
        LOG.debug("clean id " + id + " @ " + (System.currentTimeMillis()));
        
        idtoSem.remove(id);
        idtoResult.remove(id);
        idtoStart.remove(id);
    }
    
    AsyncLoopThread clearthread;
    
    public Drpc() {
        clearthread = this.clearThread();
    }
    
    public class clearThreadcall extends RunnableCallback {
        private static final int          REQUEST_TIMEOUT_SECS = 60;
        private static final int          TIMEOUT_CHECK_SECS   = 5;

        
        @Override
        public void run() {

            for (Entry<String, Integer> e : Drpc.this.idtoStart.entrySet()) {
                if (TimeUtils.time_delta(e.getValue()) > clearThreadcall.REQUEST_TIMEOUT_SECS) {
                    String id = e.getKey();
                    Drpc.this.idtoResult.put(id, new DRPCExecutionException(
                            "Request timed out"));
                    Semaphore s = Drpc.this.idtoSem.get(id);
                    if (s != null) {
                        s.release();
                    }
                    Drpc.this.cleanup(id);
                }
            }
            
            
        }
        
        public Object getResult() {
            return clearThreadcall.TIMEOUT_CHECK_SECS;
        }
    }
    
    private AsyncLoopThread clearThread() {
        return new AsyncLoopThread(new clearThreadcall());
    }
    
    @Override
    public String execute(String function, String args)
            throws DRPCExecutionException, TException {
        LOG.debug("Received DRPC request for " + function + " " + args + " at "
                + (System.currentTimeMillis()));
        int idinc = this.ctr.incrementAndGet();
        int maxvalue = 1000000000;
        int newid = idinc % maxvalue;
        if (idinc != newid) {
            this.ctr.compareAndSet(idinc, newid);
        }
        
        String strid = String.valueOf(newid);
        Semaphore sem = new Semaphore(0);
        
        DRPCRequest req = new DRPCRequest(args, strid);
        this.idtoStart.put(strid, TimeUtils.current_time_secs());
        this.idtoSem.put(strid, sem);
        ConcurrentLinkedQueue<DRPCRequest> queue = this.acquire_queue(
                this.requestQueues, function);
        queue.add(req);
        LOG.debug("Waiting for DRPC request for " + function + " " + args
                + " at " + (System.currentTimeMillis()));
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            LOG.error("acquire fail ", e);
        }
        LOG.debug("Acquired for DRPC request for " + function + " " + args
                + " at " + (System.currentTimeMillis()));
        
        Object result = this.idtoResult.get(strid);
        LOG.debug("Returning for DRPC request for " + function + " " + args
                + " at " + (System.currentTimeMillis()));
        
        this.cleanup(strid);
        
        if (result instanceof DRPCExecutionException) {
            throw (DRPCExecutionException) result;
        }
        return String.valueOf(result);
    }
    
    @Override
    public void result(String id, String result) throws TException {
        Semaphore sem = this.idtoSem.get(id);
        LOG.debug("Received result " + result + " for id " + id + " at "
                + (System.currentTimeMillis()));
        if (sem != null) {
            this.idtoResult.put(id, result);
            sem.release();
        }
        
    }
    
    @Override
    public DRPCRequest fetchRequest(String functionName) throws TException {
        
        ConcurrentLinkedQueue<DRPCRequest> queue = this.acquire_queue(
                this.requestQueues, functionName);
        DRPCRequest req = queue.poll();
        if (req != null) {
            LOG.debug("Fetched request for " + functionName + " at "
                    + (System.currentTimeMillis()));
            return req;
        }
        return new DRPCRequest("", "");
    }
    
    @Override
    public void failRequest(String id) throws TException {
        Semaphore sem = this.idtoSem.get(id);
        LOG.debug("failRequest result  for id " + id + " at "
                + (System.currentTimeMillis()));
        if (sem != null) {
            this.idtoResult.put(id,
                    new DRPCExecutionException("Request failed"));
            sem.release();
        }
    }
    
    @Override
    public void shutdown() {
        this.clearthread.interrupt();
    }
    
    private ConcurrentLinkedQueue<DRPCRequest> acquire_queue(
            ConcurrentHashMap<String, ConcurrentLinkedQueue<DRPCRequest>> queues,
            String function) {
        if (!queues.containsKey(function)) {
            queues.putIfAbsent(function,
                    new ConcurrentLinkedQueue<DRPCRequest>());
        }
        return queues.get(function);
    }
}

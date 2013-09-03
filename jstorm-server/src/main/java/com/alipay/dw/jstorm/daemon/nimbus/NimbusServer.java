package com.alipay.dw.jstorm.daemon.nimbus;

import java.io.IOException;
import java.nio.channels.Channel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import backtype.storm.Config;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.Nimbus.Iface;
import backtype.storm.utils.BufferFileInputStream;
import backtype.storm.utils.TimeCacheMap;
import backtype.storm.utils.Utils;

import com.alipay.dw.jstorm.cluster.StormConfig;
import com.alipay.dw.jstorm.common.JStormUtils;
import com.alipay.dw.jstorm.schedule.CleanRunnable;
import com.alipay.dw.jstorm.schedule.MonitorRunnable;

/**
 * 
 * NimbusServer work flow: 1. cleanup interrupted topology delete
 * /storm-local-dir/nimbus/topologyid/stormdis delete
 * /storm-zk-root/storms/topologyid
 * 
 * 2. set /storm-zk-root/storms/topology stats as run
 * 
 * 3. start one thread, every nimbus.monitor.reeq.secs set
 * /storm-zk-root/storms/ all topology as monitor. when the topology's status is
 * monitor, nimubs would reassign workers 4. start one threa, every
 * nimubs.cleanup.inbox.freq.secs cleanup useless jar
 * 
 * @author
 *         version 1: Nathan Marz
 *         version 2: Lixin/Chenjun
 *         version 3: Longda
 * 
 */
public class NimbusServer {
    
    private static final Logger LOG = Logger.getLogger(NimbusServer.class);
    
    private NimbusData          data;
    
    private ServiceHandler      serviceHandler;
    
    private TopologyAssign      topologyAssign;
    
    private THsHaServer         thriftServer;
    
    public static void main(String[] args) throws Exception {
        // read configuration files
        @SuppressWarnings("rawtypes")
        Map config = Utils.readStormConfig();
        
        NimbusServer instance = new NimbusServer();
        
        instance.launchServer(config);
        
    }
    
    @SuppressWarnings("rawtypes")
    private void launchServer(Map conf) throws Exception {
        LOG.info("Begin to start nimbus with conf " + conf);
        
        // 1. check whether mode is distributed or not
        StormConfig.validate_distributed_mode(conf);
        
        initShutdownHook();
        
        data = createNimbusData(conf);
        
        NimbusUtils.cleanupCorruptTopologies(data);
        
        initTopologyAssign();
        
        initTopologyStatus();
        
        initMonitor(conf);
        
        initCleaner(conf);
        
        serviceHandler = new ServiceHandler(data);
        
        initThrift(conf);
        
    }
    
    @SuppressWarnings("rawtypes")
    private NimbusData createNimbusData(Map conf) throws Exception {
        
        TimeCacheMap.ExpiredCallback<Object, Object> expiredCallback = new TimeCacheMap.ExpiredCallback<Object, Object>() {
            @Override
            public void expire(Object key, Object val) {
                try {
                    LOG.info("Close file " + String.valueOf(key));
                    if (val != null) {
                        if (val instanceof Channel) {
                            Channel channel = (Channel) val;
                            channel.close();
                        } else if (val instanceof BufferFileInputStream) {
                            BufferFileInputStream is = (BufferFileInputStream) val;
                            is.close();
                        }
                    }
                } catch (IOException e) {
                    LOG.error(e.getMessage(), e);
                }
                
            }
        };
        
        int file_copy_expiration_secs = JStormUtils.parseInt(
                conf.get(Config.NIMBUS_FILE_COPY_EXPIRATION_SECS), 30);
        TimeCacheMap<Object, Object> uploaders = new TimeCacheMap<Object, Object>(
                file_copy_expiration_secs, expiredCallback);
        TimeCacheMap<Object, Object> downloaders = new TimeCacheMap<Object, Object>(
                file_copy_expiration_secs, expiredCallback);
        
        // Callback callback=new TimerCallBack();
        // StormTimer timer=Timer.mkTimerTimer(callback);
        NimbusData data = new NimbusData(conf, downloaders, uploaders);
        
        return data;
        
    }
    
    private void initTopologyAssign() {
        topologyAssign = TopologyAssign.getInstance();
        topologyAssign.init(data);
    }
    
    private void initTopologyStatus() throws Exception {
        // get active topology in ZK
        List<String> active_ids = data.getStormClusterState().active_storms();
        
        if (active_ids != null) {
            
            for (String topologyid : active_ids) {
                // set the topology status as startup
                // in fact, startup won't change anything
                NimbusUtils.transition(data, topologyid, false,
                        StatusType.startup);
            }
            
        }
        
        LOG.info("Successfully init topology status");
    }
    
    @SuppressWarnings("rawtypes")
    private void initMonitor(Map conf) {
        final ScheduledExecutorService scheduExec = data.getScheduExec();
        
        // Schedule Nimbus monitor
        MonitorRunnable r1 = new MonitorRunnable(data);
        
        int monitor_freq_secs = JStormUtils.parseInt(
                conf.get(Config.NIMBUS_MONITOR_FREQ_SECS), 10);
        scheduExec.scheduleAtFixedRate(r1, 0, monitor_freq_secs,
                TimeUnit.SECONDS);
        
        LOG.info("Successfully init Monitor thread");
    }
    
    /**
     * Right now, every 600 seconds, nimbus will clean jar under
     * /LOCAL-DIR/nimbus/inbox, which is the uploading topology directory
     * 
     * @param conf
     * @throws IOException
     */
    @SuppressWarnings("rawtypes")
    private void initCleaner(Map conf) throws IOException {
        final ScheduledExecutorService scheduExec = data.getScheduExec();
        
        // Schedule Nimbus inbox cleaner/nimbus/inbox jar
        String dir_location = StormConfig.masterInbox(conf);
        int inbox_jar_expiration_secs = JStormUtils.parseInt(
                conf.get(Config.NIMBUS_INBOX_JAR_EXPIRATION_SECS), 3600);
        CleanRunnable r2 = new CleanRunnable(dir_location,
                inbox_jar_expiration_secs);
        
        int cleanup_inbox_freq_secs = JStormUtils.parseInt(
                conf.get(Config.NIMBUS_CLEANUP_INBOX_FREQ_SECS), 600);
        
        scheduExec.scheduleAtFixedRate(r2, 0, cleanup_inbox_freq_secs,
                TimeUnit.SECONDS);
        
        LOG.info("Successfully init " + dir_location + " cleaner");
    }
    
    @SuppressWarnings("rawtypes")
    private void initThrift(Map conf) throws TTransportException {
        Integer thrift_port = (Integer) conf.get(Config.NIMBUS_THRIFT_PORT);
        TNonblockingServerSocket socket = new TNonblockingServerSocket(
                thrift_port);
        THsHaServer.Args args = new THsHaServer.Args(socket);
        args.workerThreads(ServiceHandler.THREAD_NUM);
        args.protocolFactory(new TBinaryProtocol.Factory());
        
        args.processor(new Nimbus.Processor<Iface>(serviceHandler));
        thriftServer = new THsHaServer(args);
        
        LOG.info("Successfully started nimbus: started Thrift server...");
        thriftServer.serve();
    }
    
    private void initShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                NimbusServer.this.cleanup();
            }
            
        });
    }
    
    private void cleanup() {
        if (serviceHandler != null) {
            serviceHandler.shutdown();
        }
        
        if (topologyAssign != null) {
            topologyAssign.cleanup();
        }
        
        if (data != null) {
            data.cleanup();
        }
        
        if (thriftServer != null) {
            thriftServer.stop();
        }
        
        LOG.info("Successfully shutdown nimbus");
        
    }
    
}

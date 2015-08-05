/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.daemon.nimbus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.Nimbus.Iface;
import backtype.storm.scheduler.INimbus;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.callback.AsyncLoopRunnable;
import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.daemon.supervisor.Httpserver;
import com.alibaba.jstorm.daemon.worker.hearbeat.SyncContainerHb;
import com.alibaba.jstorm.metric.SimpleJStormMetric;
import com.alibaba.jstorm.schedule.CleanRunnable;
import com.alibaba.jstorm.schedule.FollowerRunnable;
import com.alibaba.jstorm.schedule.MonitorRunnable;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;

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
 * @author version 1: Nathan Marz version 2: Lixin/Chenjun version 3: Longda
 * 
 */
public class NimbusServer {

    private static final Logger LOG = LoggerFactory
            .getLogger(NimbusServer.class);

    private NimbusData data;

    private ServiceHandler serviceHandler;

    private TopologyAssign topologyAssign;

    private THsHaServer thriftServer;

    private FollowerRunnable follower;

    private Httpserver hs;

    private List<AsyncLoopThread> smartThreads =
            new ArrayList<AsyncLoopThread>();

    public static void main(String[] args) throws Exception {
        // read configuration files
        @SuppressWarnings("rawtypes")
        Map config = Utils.readStormConfig();

        JStormServerUtils.startTaobaoJvmMonitor();

        NimbusServer instance = new NimbusServer();

        INimbus iNimbus = new DefaultInimbus();

        instance.launchServer(config, iNimbus);

    }

    private void createPid(Map conf) throws Exception {
        String pidDir = StormConfig.masterPids(conf);

        JStormServerUtils.createPid(pidDir);
    }

    @SuppressWarnings("rawtypes")
    private void launchServer(final Map conf, INimbus inimbus) {
        LOG.info("Begin to start nimbus with conf " + conf);

        try {
            // 1. check whether mode is distributed or not
            StormConfig.validate_distributed_mode(conf);

            createPid(conf);

            initShutdownHook();

            inimbus.prepare(conf, StormConfig.masterInimbus(conf));

            data = createNimbusData(conf, inimbus);

            initFollowerThread(conf);

            int port = ConfigExtension.getNimbusDeamonHttpserverPort(conf);
            hs = new Httpserver(port, conf);
            hs.start();

            initContainerHBThread(conf);

            while (!data.isLeader())
                Utils.sleep(5000);

            initUploadMetricThread(data);

            init(conf);
        } catch (Throwable e) {
            LOG.error("Fail to run nimbus ", e);
        } finally {
            cleanup();
        }

        LOG.info("Quit nimbus");
    }

    public ServiceHandler launcherLocalServer(final Map conf, INimbus inimbus)
            throws Exception {
        LOG.info("Begin to start nimbus on local model");

        StormConfig.validate_local_mode(conf);

        inimbus.prepare(conf, StormConfig.masterInimbus(conf));

        data = createNimbusData(conf, inimbus);

        // @@@ testing
        initUploadMetricThread(data);

        init(conf);

        return serviceHandler;
    }

    private void initContainerHBThread(Map conf) throws IOException {
        AsyncLoopThread thread = SyncContainerHb.mkNimbusInstance(conf);
        if (thread != null) {
            smartThreads.add(thread);
        }
    }

    private void init(Map conf) throws Exception {

        NimbusUtils.cleanupCorruptTopologies(data);

        initTopologyAssign();

        initTopologyStatus();

        initCleaner(conf);

        serviceHandler = new ServiceHandler(data);

        if (!data.isLocalMode()) {

            initMonitor(conf);

            initThrift(conf);

        }
    }

    @SuppressWarnings("rawtypes")
    private NimbusData createNimbusData(Map conf, INimbus inimbus)
            throws Exception {

        // Callback callback=new TimerCallBack();
        // StormTimer timer=Timer.mkTimerTimer(callback);
        NimbusData data = new NimbusData(conf, inimbus);

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
                NimbusUtils.updateTopologyTaskTimeout(data, topologyid);
            }

        }

        LOG.info("Successfully init topology status");
    }

    @SuppressWarnings("rawtypes")
    private void initMonitor(Map conf) {
        final ScheduledExecutorService scheduExec = data.getScheduExec();

        // Schedule Nimbus monitor
        MonitorRunnable r1 = new MonitorRunnable(data);

        int monitor_freq_secs =
                JStormUtils.parseInt(conf.get(Config.NIMBUS_MONITOR_FREQ_SECS),
                        10);
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
        int inbox_jar_expiration_secs =
                JStormUtils
                        .parseInt(conf
                                .get(Config.NIMBUS_INBOX_JAR_EXPIRATION_SECS),
                                3600);
        CleanRunnable r2 =
                new CleanRunnable(dir_location, inbox_jar_expiration_secs);

        int cleanup_inbox_freq_secs =
                JStormUtils.parseInt(
                        conf.get(Config.NIMBUS_CLEANUP_INBOX_FREQ_SECS), 600);

        scheduExec.scheduleAtFixedRate(r2, 0, cleanup_inbox_freq_secs,
                TimeUnit.SECONDS);

        LOG.info("Successfully init " + dir_location + " cleaner");
    }

    @SuppressWarnings("rawtypes")
    private void initThrift(Map conf) throws TTransportException {
        Integer thrift_port =
                JStormUtils.parseInt(conf.get(Config.NIMBUS_THRIFT_PORT));
        TNonblockingServerSocket socket =
                new TNonblockingServerSocket(thrift_port);

        Integer maxReadBufSize =
                JStormUtils.parseInt(conf
                        .get(Config.NIMBUS_THRIFT_MAX_BUFFER_SIZE));

        THsHaServer.Args args = new THsHaServer.Args(socket);
        args.workerThreads(ServiceHandler.THREAD_NUM);
        args.protocolFactory(new TBinaryProtocol.Factory(false, true,
                maxReadBufSize, -1));

        args.processor(new Nimbus.Processor<Iface>(serviceHandler));
        args.maxReadBufferBytes = maxReadBufSize;

        thriftServer = new THsHaServer(args);

        LOG.info("Successfully started nimbus: started Thrift server...");
        thriftServer.serve();
    }

    private void initFollowerThread(Map conf) {
        follower = new FollowerRunnable(data, 5000);
        Thread thread = new Thread(follower);
        thread.setDaemon(true);
        thread.start();
        LOG.info("Successfully init Follower thread");
    }

    private void initShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                NimbusServer.this.cleanup();
            }

        });
    }

    private void initUploadMetricThread(NimbusData data) {
        final TopologyMetricsRunnable metricRunnable = data.getMetricRunnable();
        
        int threadNum = ConfigExtension.getNimbusMetricThreadNum(data.getConf());
        
        for (int i = 0; i < threadNum; i++) {
            AsyncLoopThread thread = new AsyncLoopThread(metricRunnable);
            smartThreads.add(thread);
        }
        
        Runnable pusher = new Runnable() {

            @Override
            public void run() {
                // TODO Auto-generated method stub
                TopologyMetricsRunnable.Upload event =
                        new TopologyMetricsRunnable.Upload();
                event.timeStamp = System.currentTimeMillis();

                metricRunnable.pushEvent(event);
            }

        };

        ScheduledExecutorService scheduleService = data.getScheduExec();
        scheduleService.scheduleAtFixedRate(pusher, 120, 60,
                TimeUnit.SECONDS);
        
        SimpleJStormMetric nimbusMetric = SimpleJStormMetric.mkInstance();
        scheduleService.scheduleAtFixedRate(nimbusMetric, 120, 60,
                        TimeUnit.SECONDS);
        
        //AsyncLoopThread nimbusCacheThread = new AsyncLoopThread(data.getNimbusCache().getCacheRunnable());
        //smartThreads.add(nimbusCacheThread);

        LOG.info("Successfully init metrics uploading thread");
    }

    public void cleanup() {
        if (data.getIsShutdown().getAndSet(true) == true) {
            LOG.info("Notify to quit nimbus");
            return;
        }

        LOG.info("Begin to shutdown nimbus");
        AsyncLoopRunnable.getShutdown().set(true);
        
        data.getScheduExec().shutdownNow();

        for (AsyncLoopThread t : smartThreads) {

            t.cleanup();
            JStormUtils.sleepMs(10);
            t.interrupt();
            // try {
            // t.join();
            // } catch (InterruptedException e) {
            // LOG.error("join thread", e);
            // }
            LOG.info("Successfully cleanup " + t.getThread().getName());
        }

        if (serviceHandler != null) {
            serviceHandler.shutdown();
        }

        if (topologyAssign != null) {
            topologyAssign.cleanup();
            LOG.info("Successfully shutdown TopologyAssign thread");
        }

        if (follower != null) {
            follower.clean();
            LOG.info("Successfully shutdown follower thread");
        }

        if (data != null) {
            data.cleanup();
            LOG.info("Successfully shutdown NimbusData");
        }

        if (thriftServer != null) {
            thriftServer.stop();
            LOG.info("Successfully shutdown thrift server");
        }

        if (hs != null) {
            hs.shutdown();
            LOG.info("Successfully shutdown httpserver");
        }

        LOG.info("Successfully shutdown nimbus");
        // make sure shutdown nimbus
        JStormUtils.halt_process(0, "!!!Shutdown!!!");

    }

}

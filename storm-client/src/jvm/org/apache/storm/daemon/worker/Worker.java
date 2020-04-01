/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.daemon.worker;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.security.auth.Subject;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.cluster.ClusterStateContext;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.cluster.IStateStorage;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.daemon.DaemonCommon;
import org.apache.storm.daemon.Shutdownable;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.executor.Executor;
import org.apache.storm.executor.ExecutorShutdown;
import org.apache.storm.executor.IRunningExecutor;
import org.apache.storm.executor.LocalExecutor;
import org.apache.storm.generated.Credentials;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.LSWorkerHeartbeat;
import org.apache.storm.generated.LogConfig;
import org.apache.storm.generated.SupervisorWorkerHeartbeat;
import org.apache.storm.messaging.IConnection;
import org.apache.storm.messaging.IContext;
import org.apache.storm.metrics2.StormMetricRegistry;
import org.apache.storm.security.auth.ClientAuthUtils;
import org.apache.storm.security.auth.IAutoCredentials;
import org.apache.storm.shade.com.google.common.base.Preconditions;
import org.apache.storm.shade.org.apache.commons.io.FileUtils;
import org.apache.storm.shade.org.apache.commons.lang.ObjectUtils;
import org.apache.storm.shade.uk.org.lidalia.sysoutslf4j.context.SysOutOverSLF4J;
import org.apache.storm.stats.ClientStatsUtil;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.SupervisorClient;
import org.apache.storm.utils.SupervisorIfaceFactory;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Worker implements Shutdownable, DaemonCommon {

    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
    private static final Pattern BLOB_VERSION_EXTRACTION = Pattern.compile(".*\\.([0-9]+)$");
    private final Map<String, Object> conf;
    private final Map<String, Object> topologyConf;
    private final IContext context;
    private final String topologyId;
    private final String assignmentId;
    private final int supervisorPort;
    private final int port;
    private final String workerId;
    private final LogConfigManager logConfigManager;
    private final StormMetricRegistry metricRegistry;

    private WorkerState workerState;
    private AtomicReference<List<IRunningExecutor>> executorsAtom;
    private Thread transferThread;

    private AtomicReference<Credentials> credentialsAtom;
    private Subject subject;
    private Collection<IAutoCredentials> autoCreds;
    private final Supplier<SupervisorIfaceFactory> supervisorIfaceSupplier;

    /**
     * TODO: should worker even take the topologyId as input? this should be deducible from cluster state (by searching through assignments)
     * what about if there's inconsistency in assignments? -> but nimbus should guarantee this consistency.
     *
     * @param conf           - Storm configuration
     * @param context        -
     * @param topologyId     - topology id
     * @param assignmentId   - assignment id
     * @param supervisorPort - parent supervisor thrift server port
     * @param port           - port on which the worker runs
     * @param workerId       - worker id
     */
    public Worker(Map<String, Object> conf, IContext context, String topologyId, String assignmentId,
                  int supervisorPort, int port, String workerId, Supplier<SupervisorIfaceFactory> supervisorIfaceSupplier)
        throws IOException {
        this.conf = conf;
        this.context = context;
        this.topologyId = topologyId;
        this.assignmentId = assignmentId;
        this.supervisorPort = supervisorPort;
        this.port = port;
        this.workerId = workerId;
        this.logConfigManager = new LogConfigManager();
        this.metricRegistry = new StormMetricRegistry();

        this.topologyConf = ConfigUtils.overrideLoginConfigWithSystemProperty(ConfigUtils.readSupervisorStormConf(conf, topologyId));

        if (supervisorIfaceSupplier == null) {
            this.supervisorIfaceSupplier = () -> {
                try {
                    return SupervisorClient.getConfiguredClient(topologyConf, Utils.hostname(), supervisorPort);
                } catch (UnknownHostException e) {
                    throw Utils.wrapInRuntime(e);
                }
            };
        } else {
            this.supervisorIfaceSupplier = supervisorIfaceSupplier;
        }
    }

    public Worker(Map<String, Object> conf, IContext context, String topologyId, String assignmentId,
                  int supervisorPort, int port, String workerId) throws IOException {
        this(conf, context, topologyId, assignmentId, supervisorPort, port, workerId, null);
    }

    public static void main(String[] args) throws Exception {
        Preconditions.checkArgument(args.length == 5, "Illegal number of arguments. Expected: 5, Actual: " + args.length);
        String stormId = args[0];
        String assignmentId = args[1];
        String supervisorPort = args[2];
        String portStr = args[3];
        String workerId = args[4];
        Map<String, Object> conf = ConfigUtils.readStormConfig();
        Utils.setupDefaultUncaughtExceptionHandler();
        StormCommon.validateDistributedMode(conf);
        int supervisorPortInt = Integer.parseInt(supervisorPort);
        Worker worker = new Worker(conf, null, stormId, assignmentId, supervisorPortInt, Integer.parseInt(portStr), workerId);
        worker.start();
        int workerShutdownSleepSecs = ObjectReader.getInt(conf.get(Config.SUPERVISOR_WORKER_SHUTDOWN_SLEEP_SECS));
        LOG.info("Adding shutdown hook with kill in {} secs", workerShutdownSleepSecs);
        Utils.addShutdownHookWithDelayedForceKill(worker::shutdown, workerShutdownSleepSecs);
    }

    public void start() throws Exception {
        LOG.info("Launching worker for {} on {}:{} with id {} and conf {}", topologyId, assignmentId, port, workerId,
                 ConfigUtils.maskPasswords(conf));
        // because in local mode, its not a separate
        // process. supervisor will register it in this case
        // if ConfigUtils.isLocalMode(conf) returns false then it is in distributed mode.
        if (!ConfigUtils.isLocalMode(conf)) {
            // Distributed mode
            SysOutOverSLF4J.sendSystemOutAndErrToSLF4J();
            String pid = Utils.processPid();
            FileUtils.touch(new File(ConfigUtils.workerPidPath(conf, workerId, pid)));
            FileUtils.writeStringToFile(new File(ConfigUtils.workerArtifactsPidPath(conf, topologyId, port)), pid,
                                        Charset.forName("UTF-8"));
        }

        ClusterStateContext csContext = new ClusterStateContext(DaemonType.WORKER, topologyConf);
        IStateStorage stateStorage = ClusterUtils.mkStateStorage(conf, topologyConf, csContext);
        IStormClusterState stormClusterState = ClusterUtils.mkStormClusterState(stateStorage, null, csContext);

        metricRegistry.start(conf, DaemonType.WORKER);

        Credentials initialCredentials = stormClusterState.credentials(topologyId, null);
        Map<String, String> initCreds = new HashMap<>();
        if (initialCredentials != null) {
            initCreds.putAll(initialCredentials.get_creds());
        }
        autoCreds = ClientAuthUtils.getAutoCredentials(topologyConf);
        subject = ClientAuthUtils.populateSubject(null, autoCreds, initCreds);

        Subject.doAs(subject, (PrivilegedExceptionAction<Object>)
            () -> loadWorker(stateStorage, stormClusterState, initCreds, initialCredentials)
        );

    }

    private Object loadWorker(IStateStorage stateStorage, IStormClusterState stormClusterState,
                              Map<String, String> initCreds, Credentials initialCredentials)
        throws Exception {
        workerState = new WorkerState(conf, context, topologyId, assignmentId, supervisorIfaceSupplier, port, workerId,
                                      topologyConf, stateStorage, stormClusterState, autoCreds, metricRegistry);

        // Heartbeat here so that worker process dies if this fails
        // it's important that worker heartbeat to supervisor ASAP so that supervisor knows
        // that worker is running and moves on
        doHeartBeat();

        executorsAtom = new AtomicReference<>(null);

        // launch heartbeat threads immediately so that slow-loading tasks don't cause the worker to timeout
        // to the supervisor
        workerState.heartbeatTimer
            .scheduleRecurring(0, (Integer) conf.get(Config.WORKER_HEARTBEAT_FREQUENCY_SECS), () -> {
                try {
                    doHeartBeat();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

        workerState.executorHeartbeatTimer
            .scheduleRecurring(0, (Integer) conf.get(Config.EXECUTOR_METRICS_FREQUENCY_SECS),
                               Worker.this::doExecutorHeartbeats);

        workerState.refreshConnections();

        workerState.activateWorkerWhenAllConnectionsReady();

        workerState.refreshStormActive(null);

        workerState.runWorkerStartHooks();

        List<Executor> execs = new ArrayList<>();
        for (List<Long> e : workerState.getLocalExecutors()) {
            if (ConfigUtils.isLocalMode(conf)) {
                Executor executor = LocalExecutor.mkExecutor(workerState, e, initCreds);
                execs.add(executor);
                for (int i = 0; i < executor.getTaskIds().size(); ++i) {
                    workerState.localReceiveQueues.put(executor.getTaskIds().get(i), executor.getReceiveQueue());
                }
            } else {
                Executor executor = Executor.mkExecutor(workerState, e, initCreds);
                for (int i = 0; i < executor.getTaskIds().size(); ++i) {
                    workerState.localReceiveQueues.put(executor.getTaskIds().get(i), executor.getReceiveQueue());
                }
                execs.add(executor);
            }
        }

        List<IRunningExecutor> newExecutors = new ArrayList<IRunningExecutor>();
        for (Executor executor : execs) {
            newExecutors.add(executor.execute());
        }
        executorsAtom.set(newExecutors);

        // This thread will send out messages destined for remote tasks (on other workers)
        // If there are no remote outbound tasks, don't start the thread.
        if (workerState.hasRemoteOutboundTasks()) {
            transferThread = workerState.makeTransferThread();
            transferThread.setName("Worker-Transfer");
        }

        credentialsAtom = new AtomicReference<Credentials>(initialCredentials);

        establishLogSettingCallback();

        workerState.stormClusterState.credentials(topologyId, Worker.this::checkCredentialsChanged);

        workerState.refreshCredentialsTimer.scheduleRecurring(0,
                                                              (Integer) conf.get(Config.TASK_CREDENTIALS_POLL_SECS), () -> {
                checkCredentialsChanged();
            });

        workerState.checkForUpdatedBlobsTimer.scheduleRecurring(0,
                (Integer) conf.getOrDefault(Config.WORKER_BLOB_UPDATE_POLL_INTERVAL_SECS, 10),
            () -> {
                try {
                    LOG.debug("Checking if blobs have updated");
                    updateBlobUpdates();
                } catch (IOException e) {
                    // IOException from reading the version files to be ignored
                    LOG.error(e.getStackTrace().toString());
                }
            }
        );

        // The jitter allows the clients to get the data at different times, and avoids thundering herd
        if (!(Boolean) topologyConf.get(Config.TOPOLOGY_DISABLE_LOADAWARE_MESSAGING)) {
            workerState.refreshLoadTimer.scheduleRecurringWithJitter(0, 1, 500, Worker.this::doRefreshLoad);
        }

        workerState.refreshConnectionsTimer.scheduleRecurring(0,
                                                              (Integer) conf.get(Config.TASK_REFRESH_POLL_SECS),
                                                              workerState::refreshConnections);

        workerState.resetLogLevelsTimer.scheduleRecurring(0,
                                                          (Integer) conf.get(Config.WORKER_LOG_LEVEL_RESET_POLL_SECS),
                                                          logConfigManager::resetLogLevels);

        workerState.refreshActiveTimer.scheduleRecurring(0, (Integer) conf.get(Config.TASK_REFRESH_POLL_SECS),
                                                         workerState::refreshStormActive);

        setupFlushTupleTimer(topologyConf, newExecutors);
        setupBackPressureCheckTimer(topologyConf);

        LOG.info("Worker has topology config {}", ConfigUtils.maskPasswords(topologyConf));
        LOG.info("Worker {} for storm {} on {}:{}  has finished loading", workerId, topologyId, assignmentId, port);
        return this;
    }

    private void setupFlushTupleTimer(final Map<String, Object> topologyConf, final List<IRunningExecutor> executors) {
        final Integer producerBatchSize = ObjectReader.getInt(topologyConf.get(Config.TOPOLOGY_PRODUCER_BATCH_SIZE));
        final Integer xferBatchSize = ObjectReader.getInt(topologyConf.get(Config.TOPOLOGY_TRANSFER_BATCH_SIZE));
        final Long flushIntervalMillis = ObjectReader.getLong(topologyConf.get(Config.TOPOLOGY_BATCH_FLUSH_INTERVAL_MILLIS));
        if ((producerBatchSize == 1 && xferBatchSize == 1) || flushIntervalMillis == 0) {
            LOG.info("Flush Tuple generation disabled. producerBatchSize={}, xferBatchSize={}, flushIntervalMillis={}",
                     producerBatchSize, xferBatchSize, flushIntervalMillis);
            return;
        }

        workerState.flushTupleTimer.scheduleRecurringMs(flushIntervalMillis, flushIntervalMillis,
            () -> {
                // send flush tuple to all local executors
                for (int i = 0; i < executors.size(); i++) {
                    IRunningExecutor exec = executors.get(i);
                    if (exec.getExecutorId().get(0) != Constants.SYSTEM_TASK_ID) {
                        exec.publishFlushTuple();
                    }
                }
            }
        );
        LOG.info("Flush tuple will be generated every {} millis", flushIntervalMillis);
    }

    private void setupBackPressureCheckTimer(final Map<String, Object> topologyConf) {
        if (workerState.isSingleWorker()) {
            LOG.info("BackPressure change checking is disabled as there is only one worker");
            return;
        }
        final Long bpCheckIntervalMs = ObjectReader.getLong(topologyConf.get(Config.TOPOLOGY_BACKPRESSURE_CHECK_MILLIS));
        workerState.backPressureCheckTimer.scheduleRecurringMs(bpCheckIntervalMs,
                                                               bpCheckIntervalMs, () -> workerState.refreshBackPressureStatus());
        LOG.info("BackPressure status change checking will be performed every {} millis", bpCheckIntervalMs);
    }

    public void doRefreshLoad() {
        workerState.refreshLoad(executorsAtom.get());

        final List<IRunningExecutor> executors = executorsAtom.get();
        for (IRunningExecutor executor : executors) {
            executor.loadChanged(workerState.getLoadMapping());
        }
    }

    public void doHeartBeat() throws IOException {
        LocalState state = ConfigUtils.workerState(workerState.conf, workerState.workerId);
        LSWorkerHeartbeat lsWorkerHeartbeat = new LSWorkerHeartbeat(Time.currentTimeSecs(), workerState.topologyId,
                                                                    workerState.localExecutors.stream()
                                                                                              .map(executor -> new ExecutorInfo(
                                                                                                  executor.get(0).intValue(),
                                                                                                  executor.get(1).intValue()))
                                                                                              .collect(Collectors.toList()),
                                                                    workerState.port);
        state.setWorkerHeartBeat(lsWorkerHeartbeat);
        state.cleanup(60); // this is just in case supervisor is down so that disk doesn't fill up.
        // it shouldn't take supervisor 120 seconds between listing dir and reading it
        heartbeatToMasterIfLocalbeatFail(lsWorkerHeartbeat);
    }

    public void doExecutorHeartbeats() {
        Map<List<Integer>, ExecutorStats> stats;
        List<IRunningExecutor> executors = this.executorsAtom.get();
        if (null == executors) {
            stats = ClientStatsUtil.mkEmptyExecutorZkHbs(workerState.localExecutors);
        } else {
            stats = ClientStatsUtil.convertExecutorZkHbs(executors.stream().collect(Collectors
                                                                                  .toMap(IRunningExecutor::getExecutorId,
                                                                                         IRunningExecutor::renderStats)));
        }
        Map<String, Object> zkHb = ClientStatsUtil.mkZkWorkerHb(workerState.topologyId, stats, workerState.uptime.upTime());
        try {
            workerState.stormClusterState
                .workerHeartbeat(workerState.topologyId, workerState.assignmentId, (long) workerState.port,
                                 ClientStatsUtil.thriftifyZkWorkerHb(zkHb));
        } catch (Exception ex) {
            LOG.error("Worker failed to write heartbeats to ZK or Pacemaker...will retry", ex);
        }
    }

    public Map<String, Long> getCurrentBlobVersions() throws IOException {
        Map<String, Long> results = new HashMap<>();
        Map<String, Map<String, Object>> blobstoreMap =
            (Map<String, Map<String, Object>>) workerState.getTopologyConf().get(Config.TOPOLOGY_BLOBSTORE_MAP);
        if (blobstoreMap != null) {
            String stormRoot = ConfigUtils.supervisorStormDistRoot(workerState.getTopologyConf(), workerState.getTopologyId());
            for (Map.Entry<String, Map<String, Object>> entry : blobstoreMap.entrySet()) {
                String localFileName = entry.getKey();
                Map<String, Object> blobInfo = entry.getValue();
                if (blobInfo != null && blobInfo.containsKey("localname")) {
                    localFileName = (String) blobInfo.get("localname");
                }

                String blobWithVersion = new File(stormRoot, localFileName).getCanonicalFile().getName();
                Matcher m = BLOB_VERSION_EXTRACTION.matcher(blobWithVersion);
                if (m.matches()) {
                    results.put(localFileName, Long.valueOf(m.group(1)));
                }
            }
        }
        return results;
    }

    public void updateBlobUpdates() throws IOException {
        Map<String, Long> latestBlobVersions = getCurrentBlobVersions();
        workerState.blobToLastKnownVersion.putAll(latestBlobVersions);
        LOG.debug("Latest versions for blobs {}", latestBlobVersions);
    }

    public void checkCredentialsChanged() {
        Credentials newCreds = workerState.stormClusterState.credentials(topologyId, null);
        if (!ObjectUtils.equals(newCreds, credentialsAtom.get())) {
            // This does not have to be atomic, worst case we update when one is not needed
            ClientAuthUtils.updateSubject(subject, autoCreds, (null == newCreds) ? null : newCreds.get_creds());
            for (IRunningExecutor executor : executorsAtom.get()) {
                executor.credentialsChanged(newCreds);
            }
            credentialsAtom.set(newCreds);
        }
    }

    public void checkLogConfigChanged() {
        LogConfig logConfig = workerState.stormClusterState.topologyLogConfig(topologyId, null);
        logConfigManager.processLogConfigChange(logConfig);
        establishLogSettingCallback();
    }

    public void establishLogSettingCallback() {
        workerState.stormClusterState.topologyLogConfig(topologyId, this::checkLogConfigChanged);
    }

    /**
     * Send a heartbeat to local supervisor first to check if supervisor is ok for heartbeating.
     */
    private void heartbeatToMasterIfLocalbeatFail(LSWorkerHeartbeat lsWorkerHeartbeat) {
        if (ConfigUtils.isLocalMode(this.conf)) {
            return;
        }
        //In distributed mode, send heartbeat directly to master if local supervisor goes down.
        SupervisorWorkerHeartbeat workerHeartbeat = new SupervisorWorkerHeartbeat(lsWorkerHeartbeat.get_topology_id(),
                                                                                  lsWorkerHeartbeat.get_executors(),
                                                                                  lsWorkerHeartbeat.get_time_secs());
        try (SupervisorIfaceFactory fac = supervisorIfaceSupplier.get()) {
            fac.getIface().sendSupervisorWorkerHeartbeat(workerHeartbeat);
        } catch (Exception tr1) {
            //If any error/exception thrown, report directly to nimbus.
            LOG.warn("Exception when send heartbeat to local supervisor", tr1.getMessage());
            try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(topologyConf)) {
                nimbusClient.getClient().sendSupervisorWorkerHeartbeat(workerHeartbeat);
            } catch (Exception tr2) {
                //if any error/exception thrown, just ignore.
                LOG.error("Exception when send heartbeat to master", tr2.getMessage());
            }
        }
    }

    @Override
    public void shutdown() {
        try {
            LOG.info("Shutting down worker {} {} {}", topologyId, assignmentId, port);

            for (IConnection socket : workerState.cachedNodeToPortSocket.get().values()) {
                //this will do best effort flushing since the linger period
                // was set on creation
                socket.close();
            }
            LOG.info("Terminating messaging context");
            LOG.info("Shutting down executors");
            for (IRunningExecutor executor : executorsAtom.get()) {
                ((ExecutorShutdown) executor).shutdown();
            }
            LOG.info("Shut down executors");

            LOG.info("Shutting down transfer thread");
            workerState.haltWorkerTransfer();

            if (transferThread != null) {
                transferThread.interrupt();
                transferThread.join();
                LOG.info("Shut down transfer thread");
            }

            workerState.heartbeatTimer.close();
            workerState.refreshConnectionsTimer.close();
            workerState.refreshCredentialsTimer.close();
            workerState.checkForUpdatedBlobsTimer.close();
            workerState.refreshActiveTimer.close();
            workerState.executorHeartbeatTimer.close();
            workerState.userTimer.close();
            workerState.refreshLoadTimer.close();
            workerState.resetLogLevelsTimer.close();
            workerState.flushTupleTimer.close();
            workerState.backPressureCheckTimer.close();
            
            // this is fine because the only time this is shared is when it's a local context,
            // in which case it's a noop
            workerState.mqContext.term();
            
            workerState.closeResources();

            metricRegistry.stop();

            LOG.info("Trigger any worker shutdown hooks");
            workerState.runWorkerShutdownHooks();

            workerState.stormClusterState.removeWorkerHeartbeat(topologyId, assignmentId, (long) port);
            LOG.info("Disconnecting from storm cluster state context");
            workerState.stormClusterState.disconnect();
            workerState.stateStorage.close();
            LOG.info("Shut down worker {} {} {}", topologyId, assignmentId, port);
        } catch (Exception ex) {
            throw Utils.wrapInRuntime(ex);
        }

    }

    @Override
    public boolean isWaiting() {
        return workerState.heartbeatTimer.isTimerWaiting()
               && workerState.refreshConnectionsTimer.isTimerWaiting()
               && workerState.refreshLoadTimer.isTimerWaiting()
               && workerState.refreshCredentialsTimer.isTimerWaiting()
               && workerState.checkForUpdatedBlobsTimer.isTimerWaiting()
               && workerState.refreshActiveTimer.isTimerWaiting()
               && workerState.executorHeartbeatTimer.isTimerWaiting()
               && workerState.userTimer.isTimerWaiting()
               && workerState.flushTupleTimer.isTimerWaiting();
    }
}

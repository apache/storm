/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.daemon.worker;

import com.lmax.disruptor.EventHandler;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.storm.Config;
import org.apache.storm.cluster.*;
import org.apache.storm.daemon.DaemonCommon;
import org.apache.storm.daemon.Shutdownable;
import org.apache.storm.executor.Executor;
import org.apache.storm.executor.ExecutorShutdown;
import org.apache.storm.executor.IRunningExecutor;
import org.apache.storm.executor.LocalExecutor;
import org.apache.storm.generated.*;
import org.apache.storm.messaging.IConnection;
import org.apache.storm.messaging.IContext;
import org.apache.storm.messaging.TaskMessage;
import org.apache.storm.security.auth.AuthUtils;
import org.apache.storm.security.auth.IAutoCredentials;
import org.apache.storm.stats.StatsUtil;
import org.apache.storm.utils.*;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.org.lidalia.sysoutslf4j.context.SysOutOverSLF4J;

import javax.security.auth.Subject;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Worker implements Shutdownable, DaemonCommon {

    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
    private final Map conf;
    private final IContext context;
    private final String topologyId;
    private final String assignmentId;
    private final int port;
    private final String workerId;
    private final LogConfigManager logConfigManager;


    private WorkerState workerState;
    private AtomicReference<List<IRunningExecutor>> executors;
    private Thread transferThread;
    private WorkerBackpressureThread backpressureThread;

    private AtomicReference<Credentials> credentials;
    private Subject subject;
    private Collection<IAutoCredentials> autoCreds;


    /**
     * TODO: should worker even take the topologyId as input? this should be
     * deducable from cluster state (by searching through assignments)
     * what about if there's inconsistency in assignments? -> but nimbus should guarantee this consistency
     *
     * @param conf         - Storm configuration
     * @param context      -
     * @param topologyId   - topology id
     * @param assignmentId - assignement id
     * @param port         - port on which the worker runs
     * @param workerId     - worker id
     */

    public Worker(Map conf, IContext context, String topologyId, String assignmentId, int port, String workerId) {
        this.conf = conf;
        this.context = context;
        this.topologyId = topologyId;
        this.assignmentId = assignmentId;
        this.port = port;
        this.workerId = workerId;
        this.logConfigManager = new LogConfigManager();
    }

    public void start() throws Exception {
        LOG.info("Launching worker for {} on {}:{} with id {} and conf {}", topologyId, assignmentId, port, workerId,
            conf);
        // because in local mode, its not a separate
        // process. supervisor will register it in this case
        // if ConfigUtils.isLocalMode(conf) returns false then it is in distributed mode.
        if (!ConfigUtils.isLocalMode(conf)) {
            // Distributed mode
            SysOutOverSLF4J.sendSystemOutAndErrToSLF4J();
            String pid = Utils.processPid();
            FileUtils.touch(new File(ConfigUtils.workerPidPath(conf, workerId, pid)));
            FileUtils.writeStringToFile(new File(ConfigUtils.workerArtifactsPidPath(conf, topologyId, port)), pid,
                Charset.defaultCharset());
        }
        final Map topologyConf =
            ConfigUtils.overrideLoginConfigWithSystemProperty(ConfigUtils.readSupervisorStormConf(conf, topologyId));
        List<ACL> acls = Utils.getWorkerACL(topologyConf);
        IStateStorage stateStorage =
            ClusterUtils.mkStateStorage(conf, topologyConf, acls, new ClusterStateContext(DaemonType.WORKER));
        IStormClusterState stormClusterState =
            ClusterUtils.mkStormClusterState(stateStorage, acls, new ClusterStateContext());
        Credentials initialCredentials = stormClusterState.credentials(topologyId, null);
        autoCreds = AuthUtils.GetAutoCredentials(topologyConf);
        subject = AuthUtils.populateSubject(null, autoCreds, initialCredentials.get_creds());

        Subject.doAs(subject, new PrivilegedExceptionAction<Object>() {
            @Override public Object run() throws Exception {
                workerState =
                    new WorkerState(conf, context, topologyId, assignmentId, port, workerId, topologyConf, stateStorage,
                        stormClusterState);

                // Heartbeat here so that worker process dies if this fails
                // it's important that worker heartbeat to supervisor ASAP so that supervisor knows
                // that worker is running and moves on
                doHeartBeat(workerState);

                executors = new AtomicReference<>(null);

                // launch heartbeat threads immediately so that slow-loading tasks don't cause the worker to timeout
                // to the supervisor
                workerState.heartbeatTimer
                    .scheduleRecurring(0, (Integer) conf.get(Config.WORKER_HEARTBEAT_FREQUENCY_SECS), () -> {
                        try {
                            doHeartBeat(workerState);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });

                workerState.executorHeartbeatTimer
                    .scheduleRecurring(0, (Integer) conf.get(Config.WORKER_HEARTBEAT_FREQUENCY_SECS),
                        () -> doExecutorHeartbeats(workerState, executors.get()));

                workerState.registerCallbacks();

                workerState.refreshConnections(null);

                workerState.activateWorkerWhenAllConnectionsReady();

                workerState.refreshStormActive(null);

                workerState.runWorkerStartHooks();

                List<IRunningExecutor> newExecutors = new ArrayList<IRunningExecutor>();
                for (IRunningExecutor e : executors.get()) {
                    if (ConfigUtils.isLocalMode(topologyConf)) {
                        newExecutors.add(
                            LocalExecutor.mkExecutor(workerState, e.getExecutorId(), initialCredentials.get_creds())
                                .execute());
                    } else {
                        newExecutors.add(
                            Executor.mkExecutor(workerState, e.getExecutorId(), initialCredentials.get_creds())
                                .execute());
                    }
                }
                executors.set(newExecutors);

                EventHandler<Object> handler = new EventHandler<Object>() {
                    @Override public void onEvent(Object o, long l, boolean b) throws Exception {

                    }
                };

                EventHandler<Object> tupleHandler = (packets, seqId, batchEnd) -> workerState
                    .sendTuplesToRemoteWorker((HashMap<Integer, ArrayList<TaskMessage>>) packets, seqId, batchEnd);

                // This thread will publish the messages destined for remote tasks to remote connections
                transferThread = Utils.asyncLoop(() -> {
                    workerState.transferQueue.consumeBatchWhenAvailable(tupleHandler);
                    return 0;
                });

                DisruptorBackpressureCallback disruptorBackpressureHandler =
                    mkDisruptorBackpressureHandler(workerState);
                workerState.transferQueue.registerBackpressureCallback(disruptorBackpressureHandler);
                workerState.transferQueue
                    .setEnableBackpressure((Boolean) topologyConf.get(Config.TOPOLOGY_BACKPRESSURE_ENABLE));
                workerState.transferQueue
                    .setHighWaterMark((Double) topologyConf.get(Config.BACKPRESSURE_DISRUPTOR_HIGH_WATERMARK));
                workerState.transferQueue
                    .setLowWaterMark((Double) topologyConf.get(Config.BACKPRESSURE_DISRUPTOR_LOW_WATERMARK));

                WorkerBackpressureCallback backpressureCallback = mkBackpressureHandler(workerState, executors.get());
                backpressureThread = new WorkerBackpressureThread(workerState.backpressureTrigger, workerState, backpressureCallback);
                if ((Boolean) topologyConf.get(Config.TOPOLOGY_BACKPRESSURE_ENABLE)) {
                    backpressureThread.start();
                    stormClusterState.topologyBackpressure(topologyId, workerState::refreshThrottle);
                }

                credentials = new AtomicReference<Credentials>(initialCredentials);

                establishLogSettingCallback();

                workerState.stormClusterState.credentials(topologyId, Worker.this::checkCredentialsChanged);

                workerState.refreshCredentialsTimer.scheduleRecurring(0,
                    (Integer) conf.get(Config.TASK_CREDENTIALS_POLL_SECS), new Runnable() {
                        @Override public void run() {
                            checkCredentialsChanged();
                            if ((Boolean) topologyConf.get(Config.TOPOLOGY_BACKPRESSURE_ENABLE)) {
                               checkThrottleChanged();
                            }
                        }
                    });

                // The jitter allows the clients to get the data at different times, and avoids thundering herd
                if (!(Boolean) topologyConf.get(Config.TOPOLOGY_DISABLE_LOADAWARE_MESSAGING)) {
                    workerState.refreshLoadTimer.scheduleRecurringWithJitter(0, 1, 500, workerState::refreshLoad);
                }

                workerState.refreshConnectionsTimer.scheduleRecurring(0,
                    (Integer) conf.get(Config.TASK_REFRESH_POLL_SECS), workerState::refreshConnections);

                workerState.resetLogTevelsTimer.scheduleRecurring(0,
                    (Integer) conf.get(Config.WORKER_LOG_LEVEL_RESET_POLL_SECS), logConfigManager::resetLogLevels);

                workerState.refreshActiveTimer.scheduleRecurring(0, (Integer) conf.get(Config.TASK_REFRESH_POLL_SECS),
                    workerState::refreshStormActive);

                LOG.info("Worker has topology config {}", Utils.redactValue(topologyConf, Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD));
                LOG.info("Worker {} for storm {} on {}:{}  has finished loading", workerId, topologyId, assignmentId, port);
                return this;
            };
        });

    }

    public void doHeartBeat(WorkerState workerState) throws IOException {
        LocalState state = ConfigUtils.workerState(workerState.conf, workerState.workerId);
        state.setWorkerHeartBeat(new LSWorkerHeartbeat(Time.currentTimeSecs(), workerState.topologyId,
            workerState.executors.stream()
                .map(executor -> new ExecutorInfo(executor.get(0).intValue(), executor.get(1).intValue()))
                .collect(Collectors.toList()), workerState.port));
        state.cleanup(60); // this is just in case supervisor is down so that disk doesn't fill up.
        // it shouldn't take supervisor 120 seconds between listing dir and reading it
    }

    public void doExecutorHeartbeats(WorkerState workerState, List<IRunningExecutor> executors) {
        Map<List<Integer>, ExecutorStats> stats;
        if (null == executors) {
            stats = StatsUtil.mkEmptyExecutorZkHbs(workerState.executors);
        } else {
            stats = StatsUtil.convertExecutorZkHbs(executors.stream().collect(Collectors
                .toMap((Function<IRunningExecutor, List<Long>>) executor -> executor.getExecutorId(),
                    (Function<IRunningExecutor, ExecutorStats>) executor -> executor.renderStats())));
        }
        Map<String, Object> zkHB = StatsUtil.mkZkWorkerHb(workerState.topologyId, stats, workerState.uptime.upTime());
        try {
            workerState.stormClusterState
                .workerHeartbeat(workerState.topologyId, workerState.assignmentId, (long) workerState.port,
                    StatsUtil.thriftifyZkWorkerHb(zkHB));
        } catch (Exception ex) {
            LOG.error("Worker failed to write heartbeats to ZK or Pacemaker...will retry", ex);
        }
    }

    public void checkCredentialsChanged() {
        Credentials newCreds = workerState.stormClusterState.credentials(topologyId, null);
        if (! ObjectUtils.equals(newCreds, credentials)) {
            // This does not have to be atomic, worst case we update when one is not needed
            AuthUtils.updateSubject(subject, autoCreds, newCreds.get_creds());
            for (IRunningExecutor executor : executors.get()) {
                executor.credenetialsChanged(newCreds);
            }
            credentials.set(newCreds);
        }
    }

    public void checkThrottleChanged() {
        boolean throttleOn = workerState.stormClusterState.topologyBackpressure(topologyId, this::checkThrottleChanged);
        workerState.throttleOn.set(throttleOn);
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
     * make a handler for the worker's send disruptor queue to
     * check highWaterMark and lowWaterMark for backpressure
     */
    private DisruptorBackpressureCallback mkDisruptorBackpressureHandler(WorkerState workerState) {
        return new DisruptorBackpressureCallback() {
            @Override public void highWaterMark() throws Exception {
                workerState.transferBackpressure.set(true);
                WorkerBackpressureThread.notifyBackpressureChecker(workerState.backpressureTrigger);
            }

            @Override public void lowWaterMark() throws Exception {
                workerState.transferBackpressure.set(false);
                WorkerBackpressureThread.notifyBackpressureChecker(workerState.backpressureTrigger);
            }
        };
    }

    /**
     * make a handler that checks and updates worker's backpressure flag
     */
    private WorkerBackpressureCallback mkBackpressureHandler(WorkerState workerState,
        final List<IRunningExecutor> executors) {
        return new WorkerBackpressureCallback() {
            @Override public void onEvent(Object obj) {
                String topologyId = workerState.topologyId;
                String assignmentId = workerState.assignmentId;
                int port = workerState.port;
                IStormClusterState stormClusterState = workerState.stormClusterState;
                boolean prevBackpressureFlag = workerState.backpressure.get();
                boolean currBackpressureFlag = prevBackpressureFlag;
                if (null != executors) {
                    currBackpressureFlag = workerState.transferQueue.getThrottleOn() || (executors.stream()
                        .map(IRunningExecutor::getBackPressureFlag).reduce((op1, op2) -> (op1 || op2)).get());
                }

                if (currBackpressureFlag != prevBackpressureFlag) {
                    try {
                        LOG.debug("worker backpressure flag changing from {} to {}", prevBackpressureFlag, currBackpressureFlag);
                        stormClusterState.workerBackpressure(topologyId, assignmentId, (long) port, currBackpressureFlag);
                    } catch (Exception ex) {
                        LOG.error("workerBackpressure update failed when connecting to ZK ... will retry", ex);
                    }
                }
            }
        };
    }

    private void setupTimers() {

    }

    @Override public void shutdown() {
        try {
            LOG.info("Shutting down worker {} {} {}", topologyId, assignmentId, port);

            for (IConnection socket : workerState.cachedNodeToPortSocket.get().values()) {
                //this will do best effort flushing since the linger period
                // was set on creation
                socket.close();
            }
            LOG.info("Terminating messaging context");
            LOG.info("Shutting down executors");
            for (IRunningExecutor executor : executors.get()) {
                ((ExecutorShutdown) executor).shutdown();
            }
            LOG.info("Shut down executors");

            // this is fine because the only time this is shared is when it's a local context,
            // in which case it's a noop
            workerState.mqContext.term();
            LOG.info("Shutting down transfer thread");
            workerState.transferQueue.haltWithInterrupt();

            transferThread.interrupt();
            transferThread.join();
            LOG.info("Shut down transfer thread");

            backpressureThread.terminate();
            LOG.info("Shut down backpressure thread");

            workerState.heartbeatTimer.close();
            workerState.refreshConnectionsTimer.close();
            workerState.refreshCredentialsTimer.close();
            workerState.refreshActiveTimer.close();
            workerState.executorHeartbeatTimer.close();
            workerState.userTimer.close();
            workerState.refreshLoadTimer.close();
            workerState.resetLogTevelsTimer.close();
            workerState.closeResources();

            LOG.info("Trigger any worker shutdown hooks");
            workerState.runWorkerShutdownHooks();

            workerState.stormClusterState.removeWorkerHeartbeat(topologyId, assignmentId, (long) port);
            workerState.stormClusterState.removeWorkerBackpressure(topologyId, assignmentId, (long) port);
            LOG.info("Disconnecting from storm cluster state context");
            workerState.stormClusterState.disconnect();
            workerState.stateStorage.close();
            LOG.info("Shut down worker {} {} {}", topologyId, assignmentId, port);
        } catch (Exception ex) {
            throw Utils.wrapInRuntime(ex);
        }

    }

    @Override public boolean isWaiting() {
        return workerState.heartbeatTimer.isTimerWaiting()
            && workerState.refreshConnectionsTimer.isTimerWaiting()
            && workerState.refreshLoadTimer.isTimerWaiting()
            && workerState.refreshCredentialsTimer.isTimerWaiting()
            && workerState.refreshActiveTimer.isTimerWaiting()
            && workerState.executorHeartbeatTimer.isTimerWaiting()
            && workerState.userTimer.isTimerWaiting();
    }
}

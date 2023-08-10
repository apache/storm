/*
 * Copyright 2018 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.daemon.worker;

import org.apache.storm.Config;
import org.apache.storm.cluster.IStateStorage;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.Assignment;
import org.apache.storm.generated.Credentials;
import org.apache.storm.generated.Supervisor;
import org.apache.storm.hooks.BaseWorkerHook;
import org.apache.storm.messaging.IContext;
import org.apache.storm.metrics2.StormMetricRegistry;
import org.apache.storm.security.auth.IAutoCredentials;
import org.apache.storm.task.WorkerUserContext;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.SupervisorIfaceFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestUtilsForWorkerState {

    public static final String RESOURCE_KEY = "resource-key";
    public static final String RESOURCE_VALUE = "resource-value";

    public static WorkerState getWorkerState(Map<String, Object> conf, String topologyId) throws IOException, TException {
        IContext context = mock(IContext.class);

        String assignmentId = null;
        Assignment assignment = mock(Assignment.class);
        when(assignment.get_executor_node_port()).thenReturn(new HashMap<>());
        Supervisor.Iface supervisorIface = mock(Supervisor.Iface.class);
        when(supervisorIface.getLocalAssignmentForStorm(anyString())).thenReturn(assignment);
        SupervisorIfaceFactory supervisorIfaceFactory = mock(SupervisorIfaceFactory.class);
        when(supervisorIfaceFactory.getIface()).thenReturn(supervisorIface);
        Supplier<SupervisorIfaceFactory> supervisorIfaceSupplier = () -> supervisorIfaceFactory;

        int port = 0;
        String workerId = null;

        Map<String, Object> topologyConf = new HashMap<>();
        topologyConf.put(Config.STORM_MESSAGING_TRANSPORT, "org.apache.storm.messaging.netty.Context");
        topologyConf.put(Config.STORM_MESSAGING_NETTY_CLIENT_WORKER_THREADS, 1);
        topologyConf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 32768);
        topologyConf.put(Config.TOPOLOGY_PRODUCER_BATCH_SIZE, 1);
        topologyConf.put(Config.TOPOLOGY_EXECUTOR_OVERFLOW_LIMIT, 0);
        topologyConf.put(Config.TOPOLOGY_BACKPRESSURE_WAIT_STRATEGY, "org.apache.storm.policy.WaitStrategyProgressive");
        topologyConf.put(Config.TOPOLOGY_BACKPRESSURE_WAIT_PROGRESSIVE_LEVEL1_COUNT, 1);
        topologyConf.put(Config.TOPOLOGY_BACKPRESSURE_WAIT_PROGRESSIVE_LEVEL2_COUNT, 1000);
        topologyConf.put(Config.TOPOLOGY_BACKPRESSURE_WAIT_PROGRESSIVE_LEVEL3_SLEEP_MILLIS, 1);
        topologyConf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 1);
        topologyConf.put(Config.TOPOLOGY_WORKERS, 1);
        topologyConf.put(Config.TOPOLOGY_TASKS, 1);
        topologyConf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 15);
        topologyConf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 30);
        topologyConf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 1000);
        topologyConf.put(Config.TOPOLOGY_TRANSFER_BATCH_SIZE, 1);

        IStateStorage stateStorage = null;
        IStormClusterState stormClusterState = null;
        Collection<IAutoCredentials> autoCreds = null;
        StormMetricRegistry metricRegistry = mock(StormMetricRegistry.class);
        Credentials initialCredentials = null;

        WorkerState workerState = new WorkerState(conf, context, topologyId, assignmentId, supervisorIfaceSupplier, port, workerId,
                topologyConf, stateStorage, stormClusterState, autoCreds, metricRegistry, initialCredentials);

        return workerState;
    }

    public static class StateTrackingWorkerHook extends BaseWorkerHook {
        private boolean isStartCalled;
        private boolean isShutdownCalled;

        public boolean isStartCalled() {
            return isStartCalled;
        }

        public boolean isShutdownCalled() {
            return isShutdownCalled;
        }

        @Override
        public void shutdown() {
            isShutdownCalled = true;
        }

        @Override
        public void start(Map<String, Object> topoConf, WorkerUserContext context) {
            isStartCalled = true;
        }
    }

    public static class ResourceInitializingWorkerHook extends BaseWorkerHook {
        private transient WorkerUserContext context;

        @Override
        public void start(Map<String, Object> topoConf, WorkerUserContext context) {
            context.setResource(RESOURCE_KEY, RESOURCE_VALUE);
        }
    }
}

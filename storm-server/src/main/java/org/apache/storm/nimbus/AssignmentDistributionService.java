/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.nimbus;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.storm.Constants;
import org.apache.storm.DaemonConfig;
import org.apache.storm.daemon.supervisor.Supervisor;
import org.apache.storm.generated.SupervisorAssignments;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.scheduler.INodeAssignmentSentCallBack;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.SupervisorClient;
import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A service for distributing master assignments to supervisors, this service makes the assignments notification
 * asynchronous.
 *
 * <p>We support multiple working threads to distribute assignment, every thread has a queue buffer.
 *
 * <p>Master will shuffle its node request to the queues, if the target queue is full, we just discard the request,
 * let the supervisors sync instead.
 *
 * <p>Caution: this class is not thread safe.
 *
 * <pre>{@code
 * Working mode
 *                      +--------+         +-----------------+
 *                      | queue1 |   ==>   | Working thread1 |
 * +--------+ shuffle   +--------+         +-----------------+
 * | Master |   ==>
 * +--------+           +--------+         +-----------------+
 *                      | queue2 |   ==>   | Working thread2 |
 *                      +--------+         +-----------------+
 * }
 * </pre>
 */
public class AssignmentDistributionService implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(AssignmentDistributionService.class);
    private ExecutorService service;
    /**
     * Flag to indicate if the service is active.
     */
    private volatile boolean active = false;

    private Random random;
    /**
     * Working threads num.
     */
    private int threadsNum = 0;
    /**
     * Working thread queue size.
     */
    private int queueSize = 0;

    /**
     * Assignments request queue.
     */
    private volatile Map<Integer, LinkedBlockingQueue<NodeAssignments>> assignmentsQueue;

    /**
     * local supervisors for local cluster assignments distribution.
     */
    private Map<String, Supervisor> localSupervisors;

    private Map conf;

    private boolean isLocalMode = false; // boolean cache for local mode decision
    private INodeAssignmentSentCallBack sendAssignmentCallback;

    /**
     * Factory method for initialize a instance.
     * @param conf config.
     * @param callback callback for sendAssignment results
     * @return an instance of {@link AssignmentDistributionService}
     */
    public static AssignmentDistributionService getInstance(Map conf, INodeAssignmentSentCallBack callback) {
        AssignmentDistributionService service = new AssignmentDistributionService();
        service.prepare(conf, callback);
        return service;
    }

    /**
     * Function for initialization.
     *
     * @param conf config
     * @param callback callback for sendAssignment results
     */
    public void prepare(Map conf, INodeAssignmentSentCallBack callBack) {
        this.conf = conf;
        this.sendAssignmentCallback = callBack;
        this.random = new Random(47);

        this.threadsNum = ObjectReader.getInt(conf.get(DaemonConfig.NIMBUS_ASSIGNMENTS_SERVICE_THREADS), 10);
        this.queueSize = ObjectReader.getInt(conf.get(DaemonConfig.NIMBUS_ASSIGNMENTS_SERVICE_THREAD_QUEUE_SIZE), 100);

        this.assignmentsQueue = new HashMap<>();
        for (int i = 0; i < threadsNum; i++) {
            this.assignmentsQueue.put(i, new LinkedBlockingQueue<NodeAssignments>(queueSize));
        }
        //start the thread pool
        this.service = Executors.newFixedThreadPool(threadsNum);
        this.active = true;
        //start the threads
        for (int i = 0; i < threadsNum; i++) {
            this.service.submit(new DistributeTask(this, i));
        }
        // for local cluster
        localSupervisors = new HashMap<>();
        if (ConfigUtils.isLocalMode(conf)) {
            isLocalMode = true;
        }
    }

    @Override
    public void close() throws IOException {
        this.active = false;
        this.service.shutdownNow();
        try {
            this.service.awaitTermination(1L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("Failed to close assignments distribute service");
        }
        this.assignmentsQueue = null;
    }

    /**
     * Add an assignments for a node/supervisor for distribution.
     * @param node node id of supervisor.
     * @param host host name for the node.
     * @param serverPort node thrift server port.
     * @param assignments the {@link org.apache.storm.generated.SupervisorAssignments}
     */
    public void addAssignmentsForNode(String node, String host, Integer serverPort, SupervisorAssignments assignments,
                                      StormMetricsRegistry metricsRegistry) {
        try {
            //For some reasons, we can not get supervisor port info, eg: supervisor shutdown,
            //Just skip for this scheduling round.
            if (serverPort == null) {
                LOG.warn("Discard an assignment distribution for node {} because server port info is missing.", node);
                return;
            }

            boolean success = nextQueue().offer(NodeAssignments.getInstance(node, host, serverPort,
                                                assignments, metricsRegistry), 5L, TimeUnit.SECONDS);
            if (!success) {
                LOG.warn("Discard an assignment distribution for node {} because the target sub queue is full.", node);
            }

        } catch (InterruptedException e) {
            LOG.error("Add node assignments interrupted: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void addLocalSupervisor(Supervisor supervisor) {
        this.localSupervisors.put(supervisor.getId(), supervisor);
    }

    private Integer nextQueueId() {
        return this.random.nextInt(threadsNum);
    }

    private LinkedBlockingQueue<NodeAssignments> nextQueue() {
        return this.assignmentsQueue.get(nextQueueId());
    }

    private LinkedBlockingQueue<NodeAssignments> getQueueById(Integer queueIndex) {
        return this.assignmentsQueue.get(queueIndex);
    }

    /**
     * Get an assignments from the target queue with the specific index.
     * @param queueIndex index of the queue
     * @return an {@link NodeAssignments}
     */
    public NodeAssignments nextAssignments(Integer queueIndex) throws InterruptedException {
        NodeAssignments target = null;
        while (true) {
            target = getQueueById(queueIndex).poll();
            if (target != null) {
                return target;
            }
            Time.sleep(100L);
        }
    }

    public boolean isActive() {
        return this.active;
    }

    public Map getConf() {
        return this.conf;
    }

    static class NodeAssignments {
        private String node;
        private String host;
        private Integer serverPort;
        private SupervisorAssignments assignments;
        private StormMetricsRegistry metricsRegistry;

        private NodeAssignments(String node, String host, Integer serverPort, SupervisorAssignments assignments,
                                StormMetricsRegistry metricsRegistry) {
            this.node = node;
            this.host = host;
            this.serverPort = serverPort;
            this.assignments = assignments;
            this.metricsRegistry = metricsRegistry;
        }

        public static NodeAssignments getInstance(String node, String host, Integer serverPort,
                                                  SupervisorAssignments assignments, StormMetricsRegistry metricsRegistry) {
            return new NodeAssignments(node, host, serverPort, assignments, metricsRegistry);
        }

        //supervisor assignment id/supervisor id
        public String getNode() {
            return this.node;
        }

        public String getHost() {
            return host;
        }

        public Integer getServerPort() {
            return serverPort;
        }

        public SupervisorAssignments getAssignments() {
            return this.assignments;
        }

        public StormMetricsRegistry getMetricsRegistry() {
            return metricsRegistry;
        }
    }

    /**
     * Task to distribute assignments.
     */
    static class DistributeTask implements Runnable {
        private AssignmentDistributionService service;
        private Integer queueIndex;

        DistributeTask(AssignmentDistributionService service, Integer index) {
            this.service = service;
            this.queueIndex = index;
        }

        @Override
        public void run() {
            while (service.isActive()) {
                try {
                    NodeAssignments nodeAssignments = this.service.nextAssignments(queueIndex);
                    sendAssignmentsToNode(nodeAssignments);
                } catch (InterruptedException e) {
                    if (service.isActive()) {
                        LOG.error("Get an unexpected interrupt when distributing assignments to node, {}", e.getCause());
                    } else {
                        // service is off now just interrupt it.
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }

        private void sendAssignmentsToNode(NodeAssignments assignments) {
            if (this.service.isLocalMode) {
                //local node
                Supervisor supervisor = this.service.localSupervisors.get(assignments.getNode());
                if (supervisor != null) {
                    supervisor.sendSupervisorAssignments(assignments.getAssignments());
                    service.sendAssignmentCallback.nodeAssignmentSent(assignments.getNode(), true);
                } else {
                    LOG.error("Can not find node {} for assignments distribution", assignments.getNode());
                    service.sendAssignmentCallback.nodeAssignmentSent(assignments.getNode(), false);
                    throw new RuntimeException("null for node " + assignments.getNode() + " supervisor instance.");
                }
            } else {
                // distributed mode
                try (SupervisorClient client = SupervisorClient.getConfiguredClient(service.getConf(),
                                                                                    assignments.getHost(), assignments.getServerPort())) {
                    try {
                        client.getIface().sendSupervisorAssignments(assignments.getAssignments());
                        service.sendAssignmentCallback.nodeAssignmentSent(assignments.getNode(), true);
                    } catch (Exception e) {
                        assignments.getMetricsRegistry().getMeter(Constants.NIMBUS_SEND_ASSIGNMENT_EXCEPTIONS).mark();
                        LOG.error("Exception when trying to send assignments to node {}: {}", assignments.getNode(), e.getMessage());
                        service.sendAssignmentCallback.nodeAssignmentSent(assignments.getNode(), false);
                    }
                } catch (Throwable e) {
                    //just ignore any error/exception.
                    LOG.error("Exception to create supervisor client for node {}: {}", assignments.getNode(), e.getMessage());
                }
            }
        }
    }
}

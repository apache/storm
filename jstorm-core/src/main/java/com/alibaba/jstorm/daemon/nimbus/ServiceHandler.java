
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

import backtype.storm.Config;
import backtype.storm.daemon.Shutdownable;
import backtype.storm.generated.*;
import backtype.storm.generated.Nimbus.Iface;
import backtype.storm.utils.BufferFileInputStream;
import backtype.storm.utils.TimeCacheMap;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.callback.impl.RemoveTransitionCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.*;
import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.metric.MetaType;
import com.alibaba.jstorm.metric.MetricUtils;
import com.alibaba.jstorm.metric.SimpleJStormMetric;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.task.TaskInfo;
import com.alibaba.jstorm.task.error.TaskError;
import com.alibaba.jstorm.utils.*;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileExistsException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.security.InvalidParameterException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.jstorm.daemon.nimbus.TopologyMetricsRunnable.*;

/**
 * Thrift callback, all commands handling entrance
 *
 * @author version 1: lixin, version 2:Longda
 */
public class ServiceHandler implements Iface, Shutdownable, DaemonCommon {
    private final static Logger LOG = LoggerFactory.getLogger(ServiceHandler.class);

    public final static int THREAD_NUM = 64;

    private NimbusData data;

    private Map<Object, Object> conf;

    public ServiceHandler(NimbusData data) {
        this.data = data;
        conf = data.getConf();
    }

    /**
     * Shutdown the nimbus
     */
    @Override
    public void shutdown() {
        LOG.info("Begin to shut down master");
        // Timer.cancelTimer(nimbus.getTimer());

        LOG.info("Successfully shut down master");

    }

    @Override
    public boolean waiting() {
        // @@@ TODO
        return false;
    }

    @Override
    public void submitTopology(String name, String uploadedJarLocation, String jsonConf, StormTopology topology) throws TException, AlreadyAliveException,
            InvalidTopologyException, TopologyAssignException {
        SubmitOptions options = new SubmitOptions(TopologyInitialStatus.ACTIVE);
        submitTopologyWithOpts(name, uploadedJarLocation, jsonConf, topology, options);
    }

    private void makeAssignment(String topologyName, String topologyId, TopologyInitialStatus status) throws FailedAssignTopologyException {
        TopologyAssignEvent assignEvent = new TopologyAssignEvent();
        assignEvent.setTopologyId(topologyId);
        assignEvent.setScratch(false);
        assignEvent.setTopologyName(topologyName);
        assignEvent.setOldStatus(Thrift.topologyInitialStatusToStormStatus(status));

        TopologyAssign.push(assignEvent);

        boolean isSuccess = assignEvent.waitFinish();
        if (isSuccess == true) {
            LOG.info("Finish submit for " + topologyName);
        } else {
            throw new FailedAssignTopologyException(assignEvent.getErrorMsg());
        }
    }

    /**
     * Submit one Topology
     *
     * @param topologyName        String: topology name
     * @param uploadedJarLocation String: already uploaded jar path
     * @param jsonConf            String: jsonConf serialize all toplogy configuration to
     *                            Json
     * @param topology            StormTopology: topology Object
     */
    @SuppressWarnings("unchecked")
    @Override
    public void submitTopologyWithOpts(String topologyName, String uploadedJarLocation, String jsonConf, StormTopology topology, SubmitOptions options)
            throws AlreadyAliveException, InvalidTopologyException, TopologyAssignException, TException {
        LOG.info("Receive " + topologyName + ", uploadedJarLocation:" + uploadedJarLocation);
        long start = System.nanoTime();

        //check topologyname is valid
        if (!Common.charValidate(topologyName)) {
            throw new InvalidTopologyException(topologyName + " is not a valid topology name");
        }

        try {
            checkTopologyActive(data, topologyName, false);
        } catch (AlreadyAliveException e) {
            LOG.info(topologyName + " already exists ");
            throw e;
        } catch (Throwable e) {
            LOG.info("Failed to check whether topology is alive or not", e);
            throw new TException(e);
        }

        String topologyId = null;
        synchronized (data) {
            // avoid to the same topologys wered submmitted at the same time
            Set<String> pendingTopologys =
                    data.getPendingSubmitTopoloygs().keySet();
            for (String cachTopologyId : pendingTopologys) {
                if (cachTopologyId.contains(topologyName + "-"))
                    throw new AlreadyAliveException(
                            topologyName + "  were submitted");
            }
            int counter = data.getSubmittedCount().incrementAndGet();
            topologyId = Common.topologyNameToId(topologyName, counter);
            data.getPendingSubmitTopoloygs().put(topologyId, null);
        }
        try {

            Map<Object, Object> serializedConf = (Map<Object, Object>) JStormUtils.from_json(jsonConf);
            if (serializedConf == null) {
                LOG.warn("Failed to serialized Configuration");
                throw new InvalidTopologyException("Failed to serialize topology configuration");
            }

            serializedConf.put(Config.TOPOLOGY_ID, topologyId);
            serializedConf.put(Config.TOPOLOGY_NAME, topologyName);

            Map<Object, Object> stormConf;

            stormConf = NimbusUtils.normalizeConf(conf, serializedConf, topology);
            LOG.info("Normalized configuration:" + stormConf);

            Map<Object, Object> totalStormConf = new HashMap<Object, Object>(conf);
            totalStormConf.putAll(stormConf);

            StormTopology normalizedTopology = NimbusUtils.normalizeTopology(stormConf, topology, true);

            // this validates the structure of the topology
            Common.validate_basic(normalizedTopology, totalStormConf, topologyId);
            // don't need generate real topology, so skip Common.system_topology
            // Common.system_topology(totalStormConf, topology);

            StormClusterState stormClusterState = data.getStormClusterState();

            double metricsSampleRate = ConfigExtension.getMetricSampleRate(stormConf);
            // create /local-dir/nimbus/topologyId/xxxx files
            setupStormCode(conf, topologyId, uploadedJarLocation, stormConf, normalizedTopology);

            // generate TaskInfo for every bolt or spout in ZK
            // /ZK/tasks/topoologyId/xxx
            setupZkTaskInfo(conf, topologyId, stormClusterState);

            // make assignments for a topology
            LOG.info("Submit for " + topologyName + " with conf " + serializedConf);
            makeAssignment(topologyName, topologyId, options.get_initial_status());

            // when make assignment for a topology,so remove the topologyid form
            // pendingSubmitTopologys
            data.getPendingSubmitTopoloygs().remove(topologyId);

            // push start event after startup
            StartTopologyEvent startEvent = new StartTopologyEvent();
            startEvent.clusterName = this.data.getClusterName();
            startEvent.topologyId = topologyId;
            startEvent.timestamp = System.currentTimeMillis();
            startEvent.sampleRate = metricsSampleRate;
            this.data.getMetricRunnable().pushEvent(startEvent);

        } catch (FailedAssignTopologyException e) {
            StringBuilder sb = new StringBuilder();
            sb.append("Fail to sumbit topology, Root cause:");
            if (e.getMessage() == null) {
                sb.append("submit timeout");
            } else {
                sb.append(e.getMessage());
            }

            sb.append("\n\n");
            sb.append("topologyId:" + topologyId);
            sb.append(", uploadedJarLocation:" + uploadedJarLocation + "\n");
            LOG.error(sb.toString(), e);
            data.getPendingSubmitTopoloygs().remove(topologyId);
            throw new TopologyAssignException(sb.toString());
        } catch (InvalidParameterException e) {
            StringBuilder sb = new StringBuilder();
            sb.append("Fail to sumbit topology ");
            sb.append(e.getMessage());
            sb.append(", cause:" + e.getCause());
            sb.append("\n\n");
            sb.append("topologyId:" + topologyId);
            sb.append(", uploadedJarLocation:" + uploadedJarLocation + "\n");
            LOG.error(sb.toString(), e);
            data.getPendingSubmitTopoloygs().remove(topologyId);
            throw new InvalidParameterException(sb.toString());
        } catch (InvalidTopologyException e) {
            LOG.error("Topology is invalid. " + e.get_msg());
            data.getPendingSubmitTopoloygs().remove(topologyId);
            throw e;
        } catch (Throwable e) {
            StringBuilder sb = new StringBuilder();
            sb.append("Fail to sumbit topology ");
            sb.append(e.getMessage());
            sb.append(", cause:" + e.getCause());
            sb.append("\n\n");
            sb.append("topologyId:" + topologyId);
            sb.append(", uploadedJarLocation:" + uploadedJarLocation + "\n");
            LOG.error(sb.toString(), e);
            data.getPendingSubmitTopoloygs().remove(topologyId);
            throw new TopologyAssignException(sb.toString());
        } finally {
            double spend = (System.nanoTime() - start) / TimeUtils.NS_PER_US;
            SimpleJStormMetric.updateNimbusHistogram("submitTopologyWithOpts", spend);
            LOG.info("submitTopologyWithOpts {} costs {}ms", topologyName, spend);
        }

    }

    /**
     * kill topology
     *
     * @param topologyName String topology name
     */
    @Override
    public void killTopology(String topologyName) throws TException, NotAliveException {
        killTopologyWithOpts(topologyName, new KillOptions());

    }

    @Override
    public void killTopologyWithOpts(String topologyName, KillOptions options) throws TException, NotAliveException {
        try {
            checkTopologyActive(data, topologyName, true);

            String topologyId = getTopologyId(topologyName);

            Integer wait_amt = null;
            if (options.is_set_wait_secs()) {
                wait_amt = options.get_wait_secs();
            }
            NimbusUtils.transitionName(data, topologyName, true, StatusType.kill, wait_amt);

            Remove event = new Remove();
            event.topologyId = topologyId;
            data.getMetricRunnable().pushEvent(event);
        } catch (NotAliveException e) {
            String errMsg = "KillTopology Error, no this topology " + topologyName;
            LOG.error(errMsg, e);
            throw new NotAliveException(errMsg);
        } catch (Exception e) {
            String errMsg = "Failed to kill topology " + topologyName;
            LOG.error(errMsg, e);
            throw new TException(errMsg);
        }

    }

    /**
     * set topology status as active
     *
     * @param topologyName
     */
    @Override
    public void activate(String topologyName) throws TException, NotAliveException {
        try {
            NimbusUtils.transitionName(data, topologyName, true, StatusType.activate);
        } catch (NotAliveException e) {
            String errMsg = "Activate Error, no this topology " + topologyName;
            LOG.error(errMsg, e);
            throw new NotAliveException(errMsg);
        } catch (Exception e) {
            String errMsg = "Failed to active topology " + topologyName;
            LOG.error(errMsg, e);
            throw new TException(errMsg);
        }

    }

    /**
     * set topology stauts as deactive
     *
     * @param topologyName
     */
    @Override
    public void deactivate(String topologyName) throws TException, NotAliveException {
        try {
            NimbusUtils.transitionName(data, topologyName, true, StatusType.inactivate);
        } catch (NotAliveException e) {
            String errMsg = "Deactivate Error, no this topology " + topologyName;
            LOG.error(errMsg, e);
            throw new NotAliveException(errMsg);
        } catch (Exception e) {
            String errMsg = "Failed to deactivate topology " + topologyName;
            LOG.error(errMsg, e);
            throw new TException(errMsg);
        }

    }

    /**
     * rebalance one topology
     *
     * @param topologyName topology name
     * @param options      RebalanceOptions
     * @@@ rebalance options hasn't implements
     * <p/>
     * It is used to let workers wait several seconds to finish jobs
     */
    @Override
    public void rebalance(String topologyName, RebalanceOptions options) throws TException, NotAliveException {
        try {
            checkTopologyActive(data, topologyName, true);
            Integer wait_amt = null;
            String jsonConf = null;
            Boolean reassign = false;
            if (options != null) {
                if (options.is_set_wait_secs())
                    wait_amt = options.get_wait_secs();
                if (options.is_set_reassign())
                    reassign = options.is_reassign();
                if (options.is_set_conf())
                    jsonConf = options.get_conf();
            }

            LOG.info("Begin to rebalance " + topologyName + "wait_time:" + wait_amt + ", reassign: " + reassign + ", new worker/bolt configuration:" + jsonConf);

            Map<Object, Object> conf = (Map<Object, Object>) JStormUtils.from_json(jsonConf);

            NimbusUtils.transitionName(data, topologyName, true, StatusType.rebalance, wait_amt, reassign, conf);
        } catch (NotAliveException e) {
            String errMsg = "Rebalance Error, no this topology " + topologyName;
            LOG.error(errMsg, e);
            throw new NotAliveException(errMsg);
        } catch (Exception e) {
            String errMsg = "Failed to rebalance topology " + topologyName;
            LOG.error(errMsg, e);
            throw new TException(errMsg);
        }

    }

    @Override
    public void restart(String name, String jsonConf) throws TException, NotAliveException, InvalidTopologyException, TopologyAssignException {
        LOG.info("Begin to restart " + name + ", new configuration:" + jsonConf);

        // 1. get topologyId
        StormClusterState stormClusterState = data.getStormClusterState();
        String topologyId;
        try {
            topologyId = Cluster.get_topology_id(stormClusterState, name);
        } catch (Exception e2) {
            topologyId = null;
        }
        if (topologyId == null) {
            LOG.info("No topology of " + name);
            throw new NotAliveException("No topology of " + name);
        }

        // Restart the topology: Deactivate -> Kill -> Submit
        // 2. Deactivate
        deactivate(name);
        JStormUtils.sleepMs(5000);
        LOG.info("Deactivate " + name);

        // 3. backup old jar/configuration/topology
        StormTopology topology;
        Map topologyConf;
        String topologyCodeLocation = null;
        try {
            topology = StormConfig.read_nimbus_topology_code(conf, topologyId);
            topologyConf = StormConfig.read_nimbus_topology_conf(conf, topologyId);
            if (jsonConf != null) {
                Map<Object, Object> newConf = (Map<Object, Object>) JStormUtils.from_json(jsonConf);
                topologyConf.putAll(newConf);
            }

            // Copy storm files back to stormdist dir from the tmp dir
            String oldDistDir = StormConfig.masterStormdistRoot(conf, topologyId);
            String parent = StormConfig.masterInbox(conf);
            topologyCodeLocation = parent + PathUtils.SEPERATOR + topologyId;
            FileUtils.forceMkdir(new File(topologyCodeLocation));
            FileUtils.cleanDirectory(new File(topologyCodeLocation));
            File stormDistDir = new File(oldDistDir);
            stormDistDir.setLastModified(System.currentTimeMillis());
            FileUtils.copyDirectory(stormDistDir, new File(topologyCodeLocation));

            LOG.info("Successfully read old jar/conf/topology " + name);
        } catch (Exception e) {
            LOG.error("Failed to read old jar/conf/topology", e);
            if (topologyCodeLocation != null) {
                try {
                    PathUtils.rmr(topologyCodeLocation);
                } catch (IOException ignored) {
                }
            }
            throw new TException("Failed to read old jar/conf/topology ");

        }

        // 4. Kill
        // directly use remove command to kill, more stable than issue kill cmd
        RemoveTransitionCallback killCb = new RemoveTransitionCallback(data, topologyId);
        killCb.execute(new Object[0]);
        LOG.info("Successfully kill the topology " + name);

        // send metric events
        TopologyMetricsRunnable.KillTopologyEvent killEvent = new TopologyMetricsRunnable.KillTopologyEvent();
        killEvent.clusterName = this.data.getClusterName();
        killEvent.topologyId = topologyId;
        killEvent.timestamp = System.currentTimeMillis();
        this.data.getMetricRunnable().pushEvent(killEvent);

        Remove removeEvent = new Remove();
        removeEvent.topologyId = topologyId;
        this.data.getMetricRunnable().pushEvent(removeEvent);

        // 5. submit
        try {
            submitTopology(name, topologyCodeLocation, JStormUtils.to_json(topologyConf), topology);
        } catch (AlreadyAliveException e) {
            LOG.info("Failed to kill the topology" + name);
            throw new TException("Failed to kill the topology" + name);
        } finally {
            try {
                PathUtils.rmr(topologyCodeLocation);
            } catch (IOException ignored) {
            }
        }

    }

    @Override
    public void beginLibUpload(String libName) throws TException {
        try {
            String parent = PathUtils.parent_path(libName);
            PathUtils.local_mkdirs(parent);
            data.getUploaders().put(libName, Channels.newChannel(new FileOutputStream(libName)));
            LOG.info("Begin upload file from client to " + libName);
        } catch (Exception e) {
            LOG.error("Fail to upload jar " + libName, e);
            throw new TException(e);
        }
    }

    /**
     * prepare to uploading topology jar, return the file location
     */
    @Override
    public String beginFileUpload() throws TException {

        String fileLoc = null;
        try {
            String path;
            String key = UUID.randomUUID().toString();
            path = StormConfig.masterInbox(conf) + "/" + key;
            FileUtils.forceMkdir(new File(path));
            FileUtils.cleanDirectory(new File(path));
            fileLoc = path + "/stormjar-" + key + ".jar";

            data.getUploaders().put(fileLoc, Channels.newChannel(new FileOutputStream(fileLoc)));
            LOG.info("Begin upload file from client to " + fileLoc);
            return path;
        } catch (FileNotFoundException e) {
            LOG.error("File not found: " + fileLoc, e);
            throw new TException(e);
        } catch (IOException e) {
            LOG.error("Upload file error: " + fileLoc, e);
            throw new TException(e);
        }
    }

    /**
     * uploading topology jar data
     */
    @Override
    public void uploadChunk(String location, ByteBuffer chunk) throws TException {
        TimeCacheMap<Object, Object> uploaders = data.getUploaders();
        Object obj = uploaders.get(location);
        if (obj == null) {
            throw new TException("File for that location does not exist (or timed out) " + location);
        }
        try {
            if (obj instanceof WritableByteChannel) {
                WritableByteChannel channel = (WritableByteChannel) obj;
                channel.write(chunk);
                uploaders.put(location, channel);
            } else {
                throw new TException("Object isn't WritableByteChannel for " + location);
            }
        } catch (IOException e) {
            String errMsg = " WritableByteChannel write filed when uploadChunk " + location;
            LOG.error(errMsg);
            throw new TException(e);
        }

    }

    @Override
    public void finishFileUpload(String location) throws TException {
        TimeCacheMap<Object, Object> uploaders = data.getUploaders();
        Object obj = uploaders.get(location);
        if (obj == null) {
            throw new TException("File for that location does not exist (or timed out)");
        }
        try {
            if (obj instanceof WritableByteChannel) {
                WritableByteChannel channel = (WritableByteChannel) obj;
                channel.close();
                uploaders.remove(location);
                LOG.info("Finished uploading file from client: " + location);
            } else {
                throw new TException("Object isn't WritableByteChannel for " + location);
            }
        } catch (IOException e) {
            LOG.error(" WritableByteChannel close failed when finishFileUpload " + location);
        }

    }

    @Override
    public String beginFileDownload(String file) throws TException {
        BufferFileInputStream is;
        String id;
        try {
            int bufferSize = JStormUtils.parseInt(conf.get(Config.NIMBUS_THRIFT_MAX_BUFFER_SIZE), 1024 * 1024) / 2;

            is = new BufferFileInputStream(file, bufferSize);
            id = UUID.randomUUID().toString();
            data.getDownloaders().put(id, is);
        } catch (FileNotFoundException e) {
            LOG.error(e + "file:" + file + " not found");
            throw new TException(e);
        }

        return id;
    }

    @Override
    public ByteBuffer downloadChunk(String id) throws TException {
        TimeCacheMap<Object, Object> downloaders = data.getDownloaders();
        Object obj = downloaders.get(id);
        if (obj == null) {
            throw new TException("Could not find input stream for that id");
        }

        try {
            if (obj instanceof BufferFileInputStream) {

                BufferFileInputStream is = (BufferFileInputStream) obj;
                byte[] ret = is.read();
                if (ret != null) {
                    downloaders.put(id, is);
                    return ByteBuffer.wrap(ret);
                }
            } else {
                throw new TException("Object isn't BufferFileInputStream for " + id);
            }
        } catch (IOException e) {
            LOG.error("BufferFileInputStream read failed when downloadChunk ", e);
            throw new TException(e);
        }
        byte[] empty = {};
        return ByteBuffer.wrap(empty);
    }

    @Override
    public void finishFileDownload(String id) throws TException {
        data.getDownloaders().remove(id);
    }

    /**
     * get cluster's summary, it will contain SupervisorSummary and TopologySummary
     *
     * @return ClusterSummary
     */
    @Override
    public ClusterSummary getClusterInfo() throws TException {
        long start = System.nanoTime();
        try {
            StormClusterState stormClusterState = data.getStormClusterState();

            Map<String, Assignment> assignments = new HashMap<String, Assignment>();

            // get TopologySummary
            List<TopologySummary> topologySummaries = NimbusUtils.getTopologySummary(stormClusterState, assignments);

            // all supervisors
            Map<String, SupervisorInfo> supervisorInfos = Cluster.get_all_SupervisorInfo(stormClusterState, null);

            // generate SupervisorSummaries
            List<SupervisorSummary> supervisorSummaries = NimbusUtils.mkSupervisorSummaries(supervisorInfos, assignments);

            NimbusSummary nimbusSummary = NimbusUtils.getNimbusSummary(stormClusterState, supervisorSummaries, data);

            return new ClusterSummary(nimbusSummary, supervisorSummaries, topologySummaries);
        } catch (TException e) {
            LOG.info("Failed to get ClusterSummary ", e);
            throw e;
        } catch (Exception e) {
            LOG.info("Failed to get ClusterSummary ", e);
            throw new TException(e);
        } finally {
            long end = System.nanoTime();
            SimpleJStormMetric.updateNimbusHistogram("getClusterInfo", (end - start) / TimeUtils.NS_PER_US);
        }
    }

    @Override
    public String getVersion() throws TException {
        return Utils.getVersion();
    }

    @Override
    public SupervisorWorkers getSupervisorWorkers(String host) throws NotAliveException, TException {
        long start = System.nanoTime();
        try {

            StormClusterState stormClusterState = data.getStormClusterState();

            String supervisorId = null;
            SupervisorInfo supervisorInfo = null;

            String ip = NetWorkUtils.host2Ip(host);
            String hostName = NetWorkUtils.ip2Host(host);

            // all supervisors
            Map<String, SupervisorInfo> supervisorInfos = Cluster.get_all_SupervisorInfo(stormClusterState, null);

            for (Entry<String, SupervisorInfo> entry : supervisorInfos.entrySet()) {

                SupervisorInfo info = entry.getValue();
                if (info.getHostName().equals(hostName) || info.getHostName().equals(ip)) {
                    supervisorId = entry.getKey();
                    supervisorInfo = info;
                    break;
                }
            }

            if (supervisorId == null) {
                throw new TException("No supervisor of " + host);
            }

            Map<String, Assignment> assignments = Cluster.get_all_assignment(stormClusterState, null);

            Map<Integer, WorkerSummary> portWorkerSummarys = new TreeMap<Integer, WorkerSummary>();

            int usedSlotNumber = 0;

            Map<String, Map<Integer, String>> topologyTaskToComponent = new HashMap<String, Map<Integer, String>>();

            Map<String, MetricInfo> metricInfoMap = new HashMap<String, MetricInfo>();
            for (Entry<String, Assignment> entry : assignments.entrySet()) {
                String topologyId = entry.getKey();
                Assignment assignment = entry.getValue();

                Set<ResourceWorkerSlot> workers = assignment.getWorkers();
                for (ResourceWorkerSlot worker : workers) {
                    if (supervisorId.equals(worker.getNodeId()) == false) {
                        continue;
                    }
                    usedSlotNumber++;

                    Integer port = worker.getPort();
                    WorkerSummary workerSummary = portWorkerSummarys.get(port);
                    if (workerSummary == null) {
                        workerSummary = new WorkerSummary();
                        workerSummary.set_port(port);
                        workerSummary.set_topology(topologyId);
                        workerSummary.set_tasks(new ArrayList<TaskComponent>());

                        portWorkerSummarys.put(port, workerSummary);
                    }

                    Map<Integer, String> taskToComponent = topologyTaskToComponent.get(topologyId);
                    if (taskToComponent == null) {
                        taskToComponent = Cluster.get_all_task_component(stormClusterState, topologyId, null);
                        topologyTaskToComponent.put(topologyId, taskToComponent);
                    }

                    int earliest = TimeUtils.current_time_secs();
                    for (Integer taskId : worker.getTasks()) {
                        TaskComponent taskComponent = new TaskComponent();
                        taskComponent.set_component(taskToComponent.get(taskId));
                        taskComponent.set_taskId(taskId);
                        Integer startTime = assignment.getTaskStartTimeSecs().get(taskId);
                        if (startTime != null && startTime < earliest) {
                            earliest = startTime;
                        }

                        workerSummary.add_to_tasks(taskComponent);
                    }

                    workerSummary.set_uptime(TimeUtils.time_delta(earliest));

                    String workerSlotName = getWorkerSlotName(supervisorInfo.getHostName(), port);
                    List<MetricInfo> workerMetricInfoList = this.data.getMetricCache().getMetricData(topologyId, MetaType.WORKER);
                    if (workerMetricInfoList.size() > 0) {
                        MetricInfo workerMetricInfo = workerMetricInfoList.get(0);
                        // remove metrics that don't belong to current worker
                        for (Iterator<String> itr = workerMetricInfo.get_metrics().keySet().iterator();
                             itr.hasNext(); ) {
                            String metricName = itr.next();
                            if (!metricName.contains(host)) {
                                itr.remove();
                            }
                        }
                        metricInfoMap.put(workerSlotName, workerMetricInfo);
                    }
                }
            }

            List<WorkerSummary> workerList = new ArrayList<WorkerSummary>();
            workerList.addAll(portWorkerSummarys.values());

            Map<String, Integer> supervisorToUsedSlotNum = new HashMap<String, Integer>();
            supervisorToUsedSlotNum.put(supervisorId, usedSlotNumber);
            SupervisorSummary supervisorSummary = NimbusUtils.mkSupervisorSummary(supervisorInfo, supervisorId, supervisorToUsedSlotNum);

            return new SupervisorWorkers(supervisorSummary, workerList, metricInfoMap);

        } catch (TException e) {
            LOG.info("Failed to get ClusterSummary ", e);
            throw e;
        } catch (Exception e) {
            LOG.info("Failed to get ClusterSummary ", e);
            throw new TException(e);
        } finally {
            long end = System.nanoTime();
            SimpleJStormMetric.updateNimbusHistogram("getSupervisorWorkers", (end - start) / TimeUtils.NS_PER_US);
        }
    }

    /**
     * Get TopologyInfo, it contain all data of the topology running status
     *
     * @return TopologyInfo
     */
    @Override
    public TopologyInfo getTopologyInfo(String topologyId) throws NotAliveException, TException {
        long start = System.nanoTime();
        StormClusterState stormClusterState = data.getStormClusterState();

        try {

            // get topology's StormBase
            StormBase base = stormClusterState.storm_base(topologyId, null);
            if (base == null) {
                throw new NotAliveException("No topology of " + topologyId);
            }

            Assignment assignment = stormClusterState.assignment_info(topologyId, null);
            if (assignment == null) {
                throw new NotAliveException("No topology of " + topologyId);
            }

            TopologyTaskHbInfo topologyTaskHbInfo = data.getTasksHeartbeat().get(topologyId);
            Map<Integer, TaskHeartbeat> taskHbMap = null;
            if (topologyTaskHbInfo != null)
                taskHbMap = topologyTaskHbInfo.get_taskHbs();

            Map<Integer, TaskInfo> taskInfoMap = Cluster.get_all_taskInfo(stormClusterState, topologyId);
            Map<Integer, String> taskToComponent = Cluster.get_all_task_component(stormClusterState, topologyId, taskInfoMap);
            Map<Integer, String> taskToType = Cluster.get_all_task_type(stormClusterState, topologyId, taskInfoMap);


            String errorString;
            if (Cluster.is_topology_exist_error(stormClusterState, topologyId)) {
                errorString = "Y";
            } else {
                errorString = "";
            }

            TopologySummary topologySummary = new TopologySummary();
            topologySummary.set_id(topologyId);
            topologySummary.set_name(base.getStormName());
            topologySummary.set_uptimeSecs(TimeUtils.time_delta(base.getLanchTimeSecs()));
            topologySummary.set_status(base.getStatusString());
            topologySummary.set_numTasks(NimbusUtils.getTopologyTaskNum(assignment));
            topologySummary.set_numWorkers(assignment.getWorkers().size());
            topologySummary.set_errorInfo(errorString);

            Map<String, ComponentSummary> componentSummaryMap = new HashMap<String, ComponentSummary>();
            HashMap<String, List<Integer>> componentToTasks = JStormUtils.reverse_map(taskToComponent);
            for (Entry<String, List<Integer>> entry : componentToTasks.entrySet()) {
                String name = entry.getKey();
                List<Integer> taskIds = entry.getValue();
                if (taskIds == null || taskIds.size() == 0) {
                    LOG.warn("No task of component " + name);
                    continue;
                }

                ComponentSummary componentSummary = new ComponentSummary();
                componentSummaryMap.put(name, componentSummary);

                componentSummary.set_name(name);
                componentSummary.set_type(taskToType.get(taskIds.get(0)));
                componentSummary.set_parallel(taskIds.size());
                componentSummary.set_taskIds(taskIds);
            }

            Map<Integer, TaskSummary> taskSummaryMap = new TreeMap<Integer, TaskSummary>();
            Map<Integer, List<TaskError>> taskErrors = Cluster.get_all_task_errors(stormClusterState, topologyId);

            for (Integer taskId : taskInfoMap.keySet()) {
                TaskSummary taskSummary = new TaskSummary();
                taskSummaryMap.put(taskId, taskSummary);

                taskSummary.set_taskId(taskId);
                if (taskHbMap == null) {
                    taskSummary.set_status("Starting");
                    taskSummary.set_uptime(0);
                } else {
                    TaskHeartbeat hb = taskHbMap.get(taskId);
                    if (hb == null) {
                        taskSummary.set_status("Starting");
                        taskSummary.set_uptime(0);
                    } else {
                        boolean isInactive = NimbusUtils.isTaskDead(data, topologyId, taskId);
                        if (isInactive)
                            taskSummary.set_status("INACTIVE");
                        else
                            taskSummary.set_status("ACTIVE");
                        taskSummary.set_uptime(hb.get_uptime());
                    }
                }

                if (StringUtils.isBlank(errorString)) {
                    continue;
                }

                List<TaskError> taskErrorList = taskErrors.get(taskId);
                if (taskErrorList != null && taskErrorList.size() != 0) {
                    for (TaskError taskError : taskErrorList) {
                        ErrorInfo errorInfo = new ErrorInfo(taskError.getError(), taskError.getTimSecs());
                        taskSummary.add_to_errors(errorInfo);
                        String component = taskToComponent.get(taskId);
                        componentSummaryMap.get(component).add_to_errors(errorInfo);
                    }
                }
            }

            for (ResourceWorkerSlot workerSlot : assignment.getWorkers()) {
                String hostname = workerSlot.getHostname();
                int port = workerSlot.getPort();

                for (Integer taskId : workerSlot.getTasks()) {
                    TaskSummary taskSummary = taskSummaryMap.get(taskId);
                    taskSummary.set_host(hostname);
                    taskSummary.set_port(port);
                }
            }

            TopologyInfo topologyInfo = new TopologyInfo();
            topologyInfo.set_topology(topologySummary);
            topologyInfo.set_components(JStormUtils.mk_list(componentSummaryMap.values()));
            topologyInfo.set_tasks(JStormUtils.mk_list(taskSummaryMap.values()));

            // return topology metric & component metric only
            List<MetricInfo> tpMetricList = data.getMetricCache().getMetricData(topologyId, MetaType.TOPOLOGY);
            List<MetricInfo> compMetricList = data.getMetricCache().getMetricData(topologyId, MetaType.COMPONENT);
            List<MetricInfo> workerMetricList = data.getMetricCache().getMetricData(topologyId, MetaType.WORKER);
            MetricInfo taskMetric = MetricUtils.mkMetricInfo();
            MetricInfo streamMetric = MetricUtils.mkMetricInfo();
            MetricInfo nettyMetric = MetricUtils.mkMetricInfo();
            MetricInfo tpMetric, compMetric, workerMetric;

            if (tpMetricList == null || tpMetricList.size() == 0) {
                tpMetric = MetricUtils.mkMetricInfo();
            } else {
                // get the last min topology metric
                tpMetric = tpMetricList.get(tpMetricList.size() - 1);
            }
            if (compMetricList == null || compMetricList.size() == 0) {
                compMetric = MetricUtils.mkMetricInfo();
            } else {
                compMetric = compMetricList.get(0);
            }
            if (workerMetricList == null || workerMetricList.size() == 0) {
                workerMetric = MetricUtils.mkMetricInfo();
            } else {
                workerMetric = workerMetricList.get(0);
            }
            TopologyMetric topologyMetrics = new TopologyMetric(tpMetric, compMetric, workerMetric,
                    taskMetric, streamMetric, nettyMetric);
            topologyInfo.set_metrics(topologyMetrics);

            return topologyInfo;
        } catch (TException e) {
            LOG.info("Failed to get topologyInfo " + topologyId, e);
            throw e;
        } catch (Exception e) {
            LOG.info("Failed to get topologyInfo " + topologyId, e);
            throw new TException("Failed to get topologyInfo" + topologyId);
        } finally {
            long end = System.nanoTime();
            SimpleJStormMetric.updateNimbusHistogram("getTopologyInfo", (end - start) / TimeUtils.NS_PER_US);
        }

    }

    @Override
    public TopologyInfo getTopologyInfoByName(String topologyName) throws NotAliveException, TException {
        String topologyId = getTopologyId(topologyName);
        return getTopologyInfo(topologyId);

    }

    @Override
    public String getNimbusConf() throws TException {
        try {
            return JStormUtils.to_json(data.getConf());
        } catch (Exception e) {
            String err = "Failed to generate Nimbus configuration";
            LOG.error(err, e);
            throw new TException(err);
        }
    }

    /**
     * get topology configuration
     *
     * @param id String: topology id
     * @return String
     */
    @Override
    public String getTopologyConf(String id) throws NotAliveException, TException {
        String rtn;
        try {
            Map<Object, Object> topologyConf = StormConfig.read_nimbus_topology_conf(conf, id);
            rtn = JStormUtils.to_json(topologyConf);
        } catch (IOException e) {
            LOG.info("Failed to get configuration of " + id, e);
            throw new TException(e);
        }
        return rtn;
    }

    @Override
    public String getTopologyId(String topologyName) throws NotAliveException, TException {
        StormClusterState stormClusterState = data.getStormClusterState();

        try {
            // get all active topology's StormBase
            String topologyId = Cluster.get_topology_id(stormClusterState, topologyName);
            if (topologyId != null) {
                return topologyId;
            }

        } catch (Exception e) {
            LOG.info("Failed to get getTopologyId " + topologyName, e);
            throw new TException("Failed to get getTopologyId " + topologyName);
        }

        // topologyId == null
        throw new NotAliveException("No topology of " + topologyName);
    }

    /**
     * get StormTopology throw deserialize local files
     *
     * @param id String: topology id
     * @return StormTopology
     */
    @Override
    public StormTopology getTopology(String id) throws NotAliveException, TException {
        StormTopology topology;
        try {
            StormTopology stormtopology = StormConfig.read_nimbus_topology_code(conf, id);
            if (stormtopology == null) {
                throw new NotAliveException("No topology of " + id);
            }

            Map<Object, Object> topologyConf = (Map<Object, Object>) StormConfig.read_nimbus_topology_conf(conf, id);

            topology = Common.system_topology(topologyConf, stormtopology);
        } catch (Exception e) {
            LOG.error("Failed to get topology " + id + ",", e);
            throw new TException("Failed to get system_topology");
        }
        return topology;
    }

    @Override
    public StormTopology getUserTopology(String id) throws NotAliveException, TException {
        StormTopology topology = null;
        try {
            StormTopology stormtopology = StormConfig.read_nimbus_topology_code(conf, id);
            if (stormtopology == null) {
                throw new NotAliveException("No topology of " + id);
            }

            return topology;
        } catch (Exception e) {
            LOG.error("Failed to get topology " + id + ",", e);
            throw new TException("Failed to get system_topology");
        }

    }

    /**
     * check whether the topology is bActive?
     *
     * @param nimbus
     * @param topologyName
     * @param bActive
     * @throws Exception
     */
    public void checkTopologyActive(NimbusData nimbus, String topologyName, boolean bActive) throws Exception {
        if (isTopologyActive(nimbus.getStormClusterState(), topologyName) != bActive) {
            if (bActive) {
                throw new NotAliveException(topologyName + " is not alive");
            } else {
                throw new AlreadyAliveException(topologyName + " is already active");
            }
        }
    }

    /**
     * whether the topology is active by topology name
     *
     * @param stormClusterState see Cluster_clj
     * @param topologyName
     * @return boolean if the storm is active, return true, otherwise return
     * false;
     * @throws Exception
     */
    public boolean isTopologyActive(StormClusterState stormClusterState, String topologyName) throws Exception {
        boolean rtn = false;
        if (Cluster.get_topology_id(stormClusterState, topologyName) != null) {
            rtn = true;
        }
        return rtn;
    }

    /**
     * create local topology files /local-dir/nimbus/topologyId/stormjar.jar
     * /local-dir/nimbus/topologyId/stormcode.ser
     * /local-dir/nimbus/topologyId/stormconf.ser
     *
     * @param conf
     * @param topologyId
     * @param tmpJarLocation
     * @param stormConf
     * @param topology
     * @throws IOException
     */
    private void setupStormCode(Map<Object, Object> conf, String topologyId, String tmpJarLocation, Map<Object, Object> stormConf, StormTopology topology)
            throws IOException {
        // local-dir/nimbus/stormdist/topologyId
        String stormroot = StormConfig.masterStormdistRoot(conf, topologyId);

        FileUtils.forceMkdir(new File(stormroot));
        FileUtils.cleanDirectory(new File(stormroot));

        // copy jar to /local-dir/nimbus/topologyId/stormjar.jar
        setupJar(conf, tmpJarLocation, stormroot);

        // serialize to file /local-dir/nimbus/topologyId/stormcode.ser
        FileUtils.writeByteArrayToFile(new File(StormConfig.stormcode_path(stormroot)), Utils.serialize(topology));

        // serialize to file /local-dir/nimbus/topologyId/stormconf.ser
        FileUtils.writeByteArrayToFile(new File(StormConfig.stormconf_path(stormroot)), Utils.serialize(stormConf));

        // Update downloadCode timeStamp
        StormConfig.write_nimbus_topology_timestamp(data.getConf(), topologyId, System.currentTimeMillis());
    }

    private boolean copyLibJars(String tmpJarLocation, String stormroot) throws IOException {
        String srcLibPath = StormConfig.stormlib_path(tmpJarLocation);
        String destLibPath = StormConfig.stormlib_path(stormroot);
        LOG.info("Begin to copy from " + srcLibPath + " to " + destLibPath);

        File srcFile = new File(srcLibPath);
        if (srcFile.exists() == false) {
            LOG.info("No lib jars " + srcLibPath);
            return false;
        }
        File destFile = new File(destLibPath);
        FileUtils.copyDirectory(srcFile, destFile);

        PathUtils.rmr(srcLibPath);
        LOG.info("Successfully copy libs " + destLibPath);
        return true;
    }

    /**
     * Copy jar to /local-dir/nimbus/topologyId/stormjar.jar
     *
     * @param conf
     * @param tmpJarLocation
     * @param stormroot
     * @throws IOException
     */
    private void setupJar(Map<Object, Object> conf, String tmpJarLocation, String stormroot) throws IOException {
        if (!StormConfig.local_mode(conf)) {
            boolean existLibs = copyLibJars(tmpJarLocation, stormroot);

            String jarPath = null;
            List<String> files = PathUtils.read_dir_contents(tmpJarLocation);
            for (String file : files) {
                if (file.endsWith(".jar")) {
                    jarPath = tmpJarLocation + PathUtils.SEPERATOR + file;
                    break;
                }
            }

            if (jarPath == null) {
                if (existLibs == false) {
                    throw new IllegalArgumentException("No jar under " + tmpJarLocation);
                } else {
                    LOG.info("No submit jar");
                    return;
                }
            }

            File srcFile = new File(jarPath);
            if (!srcFile.exists()) {
                throw new IllegalArgumentException(jarPath + " to copy to " + stormroot + " does not exist!");
            }

            String path = StormConfig.stormjar_path(stormroot);
            File destFile = new File(path);
            FileUtils.copyFile(srcFile, destFile);
            srcFile.delete();
        }
    }

    /**
     * generate TaskInfo for every bolt or spout in ZK /ZK/tasks/topoologyId/xxx
     *
     * @param conf
     * @param topologyId
     * @param stormClusterState
     * @throws Exception
     */
    public void setupZkTaskInfo(Map<Object, Object> conf, String topologyId, StormClusterState stormClusterState) throws Exception {
        Map<Integer, TaskInfo> taskToTaskInfo = mkTaskComponentAssignments(conf, topologyId);

        // mkdir /ZK/taskbeats/topoologyId
        int masterId = NimbusUtils.getTopologyMasterId(taskToTaskInfo);
        TopologyTaskHbInfo topoTaskHbinfo = new TopologyTaskHbInfo(topologyId, masterId);
        data.getTasksHeartbeat().put(topologyId, topoTaskHbinfo);
        stormClusterState.topology_heartbeat(topologyId, topoTaskHbinfo);

        if (taskToTaskInfo == null || taskToTaskInfo.size() == 0) {
            throw new InvalidTopologyException("Failed to generate TaskIDs map");
        }
        // key is taskid, value is taskinfo
        stormClusterState.set_task(topologyId, taskToTaskInfo);
    }

    /**
     * generate a taskid(Integer) for every task
     *
     * @param conf
     * @param topologyid
     * @return Map<Integer, String>: from taskid to componentid
     * @throws IOException
     * @throws InvalidTopologyException
     */
    public Map<Integer, TaskInfo> mkTaskComponentAssignments(Map<Object, Object> conf, String topologyid) throws IOException, InvalidTopologyException {

        // @@@ here exist a little problem,
        // we can directly pass stormConf from Submit method
        Map<Object, Object> stormConf = StormConfig.read_nimbus_topology_conf(conf, topologyid);
        StormTopology stopology = StormConfig.read_nimbus_topology_code(conf, topologyid);
        StormTopology topology = Common.system_topology(stormConf, stopology);

        return Common.mkTaskInfo(stormConf, topology, topologyid);
    }

    @Override
    public void metricMonitor(String topologyName, MonitorOptions options) throws TException {
        boolean isEnable = options.is_isEnable();
        StormClusterState clusterState = data.getStormClusterState();

        try {
            String topologyId = Cluster.get_topology_id(clusterState, topologyName);
            if (null != topologyId) {
                clusterState.set_storm_monitor(topologyId, isEnable);
            } else {
                throw new NotAliveException("Failed to update metricsMonitor status as " + topologyName + " is not alive");
            }
        } catch (Exception e) {
            String errMsg = "Failed to update metricsMonitor " + topologyName;
            LOG.error(errMsg, e);
            throw new TException(e);
        }

    }

    @Override
    public TopologyMetric getTopologyMetrics(String topologyId) throws TException {
        LOG.debug("Nimbus service handler, getTopologyMetric, topology ID: " + topologyId);
        long start = System.nanoTime();
        try {
            return data.getMetricRunnable().getTopologyMetric(topologyId);
        } finally {
            long end = System.nanoTime();
            SimpleJStormMetric.updateNimbusHistogram("getTopologyMetric", (end - start) / TimeUtils.NS_PER_US);
        }
    }

    @Override
    public void uploadTopologyMetrics(String topologyId, TopologyMetric uploadMetrics) throws TException {
        LOG.info("Received topology metrics:{}", topologyId);

        Update event = new Update();
        event.timestamp = System.currentTimeMillis();
        event.topologyMetrics = uploadMetrics;
        event.topologyId = topologyId;

        data.getMetricRunnable().pushEvent(event);
    }

    @Override
    public Map<String, Long> registerMetrics(String topologyId, Set<String> metrics) throws TException {
        try {
            return data.getMetricRunnable().registerMetrics(topologyId, metrics);
        } catch (Exception ex) {
            return null;
        }
    }

    public void uploadNewCredentials(String topologyName, Credentials creds) {
    }

    @Override
    public List<MetricInfo> getMetrics(String topologyId, int type) throws TException {
        MetaType metaType = MetaType.parse(type);
        return data.getMetricCache().getMetricData(topologyId, metaType);
    }

    @Override
    public MetricInfo getNettyMetrics(String topologyId) throws TException {
        List<MetricInfo> metricInfoList = data.getMetricCache().getMetricData(topologyId, MetaType.NETTY);
        if (metricInfoList != null && metricInfoList.size() > 0) {
            return metricInfoList.get(0);
        }
        return new MetricInfo();
    }

    @Override
    public MetricInfo getNettyMetricsByHost(String topologyId, String host) throws TException {
        MetricInfo ret = new MetricInfo();

        List<MetricInfo> metricInfoList = data.getMetricCache().getMetricData(topologyId, MetaType.NETTY);
        if (metricInfoList != null && metricInfoList.size() > 0) {
            MetricInfo metricInfo = metricInfoList.get(0);
            for (Entry<String, Map<Integer, MetricSnapshot>> metricEntry : metricInfo.get_metrics().entrySet()) {
                String metricName = metricEntry.getKey();
                Map<Integer, MetricSnapshot> data = metricEntry.getValue();
                if (metricName.contains(host)) {
                    ret.put_to_metrics(metricName, data);
                }
            }
        }

        LOG.info("getNettyMetricsByHost, total size:{}", ret.get_metrics_size());
        return ret;
    }

    @Override
    public int getNettyMetricSizeByHost(String topologyId, String host) throws TException {
        return getNettyMetricsByHost(topologyId, host).get_metrics_size();
    }

    @Override
    public MetricInfo getPagingNettyMetrics(String topologyId, String host, int page) throws TException {
        MetricInfo ret = new MetricInfo();

        int start = (page - 1) * MetricUtils.NETTY_METRIC_PAGE_SIZE;
        int end = page * MetricUtils.NETTY_METRIC_PAGE_SIZE;
        int cur = -1;
        List<MetricInfo> metricInfoList = data.getMetricCache().getMetricData(topologyId, MetaType.NETTY);
        if (metricInfoList != null && metricInfoList.size() > 0) {
            MetricInfo metricInfo = metricInfoList.get(0);
            for (Entry<String, Map<Integer, MetricSnapshot>> metricEntry : metricInfo.get_metrics().entrySet()) {
                String metricName = metricEntry.getKey();
                Map<Integer, MetricSnapshot> data = metricEntry.getValue();
                if (metricName.contains(host)) {
                    ++cur;
                    if (cur >= start && cur < end) {
                        ret.put_to_metrics(metricName, data);
                    }
                    if (cur >= end) {
                        break;
                    }
                }
            }
        }

        LOG.info("getNettyMetricsByHost, total size:{}", ret.get_metrics_size());
        return ret;
    }

    @Override
    public MetricInfo getTaskMetrics(String topologyId, String component) throws TException {
        List<MetricInfo> taskMetricList = getMetrics(topologyId, MetaType.TASK.getT());
        if (taskMetricList != null && taskMetricList.size() > 0) {
            MetricInfo metricInfo = taskMetricList.get(0);
            Map<String, Map<Integer, MetricSnapshot>> metrics = metricInfo.get_metrics();
            for (Iterator<String> itr = metrics.keySet().iterator(); itr.hasNext(); ) {
                String metricName = itr.next();
                String[] parts = metricName.split(MetricUtils.DELIM);
                if (parts.length < 7 || !parts[2].equals(component)) {
                    itr.remove();
                }
            }
            LOG.info("taskMetric, total size:{}", metricInfo.get_metrics_size());
            return metricInfo;
        }
        return MetricUtils.mkMetricInfo();
    }

    @Override
    public List<MetricInfo> getTaskAndStreamMetrics(String topologyId, int taskId) throws TException {
        List<MetricInfo> taskMetricList = getMetrics(topologyId, MetaType.TASK.getT());
        List<MetricInfo> streamMetricList = getMetrics(topologyId, MetaType.STREAM.getT());

        String taskIdStr = taskId + "";
        MetricInfo taskMetricInfo;
        if (taskMetricList != null && taskMetricList.size() > 0) {
            taskMetricInfo = taskMetricList.get(0);
            Map<String, Map<Integer, MetricSnapshot>> metrics = taskMetricInfo.get_metrics();
            for (Iterator<String> itr = metrics.keySet().iterator(); itr.hasNext(); ) {
                String metricName = itr.next();
                String[] parts = metricName.split(MetricUtils.DELIM);
                if (parts.length < 7 || !parts[3].equals(taskIdStr)) {
                    itr.remove();
                }
            }
        } else {
            taskMetricInfo = MetricUtils.mkMetricInfo();
        }

        MetricInfo streamMetricInfo;
        if (streamMetricList != null && streamMetricList.size() > 0) {
            streamMetricInfo = streamMetricList.get(0);
            Map<String, Map<Integer, MetricSnapshot>> metrics = streamMetricInfo.get_metrics();
            for (Iterator<String> itr = metrics.keySet().iterator(); itr.hasNext(); ) {
                String metricName = itr.next();
                String[] parts = metricName.split(MetricUtils.DELIM);
                if (parts.length < 7 || !parts[3].equals(taskIdStr)) {
                    itr.remove();
                }
            }
        } else {
            streamMetricInfo = MetricUtils.mkMetricInfo();
        }
        return Lists.newArrayList(taskMetricInfo, streamMetricInfo);
    }

    @Override
    public List<MetricInfo> getSummarizedTopologyMetrics(String topologyId) throws TException {
        return data.getMetricCache().getMetricData(topologyId, MetaType.TOPOLOGY);
    }

    @Override
    public void updateTopology(String name, String uploadedLocation,
            String updateConf) throws NotAliveException,
                    InvalidTopologyException, TException {
        try {
            checkTopologyActive(data, name, true);

            String topologyId = null;
            StormClusterState stormClusterState = data.getStormClusterState();
            topologyId = Cluster.get_topology_id(stormClusterState, name);
            if (topologyId == null) {
                throw new NotAliveException(name);
            }
            if (uploadedLocation != null) {
                String stormroot =
                        StormConfig.masterStormdistRoot(conf, topologyId);

                int lastIndexOf = uploadedLocation.lastIndexOf("/");
                // /local-dir/nimbus/inbox/xxxx/
                String tmpDir = uploadedLocation.substring(0, lastIndexOf);

                // /local-dir/nimbus/inbox/xxxx/stormjar.jar
                String stormJarPath = StormConfig.stormjar_path(tmpDir);

                File file = new File(uploadedLocation);
                if (file.exists()) {
                    file.renameTo(new File(stormJarPath));
                } else {
                    throw new FileNotFoundException("Source \'"
                            + uploadedLocation + "\' does not exist");
                }
                // move fileDir to /local-dir/nimbus/topologyid/
                File srcDir = new File(tmpDir);
                File destDir = new File(stormroot);
                try {
                    FileUtils.moveDirectory(srcDir, destDir);
                } catch (FileExistsException e) {
                    FileUtils.copyDirectory(srcDir, destDir);
                    FileUtils.deleteQuietly(srcDir);
                }
				// Update downloadCode timeStamp
				StormConfig.write_nimbus_topology_timestamp(data.getConf(), topologyId, System.currentTimeMillis());
                LOG.info("update jar of " + name + " successfully");
            }

            Map topoConf = StormConfig.read_nimbus_topology_conf(data.getConf(),
                    topologyId);
            Map<Object, Object> config =
                    (Map<Object, Object>) JStormUtils.from_json(updateConf);
            topoConf.putAll(config);
            StormConfig.write_nimbus_topology_conf(data.getConf(), topologyId,
                    topoConf);

            LOG.info("update topology " + name + " successfully");
            NimbusUtils.transitionName(data, name, true,
                    StatusType.update_topology, config);

        } catch (NotAliveException e) {
            String errMsg = "Error, no this topology " + name;
            LOG.error(errMsg, e);
            throw new NotAliveException(errMsg);
        } catch (Exception e) {
            String errMsg = "Failed to update topology " + name;
            LOG.error(errMsg, e);
            throw new TException(errMsg);
        }

    }

    @Override
    public void updateTaskHeartbeat(TopologyTaskHbInfo taskHbs) throws TException {
        String topologyId = taskHbs.get_topologyId();
        Integer topologyMasterId = taskHbs.get_topologyMasterId();
        TopologyTaskHbInfo nimbusTaskHbs = data.getTasksHeartbeat().get(topologyId);

        if (nimbusTaskHbs == null) {
            nimbusTaskHbs = new TopologyTaskHbInfo(topologyId, topologyMasterId);
            data.getTasksHeartbeat().put(topologyId, nimbusTaskHbs);
        }

        Map<Integer, TaskHeartbeat> nimbusTaskHbMap = nimbusTaskHbs.get_taskHbs();
        if (nimbusTaskHbMap == null) {
            nimbusTaskHbMap = new ConcurrentHashMap<Integer, TaskHeartbeat>();
            nimbusTaskHbs.set_taskHbs(nimbusTaskHbMap);
        }
        
        Map<Integer, TaskHeartbeat> taskHbMap = taskHbs.get_taskHbs();
        if (taskHbMap != null) {
            for (Entry<Integer, TaskHeartbeat> entry : taskHbMap.entrySet()) {
                nimbusTaskHbMap.put(entry.getKey(), entry.getValue());
            }
        }
    }
}

package com.alibaba.jstorm.daemon.supervisor;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.messaging.IContext;
import backtype.storm.utils.LocalState;
import backtype.storm.utils.Time;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.daemon.worker.ProcessSimulator;
import com.alibaba.jstorm.daemon.worker.State;
import com.alibaba.jstorm.daemon.worker.Worker;
import com.alibaba.jstorm.daemon.worker.WorkerHeartbeat;
import com.alibaba.jstorm.daemon.worker.WorkerShutdown;
import com.alibaba.jstorm.task.LocalAssignment;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.PathUtils;
import com.alibaba.jstorm.utils.TimeUtils;

/**
 * SyncProcesses
 * (1) kill bad worker
 * (2) start new worker
 */
class SyncProcessEvent extends ShutdownWork {
    private static Logger                     LOG = Logger.getLogger(SyncProcessEvent.class);
    
    private LocalState                        localState;
    
    private Map                               conf;
    
    private ConcurrentHashMap<String, String> workerThreadPids;
    
    private String                            supervisorId;
    
    private IContext                         sharedContext;
    
    // private Supervisor supervisor;
    
    /**
     * @param conf
     * @param localState
     * @param workerThreadPids
     * @param supervisorId
     * @param sharedContext
     * @param workerThreadPidsReadLock
     * @param workerThreadPidsWriteLock
     */
    public SyncProcessEvent(String supervisorId, Map conf,
            LocalState localState,
            ConcurrentHashMap<String, String> workerThreadPids,
            IContext sharedContext) {
        
        this.supervisorId = supervisorId;
        
        this.conf = conf;
        
        this.localState = localState;
        
        this.workerThreadPids = workerThreadPids;
        
        // right now, sharedContext is null
        this.sharedContext = sharedContext;
    }
    
    /**
     * @@@ Change the old logic
     *     In the old logic, it will store LS_LOCAL_ASSIGNMENTS
     *     Map<String, Integer> into LocalState
     * 
     *     But I don't think LS_LOCAL_ASSIGNMENTS is useful, so remove this
     *     logic
     */
    @SuppressWarnings("unchecked")
    @Override
    public void run() {
        LOG.debug("Syncing processes");
        try {
            
            /**
             * Step 1: get assigned tasks from localstat Map<port(type Integer),
             * LocalAssignment>
             */
            Map<Integer, LocalAssignment> localAssignments = null;
            try {
                localAssignments = (Map<Integer, LocalAssignment>) localState
                        .get(Common.LS_LOCAL_ASSIGNMENTS);
            } catch (IOException e) {
                LOG.error("Failed to get LOCAL_ASSIGNMENTS from LocalState", e);
                throw e;
            }
            
            if (localAssignments == null) {
                localAssignments = new HashMap<Integer, LocalAssignment>();
            }
            LOG.debug("Assigned tasks: " + localAssignments);
            
            /**
             * Step 2: get local WorkerStats from local_dir/worker/ids/heartbeat
             * Map<workerid [WorkerHeartbeat, state]>
             */
            Map<String, StateHeartbeat> localWorkerStats = null;
            try {
                localWorkerStats = getLocalWorkerStats(conf, localState,
                        localAssignments);
            } catch (IOException e) {
                LOG.error("Failed to get Local worker stats");
                throw e;
            }
            LOG.debug("Allocated: " + localWorkerStats);
            
            /**
             * Step 3: kill Invalid Workers and remove killed worker from
             * localWorkerStats
             */
            Set<Integer> keepPorts = killUselessWorkers(localWorkerStats);
            
            // start new workers
            startNewWorkers(keepPorts, localAssignments);
            
        } catch (Exception e) {
            LOG.error("Failed Sync Process", e);
            // throw e
        }
        
    }
    
    /**
     * wait for all workers of the supervisor launch
     * 
     * @param conf
     * @param workerIds
     * @throws InterruptedException
     * @throws IOException
     * @pdOid 52b11418-7474-446d-bff5-0ecd68f4954f
     */
    public void waitForWorkersLaunch(Map conf, Collection<String> workerIds)
            throws IOException, InterruptedException {
        
        int startTime = TimeUtils.current_time_secs();
        
        for (String workerId : workerIds) {
            
            waitForWorkerLaunch(conf, workerId, startTime);
        }
    }
    
    /**
     * wait for worker launch if the time is not > *
     * SUPERVISOR_WORKER_START_TIMEOUT_SECS, otherwise info failed
     * 
     * @param conf
     * @param workerId
     * @param startTime
     * @throws IOException
     * @throws InterruptedException
     * @pdOid f0a6ab43-8cd3-44e1-8fd3-015a2ec51c6a
     */
    public void waitForWorkerLaunch(Map conf, String workerId, int startTime)
            throws IOException, InterruptedException {
        
        LocalState ls = StormConfig.worker_state(conf, workerId);
        
        while (true) {
            
            WorkerHeartbeat whb = (WorkerHeartbeat) ls
                    .get(Common.LS_WORKER_HEARTBEAT);
            if (whb == null
                    && ((TimeUtils.current_time_secs() - startTime) < (Integer) conf
                            .get(Config.SUPERVISOR_WORKER_START_TIMEOUT_SECS))) {
                LOG.info(workerId + " still hasn't started");
                Time.sleep(500);
            } else {
                // whb is valid or timeout
                break;
            }
        }
        
        WorkerHeartbeat whb = (WorkerHeartbeat) ls
                .get(Common.LS_WORKER_HEARTBEAT);
        if (whb == null) {
            LOG.error("Failed to start Worker " + workerId);
        } else {
            LOG.info("Successfully start worker " + workerId);
        }
    }
    
    /**
     * get localstat approved workerId's map
     * 
     * @return Map<workerid [workerheart, state]> [workerheart, state] is also a
     *         map, key is "workheartbeat" and "state"
     * @param conf
     * @param localState
     * @param assignedTasks
     * @throws IOException
     * @pdOid 11c9bebb-d082-4c51-b323-dd3d5522a649
     */
    @SuppressWarnings("unchecked")
    public Map<String, StateHeartbeat> getLocalWorkerStats(Map conf,
            LocalState localState, Map<Integer, LocalAssignment> assignedTasks)
            throws IOException {
        
        Map<String, StateHeartbeat> workeridHbstate = new HashMap<String, StateHeartbeat>();
        
        int now = TimeUtils.current_time_secs();
        
        /**
         * Get Map<workerId, WorkerHeartbeat> from
         * local_dir/worker/ids/heartbeat
         */
        Map<String, WorkerHeartbeat> idToHeartbeat = readWorkerHeartbeats(conf);
        for (Map.Entry<String, WorkerHeartbeat> entry : idToHeartbeat
                .entrySet()) {
            
            String workerid = entry.getKey().toString();
            
            WorkerHeartbeat whb = entry.getValue();
            
            State state = null;
            
            if (whb == null) {
                
                state = State.notStarted;
                
            } else if (matchesAssignment(whb, assignedTasks) == false) {
                
                // workerId isn't approved or
                // isn't assigned task
                state = State.disallowed;
                
            } else if ((now - whb.getTimeSecs()) > (Integer) conf
                    .get(Config.SUPERVISOR_WORKER_TIMEOUT_SECS)) {//
            
                state = State.timedOut;
            } else {
                state = State.valid;
            }
            
            if (state != State.valid) {
                LOG.info("Worker:" + workerid + " state:" + state
                        + " WorkerHeartbeat: " + whb + " at supervisor time-secs "
                        + now);
            }else {
                LOG.debug("Worker:" + workerid + " state:" + state
                        + " WorkerHeartbeat: " + whb + " at supervisor time-secs "
                        + now);
            }
            
            workeridHbstate.put(workerid, new StateHeartbeat(state, whb));
        }
        
        return workeridHbstate;
    }
    
    /**
     * check whether the workerheartbeat is allowed in the assignedTasks
     * 
     * @param whb
     *            : WorkerHeartbeat
     * @param assignedTasks
     * @return boolean if true, the assignments(LS-LOCAL-ASSIGNMENTS) is match
     *         with workerheart if fasle, is not matched
     */
    public boolean matchesAssignment(WorkerHeartbeat whb,
            Map<Integer, LocalAssignment> assignedTasks) {
        
        boolean isMatch = true;
        LocalAssignment localAssignment = assignedTasks.get(whb.getPort());
        
        if (localAssignment == null) {
            isMatch = false;
        } else if (!whb.getTopologyId().equals(localAssignment.getTopologyId())) {
            // topology id not equal
            LOG.info("topology id not equal whb=" + whb.getTopologyId()
                    + ",localAssignment=" + localAssignment.getTopologyId());
            isMatch = false;
        } else if (!(whb.getTaskIds().equals(localAssignment.getTaskIds()))) {
            // task-id isn't equal
            LOG.info("task-id isn't equal whb=" + whb.getTaskIds()
                    + ",localAssignment=" + localAssignment.getTaskIds());
            isMatch = false;
        }
        
        return isMatch;
    }
    
    /**
     * get all workers heartbeats of the supervisor
     * 
     * @param conf
     * @return Map<workerId, WorkerHeartbeat>
     * @throws IOException
     * @throws IOException
     */
    public Map<String, WorkerHeartbeat> readWorkerHeartbeats(Map conf)
            throws IOException {
        
        Map<String, WorkerHeartbeat> workerHeartbeats = new HashMap<String, WorkerHeartbeat>();
        
        // get the path: STORM-LOCAL-DIR/workers
        String path = StormConfig.worker_root(conf);
        
        List<String> workerIds = PathUtils.read_dir_contents(path);
        
        if (workerIds == null) {
            LOG.info("No worker dir under " + path);
            return workerHeartbeats;
            
        }
        
        for (String workerId : workerIds) {
            
            WorkerHeartbeat whb = readWorkerHeartbeat(conf, workerId);
            
            // ATTENTION: whb can be null
            workerHeartbeats.put(workerId, whb);
        }
        return workerHeartbeats;
    }
    
    /**
     * get worker heartbeat by workerid
     * 
     * @param conf
     * @param workerId
     * @returns WorkerHeartbeat
     * @throws IOException
     */
    public WorkerHeartbeat readWorkerHeartbeat(Map conf, String workerId)
            throws IOException {
        
        LocalState ls = StormConfig.worker_state(conf, workerId);
        
        return (WorkerHeartbeat) ls.get(Common.LS_WORKER_HEARTBEAT);
    }
    
    /**
     * launch a worker in local mode
     * 
     * @param conf
     * @param sharedcontext
     * @param stormId
     * @param supervisorId
     * @param port
     * @param workerId
     * @param workerThreadPidsAtom
     * @param workerThreadPidsAtomWriteLock
     * @pdOid 405f44c7-bc1b-4e16-85cc-b59352b6ff5d
     */
    @Deprecated
    public void launchWorker(Map conf, IContext sharedcontext, String stormId,
            String supervisorId, Integer port, String workerId,
            ConcurrentHashMap<String, String> workerThreadPidsAtom)
            throws Exception {
        
        String pid = UUID.randomUUID().toString();
        
        WorkerShutdown worker = Worker.mk_worker(conf, sharedcontext, stormId,
                supervisorId, port, workerId);
        
        ProcessSimulator.registerProcess(pid, worker);
        
        workerThreadPidsAtom.put(workerId, pid);
        
    }
    
    private String getClassPath(String stormjar, String stormHome) {
        
//        String classpath = JStormUtils.current_classpath() + ":" + stormjar;
//        return classpath;
        
        
        String classpath = JStormUtils.current_classpath() ;
        
        String[] classpathes = classpath.split(":");
        
        Set<String> classSet = new HashSet<String>();
        
        for (String classJar : classpathes) {
            classSet.add(classJar);
        }
        
        if (stormHome != null) {
            List<String> stormHomeFiles = PathUtils.read_dir_contents(stormHome);
            
            for (String file : stormHomeFiles) {
                if (file.endsWith(".jar")) {
                    classSet.add(stormHome + "/" + file);
                }
            }
            
            List<String> stormLibFiles = PathUtils.read_dir_contents(stormHome + "/lib");
            for (String file : stormLibFiles) {
                if (file.endsWith(".jar")) {
                    classSet.add(stormHome + "/lib/" + file);
                }
            }
            
        }
        
        StringBuilder sb = new StringBuilder();
        for (String jar : classSet) {
            sb.append(jar + ":");
        }
        sb.append(stormjar);
        
        return sb.toString();
    }
    
    public String getChildOpts(Map stormConf, Map conf) {
        String childopts = " ";
        
        if (ConfigExtension.getWorkerGc(stormConf) != null) {
            childopts += ConfigExtension.getWorkerGc(stormConf);
        } else if (stormConf.get(Config.TOPOLOGY_WORKER_CHILDOPTS) != null) {
            childopts += (String) stormConf.get(Config.TOPOLOGY_WORKER_CHILDOPTS);
        } else if (ConfigExtension.getWorkerGc(conf) != null) {
            childopts += ConfigExtension.getWorkerGc(conf);
        } else if (conf.get(Config.TOPOLOGY_WORKER_CHILDOPTS) != null) {
            childopts += (String) conf.get(Config.TOPOLOGY_WORKER_CHILDOPTS);
        }
        
        return childopts;
    }
    
    /**
     * launch a worker in distributed mode
     * 
     * @param conf
     * @param sharedcontext
     * @param topologyId
     * @param supervisorId
     * @param port
     * @param workerId
     * @throws IOException
     * @pdOid 6ea369dd-5ce2-4212-864b-1f8b2ed94abb
     */
    public void launchWorker(Map conf, IContext sharedcontext,
            String topologyId, String supervisorId, Integer port,
            String workerId, LocalAssignment assignment) throws IOException {
        
        // STORM-LOCAL-DIR/supervisor/stormdist/topologyId
        String stormroot = StormConfig.supervisor_stormdist_root(conf,
                topologyId);
        
        // STORM-LOCAL-DIR/supervisor/stormdist/topologyId/stormjar.jar
        String stormjar = StormConfig.stormjar_path(stormroot);
        
        // get supervisor conf
        Map stormConf = StormConfig.read_supervisor_topology_conf(conf,
                topologyId);
        
        
        // get classpath
        // String[] param = new String[1];
        // param[0] = stormjar;
        // String classpath = JStormUtils.add_to_classpath(
        // JStormUtils.current_classpath(), param);
        
        // get child process parameter
        
        long memSlotSize = ConfigExtension.getMemSlotSize(conf);
        long memSize = memSlotSize * assignment.getMemSlotNum();
        
        String childopts = getChildOpts(stormConf, conf);
        
        // @@@ some hack logic in the old storm, reserve it here
        childopts = childopts.replace("%ID%", port.toString());
        
        String logFileName = topologyId + "-worker-" + port + ".log";
        //String logFileName = "worker-" + port + ".log";
        
        Map<String, String> environment = new HashMap<String, String>();
        environment.put("LD_LIBRARY_PATH",
                (String) conf.get(Config.JAVA_LIBRARY_PATH));
        
        StringBuilder commandSB = new StringBuilder();
        
//        commandSB.append("java -server -Xdebug -Xrunjdwp:transport=dt_socket,address=8000,server=y,suspend=n ");
        commandSB.append("java -server ");
        commandSB.append(childopts);
        commandSB.append(" -Xms" + memSize);
        commandSB.append(" -Xmx" + memSize + " ");
        
        
        commandSB.append(" -Djava.library.path=");
        commandSB.append((String) conf.get(Config.JAVA_LIBRARY_PATH));
        
        commandSB.append(" -Dlogfile.name=");
        commandSB.append(logFileName);
        
        String stormhome = System.getProperty("jstorm.home");
        
        
        
        if (stormhome != null) {
            commandSB.append(" -Dlogback.configurationFile=" + stormhome + "/conf/cluster.xml");
            commandSB.append(" -Djstorm.home=");
            commandSB.append(stormhome);
        }else {
            commandSB.append(" -Dlogback.configurationFile=cluster.xml");
        }
        
        String classpath = getClassPath(stormjar, stormhome);
        String workerClassPath = (String)conf.get(Config.WORKER_CLASSPATH);
        
        //commandSB.append(" -Dlog4j.configuration=storm.log.properties");
        
        commandSB.append(" -cp ");
        commandSB.append(workerClassPath + ":");
        commandSB.append(classpath);
        
        commandSB.append(" com.alibaba.jstorm.daemon.worker.Worker ");
        commandSB.append(topologyId);
        
        commandSB.append(" ");
        commandSB.append(supervisorId);
        
        commandSB.append(" ");
        commandSB.append(port);
        
        commandSB.append(" ");
        commandSB.append(workerId);
        
        LOG.info("Launching worker with command: " + commandSB);
        LOG.info("Environment:" + environment.toString());
        
        
        JStormUtils.launch_process(commandSB.toString(), environment);
    }
    
    private Set<Integer> killUselessWorkers(
            Map<String, StateHeartbeat> localWorkerStats) {
        Set<String> removed = new HashSet<String>();
        Set<Integer> keepPorts = new HashSet<Integer>();
        
        for (Entry<String, StateHeartbeat> entry : localWorkerStats.entrySet()) {
            
            String workerid = entry.getKey();
            StateHeartbeat hbstate = entry.getValue();
            
            if (hbstate.getState().equals(State.valid)) {
                // hbstate.getHeartbeat() won't be null
                keepPorts.add(hbstate.getHeartbeat().getPort());
            } else {
                removed.add(workerid);
                
                StringBuilder sb = new StringBuilder();
                sb.append("Shutting down and clearing state for id ");
                sb.append(workerid);
                sb.append(";State:");
                sb.append(hbstate.getState());
                sb.append(";Heartbeat");
                sb.append(hbstate.getHeartbeat());
                LOG.info(sb);
                
                try {
                    shutWorker(conf, supervisorId, workerid, workerThreadPids);
                } catch (IOException e) {
                    String errMsg = "Failed to shutdown worker workId:"
                            + workerid + ",supervisorId: " + supervisorId
                            + ",workerThreadPids:" + workerThreadPids;
                    LOG.error(errMsg, e);
                }
                
            }
        }
        
        for (String removedWorkerId : removed) {
            localWorkerStats.remove(removedWorkerId);
        }
        
        return keepPorts;
    }
    
    private void startNewWorkers(Set<Integer> keepPorts,
            Map<Integer, LocalAssignment> localAssignments) throws Exception {
        /**
         * Step 4: get reassigned tasks, which is in assignedTasks, but not
         * in
         * keeperPorts Map<port(type Integer), LocalAssignment>
         */
        Map<Integer, LocalAssignment> newWorkers = JStormUtils
                .select_keys_pred(keepPorts, localAssignments);
        
        /**
         * Step 5: generate new work ids
         */
        Map<Integer, String> newWorkerIds = new HashMap<Integer, String>();
        
        for (Entry<Integer, LocalAssignment> entry : newWorkers.entrySet()) {
            Integer port = entry.getKey();
            LocalAssignment assignment = entry.getValue();
            
            String workerId = UUID.randomUUID().toString();
            
            newWorkerIds.put(port, workerId);
            
            // create new worker Id directory
            // LOCALDIR/workers/newworkid/pids
            try {
                StormConfig.worker_pids_root(conf, workerId);
            } catch (IOException e1) {
                LOG.error("Failed to create " + workerId + " localdir", e1);
                throw e1;
            }
            
            StringBuilder sb = new StringBuilder();
            sb.append("Launching worker with assiangment ");
            sb.append(assignment.toString());
            sb.append(" for the supervisor ");
            sb.append(supervisorId);
            sb.append(" on port ");
            sb.append(port);
            sb.append(" with id ");
            sb.append(workerId);
            LOG.info(sb);
            
            try {
                String clusterMode = StormConfig.cluster_mode(conf);
                
                if (clusterMode.equals("distributed")) {
                    launchWorker(conf, sharedContext,
                            assignment.getTopologyId(), supervisorId, port,
                            workerId, assignment);
                } else if (clusterMode.equals("local")) {
                    // in fact, this is no use
                    launchWorker(conf, sharedContext,
                            assignment.getTopologyId(), supervisorId, port,
                            workerId, workerThreadPids);
                }
            } catch (Exception e) {
                String errorMsg = "Failed to launchWorker workerId:" + workerId
                        + ":" + port;
                LOG.error(errorMsg, e);
                throw e;
            }
            
        }
        
        /**
         * FIXME, workerIds should be Set, not Collection,
         * but here simplify the logic
         */
        Collection<String> workerIds = newWorkerIds.values();
        try {
            waitForWorkersLaunch(conf, workerIds);
        } catch (IOException e) {
            LOG.error(e + " waitForWorkersLaunch failed");
        } catch (InterruptedException e) {
            LOG.error(e + " waitForWorkersLaunch failed");
        }
    }
    
}

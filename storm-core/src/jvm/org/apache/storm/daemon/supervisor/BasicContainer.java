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
package org.apache.storm.daemon.supervisor;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.generated.ProfileAction;
import org.apache.storm.generated.ProfileRequest;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * A container that runs processes on the local box.
 */
public class BasicContainer extends Container {
    private static final Logger LOG = LoggerFactory.getLogger(BasicContainer.class);
    private static final FilenameFilter jarFilter = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(".jar");
        }
    };
    private static final Joiner CPJ = 
            Joiner.on(Utils.CLASS_PATH_SEPARATOR).skipNulls();
    
    protected final LocalState _localState;
    protected final String _profileCmd;
    protected final String _stormHome = System.getProperty("storm.home");
    protected volatile boolean _exitedEarly = false;

    private class ProcessExitCallback implements ExitCodeCallback {
        private final String _logPrefix;

        public ProcessExitCallback(String logPrefix) {
            _logPrefix = logPrefix;
        }

        @Override
        public void call(int exitCode) {
            LOG.info("{} exited with code: {}", _logPrefix, exitCode);
            _exitedEarly = true;
        }
    }
    
    /**
     * Create a new BasicContainer
     * @param type the type of container being made.
     * @param conf the supervisor config
     * @param supervisorId the ID of the supervisor this is a part of.
     * @param port the port the container is on.  Should be <= 0 if only a partial recovery
     * @param assignment the assignment for this container. Should be null if only a partial recovery.
     * @param localState the local state of the supervisor.  May be null if partial recovery
     * @param workerId the id of the worker to use.  Must not be null if doing a partial recovery.
     */
    public BasicContainer(ContainerType type, Map<String, Object> conf, String supervisorId, int port,
            LocalAssignment assignment,
            LocalState localState, String workerId) throws IOException {
        this(type, conf, supervisorId, port, assignment, localState, workerId, null, null, null);
    }
    
    /**
     * Create a new BasicContainer
     * @param type the type of container being made.
     * @param conf the supervisor config
     * @param supervisorId the ID of the supervisor this is a part of.
     * @param port the port the container is on.  Should be <= 0 if only a partial recovery
     * @param assignment the assignment for this container. Should be null if only a partial recovery.
     * @param localState the local state of the supervisor.  May be null if partial recovery
     * @param workerId the id of the worker to use.  Must not be null if doing a partial recovery.
     * @param ops file system operations (mostly for testing) if null a new one is made
     * @param topoConf the config of the topology (mostly for testing) if null 
     * and not a partial recovery the real conf is read.
     * @param profileCmd the command to use when profiling (used for testing)
     * @throws IOException on any error
     * @throws ContainerRecoveryException if the Container could not be recovered.
     */
    BasicContainer(ContainerType type, Map<String, Object> conf, String supervisorId, int port,
            LocalAssignment assignment,
            LocalState localState, String workerId, Map<String, Object> topoConf, 
            AdvancedFSOps ops, String profileCmd) throws IOException {
        super(type, conf, supervisorId, port, assignment, workerId, topoConf, ops);
        assert(localState != null);
        _localState = localState;

        if (type.isRecovery() && !type.isOnlyKillable()) {
            synchronized (localState) {
                String wid = null;
                Map<String, Integer> workerToPort = localState.getApprovedWorkers();
                for (Map.Entry<String, Integer> entry : workerToPort.entrySet()) {
                    if (port == entry.getValue().intValue()) {
                        wid = entry.getKey();
                    }
                }
                if (wid == null) {
                    throw new ContainerRecoveryException("Could not find worker id for " + port + " " + assignment);
                }
                LOG.info("Recovered Worker {}", wid);
                _workerId = wid;
            }
        } else if (_workerId == null){
            createNewWorkerId();
        }

        if (profileCmd == null) {
            profileCmd = _stormHome + Utils.FILE_PATH_SEPARATOR + "bin" + Utils.FILE_PATH_SEPARATOR
                    + conf.get(Config.WORKER_PROFILER_COMMAND);
        }
        _profileCmd = profileCmd;
    }

    /**
     * Create a new worker ID for this process and store in in this object and
     * in the local state.  Never call this if a worker is currently up and running.
     * We will lose track of the process.
     */
    protected void createNewWorkerId() {
        _type.assertFull();
        assert(_workerId == null);
        synchronized (_localState) {
            _workerId = Utils.uuid();
            Map<String, Integer> workerToPort = _localState.getApprovedWorkers();
            if (workerToPort == null) {
                workerToPort = new HashMap<>(1);
            }
            removeWorkersOn(workerToPort, _port);
            workerToPort.put(_workerId, _port);
            _localState.setApprovedWorkers(workerToPort);
            LOG.info("Created Worker ID {}", _workerId);
        }
    }

    private static void removeWorkersOn(Map<String, Integer> workerToPort, int _port) {
        for (Iterator<Entry<String, Integer>> i = workerToPort.entrySet().iterator(); i.hasNext();) {
            Entry<String, Integer> found = i.next();
            if (_port == found.getValue().intValue()) {
                LOG.warn("Deleting worker {} from state", found.getKey());
                i.remove();
            }
        }
    }

    @Override
    public void cleanUpForRestart() throws IOException {
        String origWorkerId = _workerId;
        super.cleanUpForRestart();
        synchronized (_localState) {
            Map<String, Integer> workersToPort = _localState.getApprovedWorkers();
            workersToPort.remove(origWorkerId);
            removeWorkersOn(workersToPort, _port);
            _localState.setApprovedWorkers(workersToPort);
            LOG.info("Removed Worker ID {}", origWorkerId);
        }
    }

    @Override
    public void relaunch() throws IOException {
        _type.assertFull();
        //We are launching it now...
        _type = ContainerType.LAUNCH;
        createNewWorkerId();
        setup();
        launch();
    }

    @Override
    public boolean didMainProcessExit() {
        return _exitedEarly;
    }

    /**
     * Run the given command for profiling
     * 
     * @param command
     *            the command to run
     * @param env
     *            the environment to run the command
     * @param logPrefix
     *            the prefix to include in the logs
     * @param targetDir
     *            the working directory to run the command in
     * @return true if it ran successfully, else false
     * @throws IOException
     *             on any error
     * @throws InterruptedException
     *             if interrupted wile waiting for the process to exit.
     */
    protected boolean runProfilingCommand(List<String> command, Map<String, String> env, String logPrefix,
            File targetDir) throws IOException, InterruptedException {
        _type.assertFull();
        Process p = SupervisorUtils.launchProcess(command, env, logPrefix, null, targetDir);
        int ret = p.waitFor();
        return ret == 0;
    }

    @Override
    public boolean runProfiling(ProfileRequest request, boolean stop) throws IOException, InterruptedException {
        _type.assertFull();
        String targetDir = ConfigUtils.workerArtifactsRoot(_conf, _topologyId, _port);

        @SuppressWarnings("unchecked")
        Map<String, String> env = (Map<String, String>) _topoConf.get(Config.TOPOLOGY_ENVIRONMENT);
        if (env == null) {
            env = new HashMap<String, String>();
        }

        String str = ConfigUtils.workerArtifactsPidPath(_conf, _topologyId, _port);

        String workerPid = _ops.slurpString(new File(str)).trim();

        ProfileAction profileAction = request.get_action();
        String logPrefix = "ProfilerAction process " + _topologyId + ":" + _port + " PROFILER_ACTION: " + profileAction
                + " ";

        List<String> command = mkProfileCommand(profileAction, stop, workerPid, targetDir);

        File targetFile = new File(targetDir);
        if (command.size() > 0) {
            return runProfilingCommand(command, env, logPrefix, targetFile);
        }
        LOG.warn("PROFILING REQUEST NOT SUPPORTED {} IGNORED...", request);
        return true;
    }

    /**
     * Get the command to run when doing profiling
     * @param action the profiling action to perform
     * @param stop if this is meant to stop the profiling or start it
     * @param workerPid the PID of the process to profile
     * @param targetDir the current working directory of the worker process
     * @return the command to run for profiling.
     */
    private List<String> mkProfileCommand(ProfileAction action, boolean stop, String workerPid, String targetDir) {
        switch(action) {
            case JMAP_DUMP:
                return jmapDumpCmd(workerPid, targetDir);
            case JSTACK_DUMP:
                return jstackDumpCmd(workerPid, targetDir);
            case JPROFILE_DUMP:
                return jprofileDump(workerPid, targetDir);
            case JVM_RESTART:
                return jprofileJvmRestart(workerPid);
            case JPROFILE_STOP:
                if (stop) {
                    return jprofileStop(workerPid, targetDir);
                }
                return jprofileStart(workerPid);
            default:
                return Lists.newArrayList();
        }
    }

    private List<String> jmapDumpCmd(String pid, String targetDir) {
        return Lists.newArrayList(_profileCmd, pid, "jmap", targetDir);
    }

    private List<String> jstackDumpCmd(String pid, String targetDir) {
        return Lists.newArrayList(_profileCmd, pid, "jstack", targetDir);
    }

    private List<String> jprofileStart(String pid) {
        return Lists.newArrayList(_profileCmd, pid, "start");
    }

    private List<String> jprofileStop(String pid, String targetDir) {
        return Lists.newArrayList(_profileCmd, pid, "stop", targetDir);
    }

    private List<String> jprofileDump(String pid, String targetDir) {
        return Lists.newArrayList(_profileCmd, pid, "dump", targetDir);
    }

    private List<String> jprofileJvmRestart(String pid) {
        return Lists.newArrayList(_profileCmd, pid, "kill");
    }

    /**
     * Compute the java.library.path that should be used for the worker.
     * This helps it to load JNI libraries that are packaged in the uber jar.
     * @param stormRoot the root directory of the worker process
     * @param conf the config for the supervisor.
     * @return the java.library.path/LD_LIBRARY_PATH to use so native libraries load correctly.
     */
    protected String javaLibraryPath(String stormRoot, Map<String, Object> conf) {
        String resourceRoot = stormRoot + Utils.FILE_PATH_SEPARATOR + ConfigUtils.RESOURCES_SUBDIR;
        String os = System.getProperty("os.name").replaceAll("\\s+", "_");
        String arch = System.getProperty("os.arch");
        String archResourceRoot = resourceRoot + Utils.FILE_PATH_SEPARATOR + os + "-" + arch;
        String ret = CPJ.join(archResourceRoot, resourceRoot,
                conf.get(Config.JAVA_LIBRARY_PATH));
        return ret;
    }

    /**
     * Returns a collection of jar file names found under the given directory.
     * @param dir the directory to search
     * @return the jar file names
     */
    protected List<String> getFullJars(File dir) {
        File[] files = dir.listFiles(jarFilter);

        if (files == null) {
            return Collections.emptyList();
        }
        ArrayList<String> ret = new ArrayList<>(files.length);
        for (File f: files) {
            ret.add(f.getAbsolutePath());
        }
        return ret;
    }
    
    protected List<String> frameworkClasspath() {
        File stormLibDir = new File(_stormHome, "lib");
        String stormConfDir =
                System.getenv("STORM_CONF_DIR") != null ?
                System.getenv("STORM_CONF_DIR") :
                new File(_stormHome, "conf").getAbsolutePath();
        File stormExtlibDir = new File(_stormHome, "extlib");
        String extcp = System.getenv("STORM_EXT_CLASSPATH");
        List<String> pathElements = new LinkedList<>();
        pathElements.addAll(getFullJars(stormLibDir));
        pathElements.addAll(getFullJars(stormExtlibDir));
        pathElements.add(extcp);
        pathElements.add(stormConfDir);

        return pathElements;
    }
    
    @SuppressWarnings("unchecked")
    private List<String> asStringList(Object o) {
        if (o instanceof String) {
            return Arrays.asList((String)o);
        } else if (o instanceof List) {
            return (List<String>)o;
        }
        return Collections.EMPTY_LIST;
    }
    
    /**
     * Compute the classpath for the worker process
     * @param stormJar the topology jar
     * @return the full classpath
     */
    protected String getWorkerClassPath(String stormJar) {
        List<String> workercp = new ArrayList<>();
        workercp.addAll(frameworkClasspath());
        workercp.add(stormJar);
        workercp.addAll(asStringList(_topoConf.get(Config.TOPOLOGY_CLASSPATH)));
        return CPJ.join(workercp);
    }

    private String substituteChildOptsInternal(String string, int memOnheap) {
        if (StringUtils.isNotBlank(string)) {
            String p = String.valueOf(_port);
            string = string.replace("%ID%", p);
            string = string.replace("%WORKER-ID%", _workerId);
            string = string.replace("%TOPOLOGY-ID%", _topologyId);
            string = string.replace("%WORKER-PORT%", p);
            if (memOnheap > 0) {
                string = string.replace("%HEAP-MEM%", String.valueOf(memOnheap));
            }
        }
        return string;
    }
    
    protected List<String> substituteChildopts(Object value) {
        return substituteChildopts(value, -1);
    }

    protected List<String> substituteChildopts(Object value, int memOnheap) {
        List<String> rets = new ArrayList<>();
        if (value instanceof String) {
            String string = substituteChildOptsInternal((String) value, memOnheap);
            if (StringUtils.isNotBlank(string)) {
                String[] strings = string.split("\\s+");
                for (String s: strings) {
                    if (StringUtils.isNotBlank(s)) {
                        rets.add(s);
                    }
                }
            }
        } else if (value instanceof List) {
            @SuppressWarnings("unchecked")
            List<String> objects = (List<String>) value;
            for (String object : objects) {
                String str = substituteChildOptsInternal(object, memOnheap);
                if (StringUtils.isNotBlank(str)) {
                    rets.add(str);
                }
            }
        }
        return rets;
    }

    /**
     * Launch the worker process (non-blocking)
     * 
     * @param command
     *            the command to run
     * @param env
     *            the environment to run the command
     * @param processExitcallback
     *            a callback for when the process exits
     * @param logPrefix
     *            the prefix to include in the logs
     * @param targetDir
     *            the working directory to run the command in
     * @return true if it ran successfully, else false
     * @throws IOException
     *             on any error
     */
    protected void launchWorkerProcess(List<String> command, Map<String, String> env, String logPrefix,
            ExitCodeCallback processExitCallback, File targetDir) throws IOException {
        SupervisorUtils.launchProcess(command, env, logPrefix, processExitCallback, targetDir);
    }

    private String getWorkerLoggingConfigFile() {
        String log4jConfigurationDir = (String) (_conf.get(Config.STORM_LOG4J2_CONF_DIR));

        if (StringUtils.isNotBlank(log4jConfigurationDir)) {
            if (!Utils.isAbsolutePath(log4jConfigurationDir)) {
                log4jConfigurationDir = _stormHome + Utils.FILE_PATH_SEPARATOR + log4jConfigurationDir;
            }
        } else {
            log4jConfigurationDir = _stormHome + Utils.FILE_PATH_SEPARATOR + "log4j2";
        }
 
        if (Utils.IS_ON_WINDOWS && !log4jConfigurationDir.startsWith("file:")) {
            log4jConfigurationDir = "file:///" + log4jConfigurationDir;
        }
        return log4jConfigurationDir + Utils.FILE_PATH_SEPARATOR + "worker.xml";
    }
    
    /**
     * Get parameters for the class path of the worker process.  Also used by the
     * log Writer
     * @param stormRoot the root dist dir for the topology
     * @return the classpath for the topology as command line arguments.
     * @throws IOException on any error.
     */
    private List<String> getClassPathParams(final String stormRoot) throws IOException {
        final String stormJar = ConfigUtils.supervisorStormJarPath(stormRoot);
        final String workerClassPath = getWorkerClassPath(stormJar);
        
        List<String> classPathParams = new ArrayList<>();
        classPathParams.add("-cp");
        classPathParams.add(workerClassPath);
        return classPathParams;
    }
    
    /**
     * Get a set of java properties that are common to both the log writer and the worker processes.
     * These are mostly system properties that are used by logging.
     * @return a list of command line options
     */
    private List<String> getCommonParams() {
        final String workersArtifacts = ConfigUtils.workerArtifactsRoot(_conf);
        String stormLogDir = ConfigUtils.getLogDir();
        String log4jConfigurationFile = getWorkerLoggingConfigFile();
        
        List<String> commonParams = new ArrayList<>();
        commonParams.add("-Dlogging.sensitivity=" + OR((String) _topoConf.get(Config.TOPOLOGY_LOGGING_SENSITIVITY), "S3"));
        commonParams.add("-Dlogfile.name=worker.log");
        commonParams.add("-Dstorm.home=" + OR(_stormHome, ""));
        commonParams.add("-Dworkers.artifacts=" + workersArtifacts);
        commonParams.add("-Dstorm.id=" + _topologyId);
        commonParams.add("-Dworker.id=" + _workerId);
        commonParams.add("-Dworker.port=" + _port);
        commonParams.add("-Dstorm.log.dir=" + stormLogDir);
        commonParams.add("-Dlog4j.configurationFile=" + log4jConfigurationFile);
        commonParams.add("-DLog4jContextSelector=org.apache.logging.log4j.core.selector.BasicContextSelector");
        commonParams.add("-Dstorm.local.dir=" + _conf.get(Config.STORM_LOCAL_DIR));
        return commonParams;
    }
    
    private int getMemOnHeap(WorkerResources resources) {
        int memOnheap = 0;
        if (resources != null && resources.is_set_mem_on_heap() && 
                resources.get_mem_on_heap() > 0) {
            memOnheap = (int) Math.ceil(resources.get_mem_on_heap());
        } else {
            // set the default heap memory size for supervisor-test
            memOnheap = Utils.getInt(_topoConf.get(Config.WORKER_HEAP_MEMORY_MB), 768);
        }
        return memOnheap;
    }
    
    private List<String> getWorkerProfilerChildOpts(int memOnheap) {
        List<String> workerProfilerChildopts = new ArrayList<>();
        if (Utils.getBoolean(_conf.get(Config.WORKER_PROFILER_ENABLED), false)) {
            workerProfilerChildopts = substituteChildopts(_conf.get(Config.WORKER_PROFILER_CHILDOPTS), memOnheap);
        }
        return workerProfilerChildopts;
    }
    
    /**
     * a or b the first one that is not null
     * @param a something
     * @param b something else
     * @return a or b the first one that is not null
     */
    private <V> V OR(V a, V b) {
        return a == null ? b : a;
    }
    
    protected String javaCmd(String cmd) {
        String ret = null;
        String javaHome = System.getenv().get("JAVA_HOME");
        if (StringUtils.isNotBlank(javaHome)) {
            ret = javaHome + Utils.FILE_PATH_SEPARATOR + "bin" + Utils.FILE_PATH_SEPARATOR + cmd;
        } else {
            ret = cmd;
        }
        return ret;
    }
    
    /**
     * Create the command to launch the worker process
     * @param memOnheap the on heap memory for the worker
     * @param stormRoot the root dist dir for the topology
     * @param jlp java library path for the topology
     * @return the command to run
     * @throws IOException on any error.
     */
    private List<String> mkLaunchCommand(final int memOnheap, final String stormRoot,
            final String jlp) throws IOException {
        final String javaCmd = javaCmd("java");
        final String stormOptions = ConfigUtils.concatIfNotNull(System.getProperty("storm.options"));
        final String stormConfFile = ConfigUtils.concatIfNotNull(System.getProperty("storm.conf.file"));
        final String workerTmpDir = ConfigUtils.workerTmpRoot(_conf, _workerId);
        
        List<String> classPathParams = getClassPathParams(stormRoot);
        List<String> commonParams = getCommonParams();
        
        List<String> commandList = new ArrayList<>();
        //Log Writer Command...
        commandList.add(javaCmd);
        commandList.addAll(classPathParams);
        commandList.addAll(substituteChildopts(_topoConf.get(Config.TOPOLOGY_WORKER_LOGWRITER_CHILDOPTS)));
        commandList.addAll(commonParams);
        commandList.add("org.apache.storm.LogWriter"); //The LogWriter in turn launches the actual worker.

        //Worker Command...
        commandList.add(javaCmd);
        commandList.add("-server");
        commandList.addAll(commonParams);
        commandList.addAll(substituteChildopts(_conf.get(Config.WORKER_CHILDOPTS), memOnheap));
        commandList.addAll(substituteChildopts(_topoConf.get(Config.TOPOLOGY_WORKER_CHILDOPTS), memOnheap));
        commandList.addAll(substituteChildopts(OR(
                _topoConf.get(Config.TOPOLOGY_WORKER_GC_CHILDOPTS),
                _conf.get(Config.WORKER_GC_CHILDOPTS)), memOnheap));
        commandList.addAll(getWorkerProfilerChildOpts(memOnheap));
        commandList.add("-Djava.library.path=" + jlp);
        commandList.add("-Dstorm.conf.file=" + stormConfFile);
        commandList.add("-Dstorm.options=" + stormOptions);
        commandList.add("-Djava.io.tmpdir=" + workerTmpDir);
        commandList.addAll(classPathParams);
        commandList.add("org.apache.storm.daemon.worker");
        commandList.add(_topologyId);
        commandList.add(_supervisorId);
        commandList.add(String.valueOf(_port));
        commandList.add(_workerId);
        
        return commandList;
    }

    @Override
    public void launch() throws IOException {
        _type.assertFull();
        LOG.info("Launching worker with assignment {} for this supervisor {} on port {} with id {}", _assignment,
                _supervisorId, _port, _workerId);
        String logPrefix = "Worker Process " + _workerId;
        ProcessExitCallback processExitCallback = new ProcessExitCallback(logPrefix);
        _exitedEarly = false;
        
        final WorkerResources resources = _assignment.get_resources();
        final int memOnheap = getMemOnHeap(resources);
        final String stormRoot = ConfigUtils.supervisorStormDistRoot(_conf, _topologyId);
        final String jlp = javaLibraryPath(stormRoot, _conf);
        
        List<String> commandList = mkLaunchCommand(memOnheap, stormRoot, jlp);

        Map<String, String> topEnvironment = new HashMap<String, String>();
        @SuppressWarnings("unchecked")
        Map<String, String> environment = (Map<String, String>) _topoConf.get(Config.TOPOLOGY_ENVIRONMENT);
        if (environment != null) {
            topEnvironment.putAll(environment);
        }
        topEnvironment.put("LD_LIBRARY_PATH", jlp);

        LOG.info("Launching worker with command: {}. ", Utils.shellCmd(commandList));

        String workerDir = ConfigUtils.workerRoot(_conf, _workerId);

        launchWorkerProcess(commandList, topEnvironment, logPrefix, processExitCallback, new File(workerDir));
    }
}

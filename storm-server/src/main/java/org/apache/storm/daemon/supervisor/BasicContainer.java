/*
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

import static org.apache.storm.daemon.nimbus.Nimbus.MIN_VERSION_SUPPORT_RPC_HEARTBEAT;
import static org.apache.storm.utils.Utils.OR;

import java.io.File;
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
import java.util.NavigableMap;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.ServerConstants;
import org.apache.storm.container.ResourceIsolationInterface;
import org.apache.storm.container.oci.OciContainerManager;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.generated.ProfileAction;
import org.apache.storm.generated.ProfileRequest;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.shade.com.google.common.base.Joiner;
import org.apache.storm.shade.com.google.common.collect.Lists;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ServerConfigUtils;
import org.apache.storm.utils.ServerUtils;
import org.apache.storm.utils.SimpleVersion;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A container that runs processes on the local box.
 */
public class BasicContainer extends Container {
    static final TopoMetaLruCache TOPO_META_CACHE = new TopoMetaLruCache();
    private static final Logger LOG = LoggerFactory.getLogger(BasicContainer.class);
    private static final Joiner CPJ = Joiner.on(File.pathSeparator).skipNulls();
    protected final LocalState localState;
    protected final String profileCmd;
    protected final String stormHome = System.getProperty(ConfigUtils.STORM_HOME);
    protected final double hardMemoryLimitMultiplier;
    protected final long hardMemoryLimitOver;
    protected final long lowMemoryThresholdMb;
    protected final long mediumMemoryThresholdMb;
    protected final long mediumMemoryGracePeriodMs;
    protected volatile boolean exitedEarly = false;
    protected volatile long memoryLimitMb;
    protected volatile long memoryLimitExceededStart = -1;

    /**
     * Create a new BasicContainer.
     *
     * @param type                     the type of container being made.
     * @param conf                     the supervisor config
     * @param supervisorId             the ID of the supervisor this is a part of.
     * @param supervisorPort           the thrift server port of the supervisor this is a part of.
     * @param port                     the port the container is on.  Should be <= 0 if only a partial recovery
     * @param assignment               the assignment for this container. Should be null if only a partial recovery.
     * @param resourceIsolationManager used to isolate resources for a container can be null if no isolation is used.
     * @param localState               the local state of the supervisor.  May be null if partial recovery
     * @param workerId                 the id of the worker to use.  Must not be null if doing a partial recovery.
     * @param metricsRegistry          The metrics registry.
     * @param containerMemoryTracker   The shared memory tracker for the supervisor's containers
     */
    public BasicContainer(ContainerType type, Map<String, Object> conf, String supervisorId, int supervisorPort,
                          int port, LocalAssignment assignment, ResourceIsolationInterface resourceIsolationManager,
                          LocalState localState, String workerId, StormMetricsRegistry metricsRegistry, 
                          ContainerMemoryTracker containerMemoryTracker) throws IOException {
        this(type, conf, supervisorId, supervisorPort, port, assignment, resourceIsolationManager, localState,
             workerId, metricsRegistry, containerMemoryTracker, null, null, null);
    }

    /**
     * Create a new BasicContainer.
     *
     * @param type                     the type of container being made.
     * @param conf                     the supervisor config
     * @param supervisorId             the ID of the supervisor this is a part of.
     * @param supervisorPort           the thrift server port of the supervisor this is a part of.
     * @param port                     the port the container is on.  Should be <= 0 if only a partial recovery
     * @param assignment               the assignment for this container. Should be null if only a partial recovery.
     * @param resourceIsolationManager used to isolate resources for a container can be null if no isolation is used.
     * @param localState               the local state of the supervisor.  May be null if partial recovery
     * @param workerId                 the id of the worker to use.  Must not be null if doing a partial recovery.
     * @param metricsRegistry          The metrics registry.
     * @param containerMemoryTracker   The shared memory tracker for the supervisor's containers
     * @param ops                      file system operations (mostly for testing) if null a new one is made
     * @param topoConf                 the config of the topology (mostly for testing) if null and not a partial recovery the real conf is
     *                                 read.
     * @param profileCmd               the command to use when profiling (used for testing) 
     * @throws IOException                on any error
     * @throws ContainerRecoveryException if the Container could not be recovered.
     */
    BasicContainer(ContainerType type, Map<String, Object> conf, String supervisorId, int supervisorPort, int port,
        LocalAssignment assignment, ResourceIsolationInterface resourceIsolationManager, LocalState localState, String workerId,
        StormMetricsRegistry metricsRegistry, ContainerMemoryTracker containerMemoryTracker, Map<String, Object> topoConf,
        AdvancedFSOps ops, String profileCmd) throws IOException {
        super(type, conf, supervisorId, supervisorPort, port, assignment,
            resourceIsolationManager, workerId, topoConf, ops, metricsRegistry, containerMemoryTracker);
        assert (localState != null);
        this.localState = localState;

        if (type.isRecovery() && !type.isOnlyKillable()) {
            synchronized (this.localState) {
                String wid = null;
                Map<String, Integer> workerToPort = this.localState.getApprovedWorkers();
                for (Map.Entry<String, Integer> entry : workerToPort.entrySet()) {
                    if (port == entry.getValue()) {
                        wid = entry.getKey();
                    }
                }
                if (wid == null) {
                    throw new ContainerRecoveryException("Could not find worker id for " + port + " " + assignment);
                }
                LOG.info("Recovered Worker {}", wid);
                this.workerId = wid;
            }
        } else if (this.workerId == null) {
            createNewWorkerId();
        }

        if (resourceIsolationManager instanceof OciContainerManager) {
            //When we use OciContainerManager, we will only use the profiler configured in worker-launcher.cfg due to security reasons
            LOG.debug("Supervisor is using {} as the {}."
                    + "The profiler set at worker.profiler.script.path in worker-launcher.cfg is the only profiler to be used. "
                    + "Please make sure it is configured properly",
                resourceIsolationManager.getClass().getName(), ResourceIsolationInterface.class.getName());
            this.profileCmd = "";
        } else {
            if (profileCmd == null) {
                profileCmd = stormHome + File.separator + "bin" + File.separator
                    + conf.get(DaemonConfig.WORKER_PROFILER_COMMAND);
            }
            this.profileCmd = profileCmd;
        }

        hardMemoryLimitMultiplier =
            ObjectReader.getDouble(conf.get(DaemonConfig.STORM_SUPERVISOR_HARD_MEMORY_LIMIT_MULTIPLIER), 2.0);
        hardMemoryLimitOver =
            ObjectReader.getInt(conf.get(DaemonConfig.STORM_SUPERVISOR_HARD_LIMIT_MEMORY_OVERAGE_MB), 0);
        lowMemoryThresholdMb = ObjectReader.getInt(conf.get(DaemonConfig.STORM_SUPERVISOR_LOW_MEMORY_THRESHOLD_MB), 1024);
        mediumMemoryThresholdMb =
            ObjectReader.getInt(conf.get(DaemonConfig.STORM_SUPERVISOR_MEDIUM_MEMORY_THRESHOLD_MB), 1536);
        mediumMemoryGracePeriodMs =
            ObjectReader.getInt(conf.get(DaemonConfig.STORM_SUPERVISOR_MEDIUM_MEMORY_GRACE_PERIOD_MS), 20_000);

        if (assignment != null) {
            WorkerResources resources = assignment.get_resources();
            memoryLimitMb = calculateMemoryLimit(resources, getMemOnHeap(resources));
        }
    }

    private static void removeWorkersOn(Map<String, Integer> workerToPort, int port) {
        for (Iterator<Entry<String, Integer>> i = workerToPort.entrySet().iterator(); i.hasNext(); ) {
            Entry<String, Integer> found = i.next();
            if (port == found.getValue()) {
                LOG.warn("Deleting worker {} from state", found.getKey());
                i.remove();
            }
        }
    }

    public static List<String> getDependencyLocationsFor(final Map<String, Object> conf, final String topologyId, final AdvancedFSOps ops,
                                                         String stormRoot) throws IOException {
        return TOPO_META_CACHE.get(conf, topologyId, ops, stormRoot).getDepLocs();
    }

    public static String getStormVersionFor(final Map<String, Object> conf, final String topologyId, final AdvancedFSOps ops,
                                            String stormRoot) throws IOException {
        return TOPO_META_CACHE.get(conf, topologyId, ops, stormRoot).getStormVersion();
    }

    /**
     * Create a new worker ID for this process and store in in this object and in the local state.  Never call this if a worker is currently
     * up and running. We will lose track of the process.
     */
    protected void createNewWorkerId() {
        type.assertFull();
        assert (workerId == null);
        synchronized (localState) {
            workerId = Utils.uuid();
            Map<String, Integer> workerToPort = localState.getApprovedWorkers();
            if (workerToPort == null) {
                workerToPort = new HashMap<>(1);
            }
            removeWorkersOn(workerToPort, port);
            workerToPort.put(workerId, port);
            localState.setApprovedWorkers(workerToPort);
            LOG.info("Created Worker ID {}", workerId);
        }
    }

    @Override
    public void cleanUpForRestart() throws IOException {
        String origWorkerId = workerId;
        super.cleanUpForRestart();
        synchronized (localState) {
            Map<String, Integer> workersToPort = localState.getApprovedWorkers();
            if (workersToPort != null) {
                workersToPort.remove(origWorkerId);
                removeWorkersOn(workersToPort, port);
                localState.setApprovedWorkers(workersToPort);
                LOG.info("Removed Worker ID {}", origWorkerId);
            } else {
                LOG.warn("No approved workers exists");
            }
        }
    }

    @Override
    public void relaunch() throws IOException {
        type.assertFull();
        //We are launching it now...
        type = ContainerType.LAUNCH;
        createNewWorkerId();
        setup();
        launch();
    }

    @Override
    public boolean didMainProcessExit() {
        return exitedEarly;
    }

    @Override
    public boolean runProfiling(ProfileRequest request, boolean stop) throws IOException, InterruptedException {
        type.assertFull();
        String targetDir = ConfigUtils.workerArtifactsRoot(conf, topologyId, port);

        @SuppressWarnings("unchecked")
        Map<String, String> env = (Map<String, String>) topoConf.get(Config.TOPOLOGY_ENVIRONMENT);
        if (env == null) {
            env = new HashMap<>();
        }

        String str = ConfigUtils.workerArtifactsPidPath(conf, topologyId, port);

        String workerPid = ops.slurpString(new File(str)).trim();

        ProfileAction profileAction = request.get_action();
        String logPrefix = "ProfilerAction process " + topologyId + ":" + port + " PROFILER_ACTION: " + profileAction
                           + " ";

        List<String> command = mkProfileCommand(profileAction, stop, workerPid, targetDir);

        File targetFile = new File(targetDir);
        if (command.size() > 0) {
            return resourceIsolationManager.runProfilingCommand(getWorkerUser(), workerId, command, env, logPrefix, targetFile);
        }
        LOG.warn("PROFILING REQUEST NOT SUPPORTED {} IGNORED...", request);
        return true;
    }

    /**
     * Get the command to run when doing profiling.
     *
     * @param action    the profiling action to perform
     * @param stop      if this is meant to stop the profiling or start it
     * @param workerPid the PID of the process to profile
     * @param targetDir the current working directory of the worker process
     * @return the command to run for profiling.
     */
    private List<String> mkProfileCommand(ProfileAction action, boolean stop, String workerPid, String targetDir) {
        switch (action) {
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
        return Lists.newArrayList(profileCmd, pid, "jmap", targetDir);
    }

    private List<String> jstackDumpCmd(String pid, String targetDir) {
        return Lists.newArrayList(profileCmd, pid, "jstack", targetDir);
    }

    private List<String> jprofileStart(String pid) {
        return Lists.newArrayList(profileCmd, pid, "start");
    }

    private List<String> jprofileStop(String pid, String targetDir) {
        return Lists.newArrayList(profileCmd, pid, "stop", targetDir);
    }

    private List<String> jprofileDump(String pid, String targetDir) {
        return Lists.newArrayList(profileCmd, pid, "dump", targetDir);
    }

    private List<String> jprofileJvmRestart(String pid) {
        return Lists.newArrayList(profileCmd, pid, "kill");
    }

    /**
     * Compute the java.library.path that should be used for the worker. This helps it to load JNI libraries that are packaged in the uber
     * jar.
     *
     * @param stormRoot the root directory of the worker process
     * @param conf      the config for the supervisor.
     * @return the java.library.path/LD_LIBRARY_PATH to use so native libraries load correctly.
     */
    protected String javaLibraryPath(String stormRoot, Map<String, Object> conf) {
        String resourceRoot = stormRoot + File.separator + ServerConfigUtils.RESOURCES_SUBDIR;
        String os = System.getProperty("os.name").replaceAll("\\s+", "_");
        String arch = System.getProperty("os.arch");
        String archResourceRoot = resourceRoot + File.separator + os + "-" + arch;
        String ret = CPJ.join(archResourceRoot, resourceRoot,
                              conf.get(DaemonConfig.JAVA_LIBRARY_PATH));
        return ret;
    }

    /**
     * Returns a path with a wildcard as the final element, so that the JVM will expand that to all JARs in the directory.
     *
     * @param dir the directory to which a wildcard will be appended
     * @return the path with wildcard ("*") suffix
     */
    protected String getWildcardDir(File dir) {
        return dir.toString() + File.separator + "*";
    }

    protected List<String> frameworkClasspath(SimpleVersion topoVersion) {
        File stormWorkerLibDir = new File(stormHome, "lib-worker");
        String topoConfDir = System.getenv("STORM_CONF_DIR") != null
                ? System.getenv("STORM_CONF_DIR")
                : new File(stormHome, "conf").getAbsolutePath();
        File stormExtlibDir = new File(stormHome, "extlib");
        String extcp = System.getenv("STORM_EXT_CLASSPATH");
        List<String> pathElements = new LinkedList<>();
        pathElements.add(getWildcardDir(stormWorkerLibDir));
        pathElements.add(getWildcardDir(stormExtlibDir));
        pathElements.add(extcp);
        pathElements.add(topoConfDir);

        NavigableMap<SimpleVersion, List<String>> classpaths = Utils.getConfiguredClasspathVersions(conf, pathElements);

        return Utils.getCompatibleVersion(classpaths, topoVersion, "classpath", pathElements);
    }

    protected String getWorkerMain(SimpleVersion topoVersion) {
        String defaultWorkerGuess = "org.apache.storm.daemon.worker.Worker";
        if (topoVersion.getMajor() == 0) {
            //Prior to the org.apache change
            defaultWorkerGuess = "backtype.storm.daemon.worker";
        } else if (topoVersion.getMajor() == 1) {
            //Have not moved to a java worker yet
            defaultWorkerGuess = "org.apache.storm.daemon.worker";
        }
        NavigableMap<SimpleVersion, String> mains = Utils.getConfiguredWorkerMainVersions(conf);
        return Utils.getCompatibleVersion(mains, topoVersion, "worker main class", defaultWorkerGuess);
    }

    protected String getWorkerLogWriter(SimpleVersion topoVersion) {
        String defaultGuess = "org.apache.storm.LogWriter";
        if (topoVersion.getMajor() == 0) {
            //Prior to the org.apache change
            defaultGuess = "backtype.storm.LogWriter";
        }
        NavigableMap<SimpleVersion, String> mains = Utils.getConfiguredWorkerLogWriterVersions(conf);
        return Utils.getCompatibleVersion(mains, topoVersion, "worker log writer class", defaultGuess);
    }

    @SuppressWarnings("unchecked")
    private List<String> asStringList(Object o) {
        if (o instanceof String) {
            return Arrays.asList((String) o);
        } else if (o instanceof List) {
            return (List<String>) o;
        }
        return Collections.EMPTY_LIST;
    }

    /**
     * Compute the classpath for the worker process.
     *
     * @param stormJar            the topology jar
     * @param dependencyLocations any dependencies from the topology
     * @param topoVersion         the version of the storm framework to use
     * @return the full classpath
     */
    protected String getWorkerClassPath(String stormJar, List<String> dependencyLocations, SimpleVersion topoVersion) {
        List<String> workercp = new ArrayList<>();
        workercp.addAll(asStringList(topoConf.get(Config.TOPOLOGY_CLASSPATH_BEGINNING)));
        workercp.addAll(frameworkClasspath(topoVersion));
        workercp.add(stormJar);
        workercp.addAll(dependencyLocations);
        workercp.addAll(asStringList(topoConf.get(Config.TOPOLOGY_CLASSPATH)));
        return CPJ.join(workercp);
    }

    private String substituteChildOptsInternal(String string, int memOnheap, int memOffheap) {
        if (StringUtils.isNotBlank(string)) {
            String p = String.valueOf(port);
            string = string.replace("%ID%", p);
            string = string.replace("%WORKER-ID%", workerId);
            string = string.replace("%TOPOLOGY-ID%", topologyId);
            string = string.replace("%WORKER-PORT%", p);
            if (memOnheap > 0) {
                string = string.replace("%HEAP-MEM%", String.valueOf(memOnheap));
            }
            if (memOffheap > 0) {
                string = string.replace("%OFF-HEAP-MEM%", String.valueOf(memOffheap));
            }
            if (memoryLimitMb > 0) {
                string = string.replace("%LIMIT-MEM%", String.valueOf(memoryLimitMb));
            }
        }
        return string;
    }

    protected List<String> substituteChildopts(Object value) {
        return substituteChildopts(value, -1, -1);
    }

    protected List<String> substituteChildopts(Object value, int memOnheap, int memOffHeap) {
        List<String> rets = new ArrayList<>();
        if (value instanceof String) {
            String string = substituteChildOptsInternal((String) value, memOnheap, memOffHeap);
            if (StringUtils.isNotBlank(string)) {
                String[] strings = string.split("\\s+");
                for (String s : strings) {
                    if (StringUtils.isNotBlank(s)) {
                        rets.add(s);
                    }
                }
            }
        } else if (value instanceof List) {
            @SuppressWarnings("unchecked")
            List<String> objects = (List<String>) value;
            for (String object : objects) {
                String str = substituteChildOptsInternal(object, memOnheap, memOffHeap);
                if (StringUtils.isNotBlank(str)) {
                    rets.add(str);
                }
            }
        }
        return rets;
    }

    private String getWorkerLoggingConfigFile() {
        String log4jConfigurationDir = (String) (conf.get(DaemonConfig.STORM_LOG4J2_CONF_DIR));

        if (StringUtils.isNotBlank(log4jConfigurationDir)) {
            if (!ServerUtils.isAbsolutePath(log4jConfigurationDir)) {
                log4jConfigurationDir = stormHome + File.separator + log4jConfigurationDir;
            }
        } else {
            log4jConfigurationDir = stormHome + File.separator + "log4j2";
        }

        if (ServerUtils.IS_ON_WINDOWS && !log4jConfigurationDir.startsWith("file:")) {
            log4jConfigurationDir = "file:///" + log4jConfigurationDir;
        }
        return log4jConfigurationDir + File.separator + "worker.xml";
    }

    /**
     * Get parameters for the class path of the worker process.  Also used by the log Writer.
     *
     * @param stormRoot the root dist dir for the topology
     * @return the classpath for the topology as command line arguments.
     *
     * @throws IOException on any error.
     */
    private List<String> getClassPathParams(final String stormRoot, final SimpleVersion topoVersion) throws IOException {
        final String stormJar = ConfigUtils.supervisorStormJarPath(stormRoot);
        final List<String> dependencyLocations = getDependencyLocationsFor(conf, topologyId, ops, stormRoot);
        final String workerClassPath = getWorkerClassPath(stormJar, dependencyLocations, topoVersion);

        List<String> classPathParams = new ArrayList<>();
        classPathParams.add("-cp");
        classPathParams.add(workerClassPath);
        return classPathParams;
    }

    /**
     * Get a set of java properties that are common to both the log writer and the worker processes. These are mostly system properties that
     * are used by logging.
     *
     * @return a list of command line options
     */
    private List<String> getCommonParams() {
        final String workersArtifacts = ConfigUtils.workerArtifactsRoot(conf);
        String stormLogDir = ConfigUtils.getLogDir();
        
        List<String> commonParams = new ArrayList<>();
        commonParams.add("-Dlogging.sensitivity=" + OR((String) topoConf.get(Config.TOPOLOGY_LOGGING_SENSITIVITY), "S3"));
        commonParams.add("-Dlogfile.name=worker.log");
        commonParams.add("-Dstorm.home=" + OR(stormHome, ""));
        commonParams.add("-Dworkers.artifacts=" + workersArtifacts);
        commonParams.add("-Dstorm.id=" + topologyId);
        commonParams.add("-Dworker.id=" + workerId);
        commonParams.add("-Dworker.port=" + port);
        commonParams.add("-Dstorm.log.dir=" + stormLogDir);
        commonParams.add("-DLog4jContextSelector=org.apache.logging.log4j.core.selector.BasicContextSelector");
        commonParams.add("-Dstorm.local.dir=" + conf.get(Config.STORM_LOCAL_DIR));
        if (memoryLimitMb > 0) {
            commonParams.add("-Dworker.memory_limit_mb=" + memoryLimitMb);
        }
        return commonParams;
    }

    private int getMemOnHeap(WorkerResources resources) {
        int memOnheap = 0;
        if (resources != null
                && resources.is_set_mem_on_heap()
                && resources.get_mem_on_heap() > 0) {
            memOnheap = (int) Math.ceil(resources.get_mem_on_heap());
        } else {
            // set the default heap memory size for supervisor-test
            memOnheap = ObjectReader.getInt(topoConf.get(Config.WORKER_HEAP_MEMORY_MB), 768);
        }
        return memOnheap;
    }

    private int getMemOffHeap(WorkerResources resources) {
        int memOffheap = 0;
        if (resources != null && resources.is_set_mem_off_heap() && resources.get_mem_off_heap() > 0) {
            memOffheap = (int) Math.ceil(resources.get_mem_off_heap());
        }
        return memOffheap;
    }

    private List<String> getWorkerProfilerChildOpts(int memOnheap, int memOffheap) {
        List<String> workerProfilerChildopts = new ArrayList<>();
        if (ObjectReader.getBoolean(conf.get(DaemonConfig.WORKER_PROFILER_ENABLED), false)) {
            workerProfilerChildopts = substituteChildopts(
                    conf.get(DaemonConfig.WORKER_PROFILER_CHILDOPTS), memOnheap, memOffheap
            );
        }
        return workerProfilerChildopts;
    }

    protected String javaCmd(String cmd) {
        String ret = null;
        String javaHome = System.getenv().get("JAVA_HOME");
        if (StringUtils.isNotBlank(javaHome)) {
            ret = javaHome + File.separator + "bin" + File.separator + cmd;
        } else {
            ret = cmd;
        }
        return ret;
    }

    /**
     * Create the command to launch the worker process.
     *
     * @param memOnheap the on heap memory for the worker
     * @param stormRoot the root dist dir for the topology
     * @param jlp       java library path for the topology
     * @return the command to run
     *
     * @throws IOException on any error.
     */
    private List<String> mkLaunchCommand(final int memOnheap, final int memOffheap, final String stormRoot,
                                         final String jlp, final String numaId) throws IOException {
        final String javaCmd = javaCmd("java");
        final String stormOptions = ConfigUtils.concatIfNotNull(System.getProperty("storm.options"));
        final String topoConfFile = ConfigUtils.concatIfNotNull(System.getProperty("storm.conf.file"));
        final String workerTmpDir = ConfigUtils.workerTmpRoot(conf, workerId);
        String topoVersionString = getStormVersionFor(conf, topologyId, ops, stormRoot);
        if (topoVersionString == null) {
            topoVersionString = (String) conf.getOrDefault(Config.SUPERVISOR_WORKER_DEFAULT_VERSION, VersionInfo.getVersion());
        }
        final SimpleVersion topoVersion = new SimpleVersion(topoVersionString);

        List<String> classPathParams = getClassPathParams(stormRoot, topoVersion);
        List<String> commonParams = getCommonParams();

        String log4jConfigurationFile = getWorkerLoggingConfigFile();
        String workerLog4jConfig = log4jConfigurationFile;
        if (topoConf.get(Config.TOPOLOGY_LOGGING_CONFIG_FILE) != null) {
            workerLog4jConfig = workerLog4jConfig + "," + topoConf.get(Config.TOPOLOGY_LOGGING_CONFIG_FILE);
        }

        List<String> commandList = new ArrayList<>();
        String logWriter = getWorkerLogWriter(topoVersion);
        if (logWriter != null) {
            //Log Writer Command...
            commandList.add(javaCmd);
            commandList.addAll(classPathParams);
            commandList.addAll(substituteChildopts(topoConf.get(Config.TOPOLOGY_WORKER_LOGWRITER_CHILDOPTS)));
            commandList.addAll(commonParams);
            commandList.add("-Dlog4j.configurationFile=" + log4jConfigurationFile);
            commandList.add(logWriter); //The LogWriter in turn launches the actual worker.
        }

        //Worker Command...
        commandList.add(javaCmd);
        commandList.add("-server");
        commandList.addAll(commonParams);
        commandList.add("-Dlog4j.configurationFile=" + workerLog4jConfig);
        commandList.addAll(substituteChildopts(conf.get(Config.WORKER_CHILDOPTS), memOnheap, memOffheap));
        commandList.addAll(substituteChildopts(topoConf.get(Config.TOPOLOGY_WORKER_CHILDOPTS), memOnheap, memOffheap));
        commandList.addAll(substituteChildopts(Utils.OR(
            topoConf.get(Config.TOPOLOGY_WORKER_GC_CHILDOPTS),
            conf.get(Config.WORKER_GC_CHILDOPTS)), memOnheap, memOffheap));
        commandList.addAll(getWorkerProfilerChildOpts(memOnheap, memOffheap));
        commandList.add("-Djava.library.path=" + jlp);
        commandList.add("-Dstorm.conf.file=" + topoConfFile);
        commandList.add("-Dstorm.options=" + stormOptions);
        commandList.add("-Djava.io.tmpdir=" + workerTmpDir);
        commandList.addAll(classPathParams);
        commandList.add(getWorkerMain(topoVersion));
        commandList.add(topologyId);
        String supervisorId = this.supervisorId;
        if (numaId != null) {
            supervisorId += ServerConstants.NUMA_ID_SEPARATOR + numaId;
        }
        commandList.add(supervisorId);
        // supervisor port should be only presented to worker which supports RPC heartbeat
        // unknown version should be treated as "current version", which supports RPC heartbeat
        if ((topoVersion.getMajor() == -1 && topoVersion.getMinor() == -1)
                || topoVersion.compareTo(MIN_VERSION_SUPPORT_RPC_HEARTBEAT) >= 0) {
            commandList.add(String.valueOf(supervisorPort));
        }

        commandList.add(String.valueOf(port));
        commandList.add(workerId);

        return commandList;
    }

    @Override
    public boolean isMemoryLimitViolated(LocalAssignment withUpdatedLimits) throws IOException {
        if (super.isMemoryLimitViolated(withUpdatedLimits)) {
            return true;
        }
        if (resourceIsolationManager.isResourceManaged()) {
            // In the short term the goal is to not shoot anyone unless we really need to.
            // The on heap should limit the memory usage in most cases to a reasonable amount
            // If someone is using way more than they requested this is a bug and we should
            // not allow it
            long usageMb;
            long memoryLimitMb;
            long hardMemoryLimitOver;
            String typeOfCheck;

            if (withUpdatedLimits.is_set_total_node_shared()) {
                //We need to do enforcement on a topology level, not a single worker level...
                // Because in for cgroups each page in shared memory goes to the worker that touched it
                // first. We may need to make this more plugable in the future and let the resource
                // isolation manager tell us what to do
                usageMb = getTotalTopologyMemoryUsed();
                memoryLimitMb = getTotalTopologyMemoryReserved(withUpdatedLimits);
                hardMemoryLimitOver = this.hardMemoryLimitOver * getTotalWorkersForThisTopology();
                typeOfCheck = "TOPOLOGY " + topologyId;
            } else {
                usageMb = getMemoryUsageMb();
                memoryLimitMb = this.memoryLimitMb;
                hardMemoryLimitOver = this.hardMemoryLimitOver;
                typeOfCheck = "WORKER " + workerId;
            }
            LOG.debug(
                "Enforcing memory usage for {} with usage of {} out of {} total and a hard limit of {}",
                typeOfCheck,
                usageMb,
                memoryLimitMb,
                hardMemoryLimitOver);

            if (usageMb <= 0) {
                //Looks like usage might not be supported
                return false;
            }
            long hardLimitMb = Math.max((long) (memoryLimitMb * hardMemoryLimitMultiplier), memoryLimitMb + hardMemoryLimitOver);
            if (usageMb > hardLimitMb) {
                LOG.warn(
                    "{} is using {} MB > adjusted hard limit {} MB", typeOfCheck, usageMb, hardLimitMb);
                return true;
            }
            if (usageMb > memoryLimitMb) {
                //For others using too much it is really a question of how much memory is free in the system
                // to be use. If we cannot calculate it assume that it is bad
                long systemFreeMemoryMb = 0;
                try {
                    systemFreeMemoryMb = resourceIsolationManager.getSystemFreeMemoryMb();
                } catch (IOException e) {
                    LOG.warn("Error trying to calculate free memory on the system {}", e);
                }
                LOG.debug("SYSTEM MEMORY FREE {} MB", systemFreeMemoryMb);
                //If the system is low on memory we cannot be kind and need to shoot something
                if (systemFreeMemoryMb <= lowMemoryThresholdMb) {
                    LOG.warn(
                        "{} is using {} MB > memory limit {} MB and system is low on memory {} free",
                        typeOfCheck,
                        usageMb,
                        memoryLimitMb,
                        systemFreeMemoryMb);
                    return true;
                }

                //If the system still has some free memory give them a grace period to
                // drop back down.
                if (systemFreeMemoryMb < mediumMemoryThresholdMb) {
                    if (memoryLimitExceededStart < 0) {
                        memoryLimitExceededStart = Time.currentTimeMillis();
                    } else {
                        long timeInViolation = Time.currentTimeMillis() - memoryLimitExceededStart;
                        if (timeInViolation > mediumMemoryGracePeriodMs) {
                            LOG.warn(
                                "{} is using {} MB > memory limit {} MB for {} seconds",
                                typeOfCheck,
                                usageMb,
                                memoryLimitMb,
                                timeInViolation / 1000);
                            return true;
                        }
                    }
                } else {
                    //Otherwise don't bother them
                    LOG.debug("{} is using {} MB > memory limit {} MB", typeOfCheck, usageMb, memoryLimitMb);
                    memoryLimitExceededStart = -1;
                }
            } else {
                memoryLimitExceededStart = -1;
            }
        }
        return false;
    }

    @Override
    public long getMemoryUsageMb() {
        try {
            long ret = 0;
            if (resourceIsolationManager.isResourceManaged()) {
                long usageBytes = resourceIsolationManager.getMemoryUsage(getWorkerUser(), workerId, port);
                if (usageBytes >= 0) {
                    ret = usageBytes / 1024 / 1024;
                }
            }
            return ret;
        } catch (IOException e) {
            LOG.warn("Error trying to calculate worker memory usage {}", e);
            return 0;
        }
    }

    @Override
    public long getMemoryReservationMb() {
        return memoryLimitMb;
    }

    private long calculateMemoryLimit(final WorkerResources resources, final int memOnHeap) {
        long ret = memOnHeap;
        if (resourceIsolationManager.isResourceManaged()) {
            final int memoffheap = (int) Math.ceil(resources.get_mem_off_heap());
            final int extraMem =
                (int)
                    (Math.ceil(
                        ObjectReader.getDouble(
                            conf.get(DaemonConfig.STORM_SUPERVISOR_MEMORY_LIMIT_TOLERANCE_MARGIN_MB),
                            0.0)));
            ret += memoffheap + extraMem;
        }
        return ret;
    }

    @Override
    public void launch() throws IOException {
        type.assertFull();
        String numaId = SupervisorUtils.getNumaIdForPort(port, conf);
        if (numaId == null) {
            LOG.info("Launching worker with assignment {} for this supervisor {} on port {} with id {}",
                    assignment, supervisorId, port, workerId);
        } else {
            LOG.info("Launching worker with assignment {} for this supervisor {} on port {} with id {}  bound to numa zone {}",
                    assignment, supervisorId, port, workerId, numaId);
        }
        exitedEarly = false;

        final WorkerResources resources = assignment.get_resources();
        final int memOnHeap = getMemOnHeap(resources);
        final int memOffHeap = getMemOffHeap(resources);
        memoryLimitMb = calculateMemoryLimit(resources, memOnHeap);
        final String stormRoot = ConfigUtils.supervisorStormDistRoot(conf, topologyId);
        String jlp = javaLibraryPath(stormRoot, conf);

        Map<String, String> topEnvironment = new HashMap<String, String>();
        @SuppressWarnings("unchecked")
        Map<String, String> environment = (Map<String, String>) topoConf.get(Config.TOPOLOGY_ENVIRONMENT);
        if (environment != null) {
            topEnvironment.putAll(environment);
        }

        String ldLibraryPath = topEnvironment.get("LD_LIBRARY_PATH");
        if (ldLibraryPath != null) {
            jlp = jlp + System.getProperty("path.separator") + ldLibraryPath;
        }

        topEnvironment.put("LD_LIBRARY_PATH", jlp);

        if (resourceIsolationManager.isResourceManaged()) {
            final int cpu = (int) Math.ceil(resources.get_cpu());
            //Save the memory limit so we can enforce it less strictly
            resourceIsolationManager.reserveResourcesForWorker(workerId, (int) memoryLimitMb, cpu, numaId);
        }

        List<String> commandList = mkLaunchCommand(memOnHeap, memOffHeap, stormRoot, jlp, numaId);

        LOG.info("Launching worker with command: {}. ", ServerUtils.shellCmd(commandList));

        String workerDir = ConfigUtils.workerRoot(conf, workerId);

        String logPrefix = "Worker Process " + workerId;
        ProcessExitCallback processExitCallback = new ProcessExitCallback(logPrefix);
        resourceIsolationManager.launchWorkerProcess(getWorkerUser(), topologyId, topoConf, port, workerId,
            commandList, topEnvironment, logPrefix, processExitCallback, new File(workerDir));
    }

    private static class TopologyMetaData {
        private final Map<String, Object> conf;
        private final String topologyId;
        private final AdvancedFSOps ops;
        private final String stormRoot;
        private boolean dataCached = false;
        private List<String> depLocs = null;
        private String stormVersion = null;

        TopologyMetaData(final Map<String, Object> conf, final String topologyId, final AdvancedFSOps ops, final String stormRoot) {
            this.conf = conf;
            this.topologyId = topologyId;
            this.ops = ops;
            this.stormRoot = stormRoot;
        }

        @Override
        public String toString() {
            List<String> data;
            String stormVersion;
            synchronized (this) {
                data = depLocs;
                stormVersion = this.stormVersion;
            }
            return "META for " + topologyId + " DEP_LOCS => " + data + " STORM_VERSION => " + stormVersion;
        }

        private synchronized void readData() throws IOException {
            final StormTopology stormTopology = ConfigUtils.readSupervisorTopology(conf, topologyId, ops);
            final List<String> dependencyLocations = new ArrayList<>();
            if (stormTopology.get_dependency_jars() != null) {
                for (String dependency : stormTopology.get_dependency_jars()) {
                    dependencyLocations.add(new File(stormRoot, dependency).getAbsolutePath());
                }
            }

            if (stormTopology.get_dependency_artifacts() != null) {
                for (String dependency : stormTopology.get_dependency_artifacts()) {
                    dependencyLocations.add(new File(stormRoot, dependency).getAbsolutePath());
                }
            }
            depLocs = dependencyLocations;
            stormVersion = stormTopology.get_storm_version();
            dataCached = true;
        }

        public synchronized List<String> getDepLocs() throws IOException {
            if (!dataCached) {
                readData();
            }
            return depLocs;
        }

        public synchronized String getStormVersion() throws IOException {
            if (!dataCached) {
                readData();
            }
            return stormVersion;
        }
    }

    static class TopoMetaLruCache {
        public final int maxSize = 100; //We could make this configurable in the future...

        @SuppressWarnings("serial")
        private LinkedHashMap<String, TopologyMetaData> cache = new LinkedHashMap<String, TopologyMetaData>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, TopologyMetaData> eldest) {
                return (size() > maxSize);
            }
        };

        public synchronized TopologyMetaData get(final Map<String, Object> conf, final String topologyId, final AdvancedFSOps ops,
                                                 String stormRoot) {
            //Only go off of the topology id for now.
            TopologyMetaData dl = cache.get(topologyId);
            if (dl == null) {
                cache.putIfAbsent(topologyId, new TopologyMetaData(conf, topologyId, ops, stormRoot));
                dl = cache.get(topologyId);
            }
            return dl;
        }

        public synchronized void clear() {
            cache.clear();
        }
    }

    private class ProcessExitCallback implements ExitCodeCallback {
        private final String logPrefix;

        ProcessExitCallback(String logPrefix) {
            this.logPrefix = logPrefix;
        }

        @Override
        public void call(int exitCode) {
            LOG.info("{} exited with code: {}", logPrefix, exitCode);
            exitedEarly = true;
        }
    }
}

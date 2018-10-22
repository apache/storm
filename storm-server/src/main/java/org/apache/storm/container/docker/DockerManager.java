package org.apache.storm.container.docker;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.container.ResourceIsolationInterface;
import org.apache.storm.container.cgroup.core.MemoryCore;
import org.apache.storm.daemon.supervisor.ClientSupervisorUtils;
import org.apache.storm.daemon.supervisor.ExitCodeCallback;
import org.apache.storm.shade.com.google.common.io.Files;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ServerUtils;
import org.apache.storm.utils.ShellCommandRunnerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DockerManager implements ResourceIsolationInterface {
    private static final Logger LOG = LoggerFactory.getLogger(DockerManager.class);
    private static final String TOPOLOGY_ENV_DOCKER_IMAGE = "DOCKER_IMAGE";
    private static final String TOPOLOGY_ENV_DOCKER_CONTAINER_NETWORK = "DOCKER_CONTAINER_NETWORK";
    private static final String DOCKER_IMAGE_PATTERN =
        "^(([a-zA-Z0-9.-]+)(:\\d+)?/)?([a-z0-9_./-]+)(:[\\w.-]+)?$";
    private static final Pattern dockerImagePattern =
        Pattern.compile(DOCKER_IMAGE_PATTERN);
    private String dockerExecutable;
    private String defaultDockerImage;
    private final String networkType = "host";
    private String cgroupParent;
    private String memoryCgroupRootPath;
    private String cgroupRootPath;
    private String nsenterExecutablePath;
    private String nscdPath;
    private Map<String, Object> conf;
    private Map<String, Integer> workerToCpu = new HashMap<>();
    private Map<String, Integer> workerToMemoryMb = new HashMap<>();
    private Map<String, String> workerToCid = new HashMap<>();
    private MemoryCore memoryCoreAtRoot;
    private String seccompJsonFile;
    private String stormHome;
    private final String TMP_DIR = File.separator + "tmp";

    @Override
    public void prepare(Map<String, Object> conf) throws IOException {
        this.conf = conf;
        dockerExecutable = ObjectReader.getString(conf.get(DaemonConfig.STORM_DOCKER_EXECUTABLE_PATH));
        //default configs can't be null
        defaultDockerImage = (String) conf.get(DaemonConfig.STORM_DOCKER_IMAGE);
        if (defaultDockerImage == null || !dockerImagePattern.matcher(defaultDockerImage).matches()) {
            throw new IllegalArgumentException(DaemonConfig.STORM_DOCKER_IMAGE + " is not set or it doesn't match " + DOCKER_IMAGE_PATTERN);
        }
        seccompJsonFile = (String) conf.get(DaemonConfig.STORM_DOCKER_SECCOMP_PROFILE);
        cgroupParent = ObjectReader.getString(conf.get(DaemonConfig.STORM_DOCKER_CGROUP_PARENT));
        cgroupRootPath = ObjectReader.getString(conf.get(DaemonConfig.STORM_DOCKER_CGROUP_ROOT));
        nsenterExecutablePath = ObjectReader.getString(conf.get(DaemonConfig.STORM_NSENTER_EXECUTABLE_PATH));
        nscdPath = ObjectReader.getString(conf.get(DaemonConfig.STORM_DOCKER_NSCD_DIR));
        memoryCgroupRootPath = cgroupRootPath + File.separator + "memory" + File.separator + cgroupParent;
        memoryCoreAtRoot = new MemoryCore(memoryCgroupRootPath);

        stormHome = System.getProperty(ConfigUtils.STORM_HOME);
        // Since we are bind mounting STORM_HOME as readonly, read-write bind mounts can't be under STORM_HOME
        if (ConfigUtils.workerRoot(conf).startsWith(stormHome)
            || ConfigUtils.workerArtifactsRoot(conf).startsWith(stormHome)
            || ConfigUtils.workerUserRoot(conf).startsWith(stormHome)) {
            throw new IllegalArgumentException(Config.STORM_LOCAL_DIR
                + " or " + Config.STORM_WORKERS_ARTIFACTS_DIR
                + " must not be under " + ConfigUtils.STORM_HOME + " directory");
        }

        if (stormHome.startsWith(TMP_DIR)) {
            throw new IllegalArgumentException(ConfigUtils.STORM_HOME
                + " can't be under " + TMP_DIR + " directory");
        }
    }

    @Override
    public void reserveResourcesForWorker(String workerId, Integer workerMemory, Integer workerCpu) {
        // The manually set STORM_WORKER_CGROUP_CPU_LIMIT config on supervisor will overwrite resources assigned by
        // RAS (Resource Aware Scheduler)
        if (conf.get(DaemonConfig.STORM_WORKER_CGROUP_CPU_LIMIT) != null) {
            workerCpu = ((Number) conf.get(DaemonConfig.STORM_WORKER_CGROUP_CPU_LIMIT)).intValue();
        }
        workerToCpu.put(workerId, workerCpu);

        if ((boolean) this.conf.get(DaemonConfig.STORM_CGROUP_MEMORY_ENFORCEMENT_ENABLE)) {
            workerToMemoryMb.put(workerId, workerMemory);
        }
    }

    @Override
    public void releaseResourcesForWorker(String workerId) {
        workerToCpu.remove(workerId);
        workerToMemoryMb.remove(workerId);
        workerToCid.remove(workerId);
    }

    private String[] getGroupIdInfo(String userName)
        throws IOException {
        String[] groupIds;
        try {
            String output = new ShellCommandRunnerImpl().execCommand("id", "--groups", userName);
            groupIds = output.trim().split(" ");
        } catch (IOException e) {
            LOG.error("Can't get group IDs of the user {}", userName);
            throw new IOException(e);
        }
        return groupIds;
    }

    private String getUserIdInfo(String userName) throws IOException {
        String uid = "";
        try {
            uid = new ShellCommandRunnerImpl().execCommand("id", "--user", userName).trim();
        } catch (IOException e) {
            LOG.error("Can't get uid of the user {}", userName);
            throw e;
        }
        return uid;
    }

    @Override
    public void launchWorkerProcess(String user, String topologyId, int port, String workerId, List<String> command, Map<String, String> env,
                                    String logPrefix, ExitCodeCallback processExitCallback, File targetDir) throws IOException {
        String dockerImage = env.get(TOPOLOGY_ENV_DOCKER_IMAGE);
        if (dockerImage == null || dockerImage.isEmpty()) {
            dockerImage = defaultDockerImage;
        }

        String workerDir = targetDir.getAbsolutePath();

        String uid = getUserIdInfo(user);
        String[] groups = getGroupIdInfo(user);
        String gid = groups[0];
        String dockerUser = uid + ":" + gid;

        DockerRunCommand dockerRunCommand = new DockerRunCommand(dockerExecutable, workerId, dockerUser, dockerImage);

        //set of locations to be bind mounted
        String workerRootDir = ConfigUtils.workerRoot(conf, workerId);
        String workerArtifactsRoot = ConfigUtils.workerArtifactsRoot(conf, topologyId, port);
        String workerUserFile = ConfigUtils.workerUserFile(conf, workerId);
        String sharedByTopologyTmpDir = ConfigUtils.sharedByTopologyTmpDir(conf, topologyId);

        // Theoretically we only need to mount ConfigUtils.supervisorStormDistRoot directory.
        // But if supervisorLocalDir is not mounted, the worker will try to create it and fail.
        String supervisorLocalDir = ConfigUtils.supervisorLocalDir(conf);

        dockerRunCommand.setNetworkType(networkType)
            //The whole file system of the container will be read-only except specific read-write bind mounts
            .setReadonly()
            .addReadOnlyMountLocation(cgroupRootPath, cgroupRootPath, false)
            .addReadOnlyMountLocation(stormHome, stormHome, false)
            .addReadOnlyMountLocation(supervisorLocalDir, supervisorLocalDir, false)
            .addMountLocation(workerRootDir, workerRootDir, false)
            .addMountLocation(workerArtifactsRoot, workerArtifactsRoot, false)
            .addMountLocation(workerUserFile, workerUserFile, false)
            //nscd must be running so that profiling can work properly
            .addMountLocation(nscdPath, nscdPath, false)
            //This is to make /tmp directory in container writable. This is very important.
            // For example
            // 1. jvm needs to write to /tmp/hsperfdata_<user> directory so that jps can work
            // 2. jstack needs to create a socket under /tmp directory.
            //Otherwise profiling will not work properly.
            .addMountLocation(sharedByTopologyTmpDir, TMP_DIR, false);

        dockerRunCommand.setCGroupParent(cgroupParent)
            .groupAdd(groups)
            .setContainerWorkDir(workerDir)
            .setCidFile(dockerCidFilePath(workerDir))
            .setCapabilities(Collections.emptySet())
            .setNoNewPrivileges();

        if (seccompJsonFile != null) {
            dockerRunCommand.setSeccompProfile(seccompJsonFile);
        }

        if (workerToCpu.containsKey(workerId)) {
            dockerRunCommand.setCpus(workerToCpu.get(workerId) / 100.0);
        }

        if (workerToMemoryMb.containsKey(workerId)) {
            dockerRunCommand.setMemoryMb(workerToMemoryMb.get(workerId));
        }

        dockerRunCommand.setOverrideCommandWithArgs(Arrays.asList("bash", ServerUtils.writeScript(workerDir, command, env)));

        runDockerCommand(conf, user, CmdType.LAUNCH_DOCKER_CONTAINER,
            dockerRunCommand.getCommandWithArguments(), null, logPrefix, processExitCallback, targetDir);

        //waiting for container id file to be written
        File cidFile = new File(dockerCidFilePath(workerDir));
        String cid = null;
        int retryCount = 1;
        do {
            try {
                Thread.sleep(100 * retryCount);
            } catch (InterruptedException e) {
                LOG.error("reading cid file got interrupted");
                throw new RuntimeException("Failed to read cid file: " + cidFile);
            }

            if (cidFile.exists()) {
                List<String> lines = Files.readLines(cidFile, Charset.defaultCharset());
                if (lines.isEmpty()) {
                    LOG.debug("cid file {} is empty. Retrying {}", cidFile, retryCount);
                } else {
                    cid = lines.get(0);
                }
            } else {
                LOG.debug("cid file {} doesn't exist. Retrying {}", cidFile, retryCount);
            }
            retryCount++;
        } while (cid == null);

        LOG.info("workerId: {}, cid={}", workerId, cid);
        workerToCid.put(workerId, cid);
    }

    @Override
    public long getMemoryUsage(String user, String workerId) throws IOException {
        String cid = workerToCid.get(workerId);
        if (cid == null) {
            //Get the worker PID outside of the container.
            DockerInspectCommand dockerInspectCommand = new DockerInspectCommand(dockerExecutable, workerId);
            dockerInspectCommand.withGettingCID();

            List<String> outputFromInspect = getOutputFromRunningDockerCommand(conf, user, CmdType.EXEC_CMD_AS_ROOT,
                dockerInspectCommand.getCommandWithArguments(), null, new File(ConfigUtils.workerRoot(conf, workerId)));

            if (!outputFromInspect.isEmpty()) {
                cid = outputFromInspect.get(outputFromInspect.size() - 1);
                workerToCid.put(workerId, cid);
            } else {
                LOG.error("Couldn't get container id of worker " + workerId);
                throw new IllegalStateException("Couldn't get container id of worker " + workerId);
            }
        }

        String memoryCgroupPath = containerCgroupPath(memoryCgroupRootPath, cid);
        MemoryCore memoryCore = new MemoryCore(memoryCgroupPath);
        return memoryCore.getPhysicalUsage();
    }

    @Override
    public long getSystemFreeMemoryMb() throws IOException {
        long rootCgroupLimitFree = Long.MAX_VALUE;

        try {
            //For cgroups no limit is max long.
            long limit = memoryCoreAtRoot.getPhysicalUsageLimit();
            long used = memoryCoreAtRoot.getMaxPhysicalUsage();
            rootCgroupLimitFree = (limit - used) / 1024 / 1024;
        } catch (FileNotFoundException e) {
            //Ignored if cgroups is not setup don't do anything with it
        }

        long res = Long.min(rootCgroupLimitFree, ServerUtils.getMemInfoFreeMb());

        return res;
    }

    @Override
    public void kill(String user, String workerId) throws IOException {
        String workerDir = ConfigUtils.workerRoot(conf, workerId);
        DockerStopCommand dockerStopCommand = new DockerStopCommand(dockerExecutable, workerId);
        runDockerCommandWaitFor(conf, user, CmdType.EXEC_CMD_AS_ROOT, dockerStopCommand.getCommandWithArguments(),
            null, null, null, new File(workerDir));

        DockerRmCommand dockerRmCommand = new DockerRmCommand(dockerExecutable, workerId);
        runDockerCommandWaitFor(conf, user, CmdType.EXEC_CMD_AS_ROOT, dockerRmCommand.getCommandWithArguments(),
            null, null, null, new File(workerDir));
    }

    @Override
    public void forceKill(String user, String workerId) throws IOException {
        String workerDir = ConfigUtils.workerRoot(conf, workerId);
        DockerRmCommand dockerRmCommand = new DockerRmCommand(dockerExecutable, workerId);
        dockerRmCommand.withForce();
        runDockerCommandWaitFor(conf, user, CmdType.EXEC_CMD_AS_ROOT, dockerRmCommand.getCommandWithArguments(),
            null, null, null, new File(workerDir));
    }

    /**
     * Currently it only checks if the container is alive.
     * If the worker process inside the container die, the container will exit.
     * So we only need to check if the container is alive to know if the worker process is still alive.
     *
     * @param user     the user of the processes
     * @param workerId the id of the worker to kill
     * @return true if all processes are dead
     * @throws IOException on I/O exception
     */
    @Override
    public boolean areAllProcessesDead(String user, String workerId) throws IOException {
        String workerDir = ConfigUtils.workerRoot(conf, workerId);
        DockerInspectCommand dockerInspectCommand = new DockerInspectCommand(dockerExecutable, workerId);
        dockerInspectCommand.withGettingContainerStatus();

        int exitCode = runDockerCommandWaitFor(conf, user, CmdType.EXEC_CMD_AS_ROOT, dockerInspectCommand.getCommandWithArguments(),
            null, null, null, new File(workerDir));
        return exitCode != 0;
    }

    @Override
    public boolean isResourceManaged() {
        return true;
    }

    @Override
    public boolean runProfilingCommand(String user, String workerId, List<String> command, Map<String, String> env,
                                       String logPrefix, File targetDir) throws IOException, InterruptedException {
        String workerDir = targetDir.getAbsolutePath();

        //Get the worker PID outside of the container.
        DockerInspectCommand dockerInspectCommand = new DockerInspectCommand(dockerExecutable, workerId);
        dockerInspectCommand.withGettingContainerPID();

        List<String> outputFromInspect = getOutputFromRunningDockerCommand(conf, user, CmdType.EXEC_CMD_AS_ROOT,
            dockerInspectCommand.getCommandWithArguments(), env, targetDir);

        if (outputFromInspect.isEmpty()) {
            LOG.error("Can't find the container PID");
            return false;
        }
        String workerPidOutSideOfContainer = outputFromInspect.get(outputFromInspect.size() - 1);
        LOG.info("The container PID is {}", workerPidOutSideOfContainer);

        //run nsenter
        String nsenterCmd = nsenterExecutablePath + " --target " + workerPidOutSideOfContainer + " --mount --pid";

        String nsenterScriptPath = dockerCommandFilePath(workerDir);
        try (BufferedWriter out = new BufferedWriter(new FileWriter(nsenterScriptPath))) {
            out.write(nsenterCmd);
        }

        List<String> args = Arrays.asList(CmdType.EXEC_CMD_AS_ROOT.toString(), workerDir, nsenterScriptPath);

        Process process = ClientSupervisorUtils.processLauncher(conf, user, null, args,
            env, logPrefix, null, targetDir);

        PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(process.getOutputStream())), true);

        //from nsenter, run profiling
        String profilingCmd = StringUtils.join(command, " ");
        profilingCmd = "sudo -u " + user + " " + profilingCmd;
        LOG.debug("executing profiling command: {}", profilingCmd);
        writer.println(profilingCmd);

        LOG.debug("executing exit command from nsenter");
        writer.println("exit");

        process.waitFor();

        int exitCode = process.exitValue();
        LOG.debug("exitCode from nsenter: {}", exitCode);

        return exitCode == 0;
    }

    private String dockerCidFilePath(String dir) {
        return dir + File.separator + "container.cid";
    }

    private String dockerCommandFilePath(String dir) {
        return dir + File.separator + "docker-command.sh";
    }

    private String containerCgroupPath(String dir, String cid) {
        return dir + File.separator + "docker-" + cid + ".scope";
    }

    /**
     * Run docker command using {@link Config#SUPERVISOR_WORKER_LAUNCHER}.
     *
     * @param conf             the storm conf
     * @param dockerCommand    the docker command to run
     * @param environment      the environment
     * @param logPrefix        the prefix of logs
     * @param exitCodeCallback the exit call back
     * @param targetDir        the working directory
     * @return the Process
     * @throws IOException on I/O exception
     */
    private Process runDockerCommand(Map<String, Object> conf, String user, CmdType cmdType, String dockerCommand,
                                     Map<String, String> environment, final String logPrefix,
                                     final ExitCodeCallback exitCodeCallback, File targetDir) throws IOException {
        String workerDir = targetDir.getAbsolutePath();
        String dockerScriptPath = dockerCommandFilePath(workerDir);
        try (BufferedWriter out = new BufferedWriter(new FileWriter(dockerScriptPath))) {
            out.write(dockerCommand);
        }

        List<String> args = Arrays.asList(cmdType.toString(), workerDir, dockerScriptPath);

        return ClientSupervisorUtils.processLauncher(conf, user, null, args, environment,
            logPrefix, exitCodeCallback, targetDir);
    }

    private List<String> getOutputFromRunningDockerCommand(Map<String, Object> conf, String user, CmdType cmdType, String dockerCommand,
                                                           Map<String, String> environment, File targetDir) throws IOException {
        List<String> outputs = new ArrayList<>();
        Process p = runDockerCommand(conf, user, cmdType, dockerCommand, environment, null, null, targetDir);
        try {
            p.waitFor();
            BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = r.readLine()) != null) {
                outputs.add(line);
            }
        } catch (InterruptedException e) {
            LOG.error("running docker command is interrupted", e);
        } catch (IOException e) {
            LOG.warn("Error while trying to log stream", e);
        }
        LOG.info("output from command is {}", StringUtils.join(outputs, " "));
        return outputs;
    }

    /**
     * Run docker command using {@link Config#SUPERVISOR_WORKER_LAUNCHER}.
     *
     * @param conf             the storm conf
     * @param dockerCommand    the docker command to run
     * @param environment      the environment
     * @param logPrefix        the prefix of logs
     * @param exitCodeCallback the exit call back
     * @param targetDir        the working directory
     * @return the Process
     * @throws IOException on I/O exception
     */
    private int runDockerCommandWaitFor(Map<String, Object> conf, String user, CmdType cmdType, String dockerCommand,
                                        Map<String, String> environment, final String logPrefix,
                                        final ExitCodeCallback exitCodeCallback, File targetDir) throws IOException {
        Process p = runDockerCommand(conf, user, cmdType, dockerCommand, environment, logPrefix, exitCodeCallback, targetDir);
        try {
            p.waitFor();
        } catch (InterruptedException e) {
            LOG.error("running docker command is interrupted", e);
        }
        return p.exitValue();
    }

    enum CmdType {
        LAUNCH_DOCKER_CONTAINER("launch-docker-container"),
        EXEC_CMD_AS_ROOT("exec-cmd-as-root");

        private final String name;

        CmdType(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return this.name;
        }
    }
}
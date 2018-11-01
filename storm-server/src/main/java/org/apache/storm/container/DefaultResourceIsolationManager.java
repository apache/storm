package org.apache.storm.container;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.storm.Config;
import org.apache.storm.daemon.supervisor.ClientSupervisorUtils;
import org.apache.storm.daemon.supervisor.ExitCodeCallback;
import org.apache.storm.daemon.supervisor.SupervisorUtils;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ServerUtils;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultResourceIsolationManager implements ResourceIsolationInterface {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultResourceIsolationManager.class);
    protected Map<String, Object> conf;
    protected boolean runAsUser;

    @Override
    public void prepare(Map<String, Object> conf) throws IOException {
        this.conf = conf;
        runAsUser = ObjectReader.getBoolean(conf.get(Config.SUPERVISOR_RUN_WORKER_AS_USER), false);
    }

    @Override
    public void reserveResourcesForWorker(String workerId, Integer workerMemory, Integer workerCpu) {
        //NO OP
    }

    @Override
    public void releaseResourcesForWorker(String workerId) {
        //NO OP
    }

    @Override
    public void launchWorkerProcess(String user, String topologyId, int port, String workerId, List<String> command, Map<String, String> env,
                                    String logPrefix, ExitCodeCallback processExitCallback, File targetDir) throws IOException {
        if (runAsUser) {
            String workerDir = targetDir.getAbsolutePath();
            List<String> args = Arrays.asList("worker", workerDir, ServerUtils.writeScript(workerDir, command, env));
            ClientSupervisorUtils.processLauncher(conf, user, null, args, null,
                logPrefix, processExitCallback, targetDir);
        } else {
            ClientSupervisorUtils.launchProcess(command, env, logPrefix, processExitCallback, targetDir);
        }
    }

    @Override
    public long getMemoryUsage(String user, String workerId) throws IOException {
        return 0;
    }

    @Override
    public long getSystemFreeMemoryMb() throws IOException {
        return 0;
    }

    @Override
    public void kill(String user, String workerId) throws IOException {
        Set<Long> pids = getAllPids(workerId);
        for (Long pid : pids) {
            kill(pid, user);
        }
    }

    /**
     * Kill a given process.
     * @param pid the id of the process to kill
     * @throws IOException on I/O exception
     */
    private void kill(long pid, String user) throws IOException {
        if (runAsUser) {
            signal(pid, 15, user);
        } else {
            ServerUtils.killProcessWithSigTerm(String.valueOf(pid));
        }
    }

    @Override
    public void forceKill(String user, String workerId) throws IOException {
        Set<Long> pids = getAllPids(workerId);
        for (Long pid : pids) {
            forceKill(pid, user);
        }
    }

    /**
     * Kill a given process forcefully.
     * @param pid the id of the process to kill
     * @throws IOException on I/O exception
     */
    private void forceKill(long pid, String user) throws IOException {
        if (runAsUser) {
            signal(pid, 9, user);
        } else {
            ServerUtils.forceKillProcess(String.valueOf(pid));
        }
    }

    /**
     * Get all the pids that are a part of the container.
     * @return all of the pids that are a part of this container
     */
    protected Set<Long> getAllPids(String workerId) throws IOException {
        Set<Long> ret = new HashSet<>();
        for (String listing : ConfigUtils.readDirContents(ConfigUtils.workerPidsRoot(conf, workerId))) {
            ret.add(Long.valueOf(listing));
        }
        return ret;
    }

    private void signal(long pid, int signal, String user) throws IOException {
        List<String> commands = Arrays.asList("signal", String.valueOf(pid), String.valueOf(signal));
        String logPrefix = "kill -" + signal + " " + pid;
        ClientSupervisorUtils.processLauncherAndWait(conf, user, commands, null, logPrefix);
    }

    @Override
    public boolean areAllProcessesDead(String user, String workerId) throws IOException {
        Set<Long> pids = getAllPids(workerId);
        boolean allDead = true;
        for (Long pid : pids) {
            LOG.debug("Checking if pid {} owner {} is alive", pid, user);
            if (!isProcessAlive(pid, user)) {
                LOG.debug("{}: PID {} is dead", workerId, pid);
            } else {
                allDead = false;
                break;
            }
        }
        return allDead;
    }

    /**
     * Is a process alive and running?.
     * @param pid the PID of the running process
     * @param user the user that is expected to own that process
     * @return true if it is, else false
     * @throws IOException on I/O exception
     */
    private boolean isProcessAlive(long pid, String user) throws IOException {
        if (ServerUtils.IS_ON_WINDOWS) {
            return isWindowsProcessAlive(pid, user);
        }
        return isPosixProcessAlive(pid, user);
    }

    private boolean isWindowsProcessAlive(long pid, String user) throws IOException {
        boolean ret = false;
        ProcessBuilder pb = new ProcessBuilder("tasklist", "/fo", "list", "/fi", "pid eq " + pid, "/v");
        pb.redirectError(Redirect.INHERIT);
        Process p = pb.start();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            String read;
            while ((read = in.readLine()) != null) {
                if (read.contains("User Name:")) { //Check for : in case someone called their user "User Name"
                    //This line contains the user name for the pid we're looking up
                    //Example line: "User Name:    exampleDomain\exampleUser"
                    List<String> userNameLineSplitOnWhitespace = Arrays.asList(read.split(":"));
                    if (userNameLineSplitOnWhitespace.size() == 2) {
                        List<String> userAndMaybeDomain = Arrays.asList(userNameLineSplitOnWhitespace.get(1).trim().split("\\\\"));
                        String processUser = userAndMaybeDomain.size() == 2 ? userAndMaybeDomain.get(1) : userAndMaybeDomain.get(0);
                        if (user.equals(processUser)) {
                            ret = true;
                        } else {
                            LOG.info("Found {} running as {}, but expected it to be {}", pid, processUser, user);
                        }
                    } else {
                        LOG.error("Received unexpected output from tasklist command. Expected one colon in user name line. Line was {}",
                            read);
                    }
                    break;
                }
            }
        }
        return ret;
    }

    private boolean isPosixProcessAlive(long pid, String user) throws IOException {
        boolean ret = false;
        ProcessBuilder pb = new ProcessBuilder("ps", "-o", "user", "-p", String.valueOf(pid));
        pb.redirectError(Redirect.INHERIT);
        Process p = pb.start();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            String first = in.readLine();
            assert ("USER".equals(first));
            String processUser;
            while ((processUser = in.readLine()) != null) {
                if (user.equals(processUser)) {
                    ret = true;
                    break;
                } else {
                    LOG.info("Found {} running as {}, but expected it to be {}", pid, processUser, user);
                }
            }
        }
        return ret;
    }

    @Override
    public boolean runProfilingCommand(String user, String workerId, List<String> command, Map<String, String> env,
                                       String logPrefix, File targetDir) throws IOException, InterruptedException {
        if (runAsUser) {
            String td = targetDir.getAbsolutePath();
            LOG.info("Running as user: {} command: {}", user, command);
            String containerFile = ServerUtils.containerFilePath(td);
            if (Utils.checkFileExists(containerFile)) {
                SupervisorUtils.rmrAsUser(conf, containerFile, containerFile);
            }
            String scriptFile = ServerUtils.scriptFilePath(td);
            if (Utils.checkFileExists(scriptFile)) {
                SupervisorUtils.rmrAsUser(conf, scriptFile, scriptFile);
            }
            String script = ServerUtils.writeScript(td, command, env);
            List<String> args = Arrays.asList("profiler", td, script);
            int ret = ClientSupervisorUtils.processLauncherAndWait(conf, user, args, env, logPrefix);
            return ret == 0;
        } else {
            Process p = ClientSupervisorUtils.launchProcess(command, env, logPrefix, null, targetDir);
            int ret = p.waitFor();
            return ret == 0;
        }
    }

    /**
     * This class doesn't really manage resources.
     * @return false
     */
    @Override
    public boolean isResourceManaged() {
        return false;
    }
}

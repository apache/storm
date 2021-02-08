/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.container.docker;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates the docker run command and its command line arguments.
 */
public class DockerRunCommand extends DockerCommand {
    private static final Logger LOG = LoggerFactory.getLogger(DockerRunCommand.class);
    private static final String RUN_COMMAND = "run";
    private final String image;
    private List<String> overrideCommandWithArgs;

    /**
     * The Construction function.
     * @param containerName the container name
     * @param userInfo the info of the user, e.g. "uid:gid"
     * @param image the container image
     */
    public DockerRunCommand(String containerName, String userInfo, String image) {
        super(RUN_COMMAND);
        super.addCommandArguments("--name=" + containerName, "--user=" + userInfo);
        this.image = image;
    }

    /**
     * Add --rm option.
     * @return the self
     */
    public DockerRunCommand removeContainerOnExit() {
        super.addCommandArguments("--rm");
        return this;
    }

    /**
     * Add -d option.
     * @return the self
     */
    public DockerRunCommand detachOnRun() {
        super.addCommandArguments("-d");
        return this;
    }

    /**
     * Set --workdir option.
     * @param workdir the working directory
     * @return the self
     */
    public DockerRunCommand setContainerWorkDir(String workdir) {
        super.addCommandArguments("--workdir=" + workdir);
        return this;
    }

    /**
     * Set --net option.
     * @param type the network type
     * @return the self
     */
    public DockerRunCommand setNetworkType(String type) {
        super.addCommandArguments("--net=" + type);
        return this;
    }

    /**
     * Add bind mount locations.
     * @param sourcePath the source path
     * @param destinationPath the destination path
     * @param createSource if createSource is false and the source path doesn't exist, do nothing
     * @return the self
     */
    public DockerRunCommand addReadWriteMountLocation(String sourcePath, String
        destinationPath, boolean createSource) throws IOException {
        if (!createSource) {
            boolean sourceExists = new File(sourcePath).exists();
            if (!sourceExists) {
                throw new IOException("SourcePath " + sourcePath + " doesn't exit.");
            }
        }
        super.addCommandArguments("-v", sourcePath + ":" + destinationPath);
        return this;
    }

    public DockerRunCommand addReadWriteMountLocation(String sourcePath, String
        destinationPath) throws IOException {
        return addReadWriteMountLocation(sourcePath, destinationPath, true);
    }

    /**
     * Add all the rw bind mount locations.
     * @param paths the locations
     * @return the self
     */
    public DockerRunCommand addAllReadWriteMountLocations(List<String> paths) throws IOException {
        return addAllReadWriteMountLocations(paths, true);
    }

    /**
     * Add all the rw bind mount locations.
     * @param paths the locations
     * @param createSource if createSource is false and the source path doesn't exist, do nothing
     * @return the self
     */
    public DockerRunCommand addAllReadWriteMountLocations(List<String> paths,
                                                          boolean createSource) throws IOException {
        for (String dir: paths) {
            this.addReadWriteMountLocation(dir, dir, createSource);
        }
        return this;
    }

    /**
     * Add readonly bind mount location.
     * @param sourcePath the source path
     * @param destinationPath the destination path
     * @param createSource if createSource is false and the source path doesn't exist, do nothing
     * @return the self
     */
    public DockerRunCommand addReadOnlyMountLocation(String sourcePath, String destinationPath,
                                                     boolean createSource) throws IOException {
        if (!createSource) {
            boolean sourceExists = new File(sourcePath).exists();
            if (!sourceExists) {
                throw new IOException("SourcePath " + sourcePath + " doesn't exit.");
            }
        }
        super.addCommandArguments("-v", sourcePath + ":" + destinationPath + ":ro");
        return this;
    }

    /**
     * Add readonly bind mout location.
     * @param sourcePath the source path
     * @param destinationPath the destination path
     * @return the self
     */
    public DockerRunCommand addReadOnlyMountLocation(String sourcePath,
                                                     String destinationPath) throws IOException {
        return addReadOnlyMountLocation(sourcePath, destinationPath, true);
    }

    /**
     * Add all readonly locations.
     * @param paths the locations
     * @return the self
     */
    public DockerRunCommand addAllReadOnlyMountLocations(List<String> paths) throws IOException {
        return addAllReadOnlyMountLocations(paths, true);
    }

    /**
     * Add all readonly locations.
     * @param paths the locations
     * @param createSource if createSource is false and the source path doesn't exist, do nothing
     * @return the self
     */
    public DockerRunCommand addAllReadOnlyMountLocations(List<String> paths,
                                                         boolean createSource) throws IOException {
        for (String dir: paths) {
            this.addReadOnlyMountLocation(dir, dir, createSource);
        }
        return this;
    }

    public DockerRunCommand addCpuSetBindings(List<String> cores, String memoryNode) {
        if (!cores.isEmpty()) {
            super.addCommandArguments("--cpuset-cpus=" + StringUtils.join(cores, ","));
        }

        if (memoryNode != null) {
            super.addCommandArguments("--cpuset-mems=" + memoryNode);
        }
        return this;
    }

    /**
     * Set --cgroup-parent option.
     * @param parentPath the cgroup parent path
     * @return the self
     */
    public DockerRunCommand setCGroupParent(String parentPath) {
        super.addCommandArguments("--cgroup-parent=" + parentPath);
        return this;
    }

    /**
     * Set --privileged option to run a privileged container. Use with extreme care.
     * @return the self.
     */
    public DockerRunCommand setPrivileged() {
        super.addCommandArguments("--privileged");
        return this;
    }

    /**
     * Set capabilities of the container.
     * @param capabilities the capabilities to be added
     * @return the self
     */
    public DockerRunCommand setCapabilities(Set<String> capabilities) {
        //first, drop all capabilities
        super.addCommandArguments("--cap-drop=ALL");

        //now, add the capabilities supplied
        for (String capability : capabilities) {
            super.addCommandArguments("--cap-add=" + capability);
        }

        return this;
    }

    /**
     * Set --device option.
     * @param sourceDevice the source device
     * @param destinationDevice the destination device
     * @return the self
     */
    public DockerRunCommand addDevice(String sourceDevice, String destinationDevice) {
        super.addCommandArguments("--device=" + sourceDevice + ":" + destinationDevice);
        return this;
    }

    /**
     * Enable detach.
     * @return the self
     */
    public DockerRunCommand enableDetach() {
        super.addCommandArguments("--detach=true");
        return this;
    }

    /**
     * Disable detach.
     * @return the self
     */
    public DockerRunCommand disableDetach() {
        super.addCommandArguments("--detach=false");
        return this;
    }

    /**
     * Set --group-add option.
     * @param groups the groups to be added
     * @return the self
     */
    public DockerRunCommand groupAdd(String[] groups) {
        for (int i = 0; i < groups.length; i++) {
            super.addCommandArguments("--group-add " + groups[i]);
        }
        return this;
    }

    /**
     * Set extra commands and args. It can override the existing commands.
     * @param overrideCommandWithArgs the extra commands and args
     * @return the self
     */
    public DockerRunCommand setOverrideCommandWithArgs(
        List<String> overrideCommandWithArgs) {
        this.overrideCommandWithArgs = overrideCommandWithArgs;
        return this;
    }

    /**
     * Add --read-only option.
     * @return the self
     */
    public DockerRunCommand setReadonly() {
        super.addCommandArguments("--read-only");
        return this;
    }

    /**
     * Set --security-opt option.
     * @param jsonPath the path to the json file
     * @return the self
     */
    public DockerRunCommand setSeccompProfile(String jsonPath) {
        super.addCommandArguments("--security-opt seccomp=" + jsonPath);
        return this;
    }

    /**
     * Set no-new-privileges option.
     * @return the self
     */
    public DockerRunCommand setNoNewPrivileges() {
        super.addCommandArguments("--security-opt no-new-privileges");
        return this;
    }

    /**
     * Set cpuShares.
     * @param cpuShares the cpu shares
     * @return the self
     */
    public DockerRunCommand setCpuShares(int cpuShares) {
        // Zero sets to default of 1024.  2 is the minimum value otherwise
        if (cpuShares > 0 && cpuShares < 2) {
            cpuShares = 2;
        }
        super.addCommandArguments("--cpu-shares=" + String.valueOf(cpuShares));
        return this;
    }

    /**
     * Set the number of cpus to use.
     * @param cpus the number of cpus
     * @return the self
     */
    public DockerRunCommand setCpus(double cpus) {
        super.addCommandArguments("--cpus=" + cpus);
        return this;
    }

    /**
     * Set the number of memory in MB to use.
     * @param memoryMb the number of memory in MB
     * @return the self
     */
    public DockerRunCommand setMemoryMb(int memoryMb) {
        super.addCommandArguments("--memory=" + memoryMb + "m");
        return this;
    }

    /**
     * Set the output container id file location.
     * @param cidFile the container id file
     * @return the self
     */
    public DockerRunCommand setCidFile(String cidFile) {
        super.addCommandArguments("--cidfile=" + cidFile);
        return this;
    }

    /**
     * Get the full command.
     * @return the full command
     */
    @Override
    public String getCommandWithArguments() {
        List<String> argList = new ArrayList<>();

        argList.add(super.getCommandWithArguments());
        argList.add(image);

        if (overrideCommandWithArgs != null) {
            argList.addAll(overrideCommandWithArgs);
        }

        return StringUtils.join(argList, " ");
    }
}


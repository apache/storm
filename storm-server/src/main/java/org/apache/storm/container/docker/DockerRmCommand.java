package org.apache.storm.container.docker;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;

/**
 * Encapsulates the docker rm command and its command
 * line arguments.
 */
public class DockerRmCommand extends DockerCommand {
    private static final String RM_COMMAND = "rm";
    private String containerName;

    public DockerRmCommand(String executable, String containerName) {
        super(executable, RM_COMMAND);
        this.containerName = containerName;
    }

    public DockerRmCommand withForce() {
        super.addCommandArguments("--force");
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
        argList.add(containerName);
        return StringUtils.join(argList, " ");
    }
}

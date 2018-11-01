package org.apache.storm.container.docker;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;

/**
 * Encapsulates the docker exec command and its command
 * line arguments.
 */
public class DockerExecCommand extends DockerCommand {
    private static final String EXEC_COMMAND = "exec";
    private String containerName;
    private List<String> commandInContainer;

    public DockerExecCommand(String executable, String containerName) {
        super(executable, EXEC_COMMAND);
        this.containerName = containerName;
    }

    /**
     * Add the command to run from inside container.
     * @param commandInContainer the command to run from inside container
     * @return the self
     */
    public DockerExecCommand addExecCommand(List<String> commandInContainer) {
        this.commandInContainer = commandInContainer;
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
        argList.addAll(commandInContainer);
        return StringUtils.join(argList, " ");
    }
}

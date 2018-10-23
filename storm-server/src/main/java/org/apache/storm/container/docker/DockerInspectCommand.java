package org.apache.storm.container.docker;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;

/**
 * Encapsulates the docker inspect command and its command
 * line arguments.
 */
public class DockerInspectCommand extends DockerCommand {
    private static final String INSPECT_COMMAND = "inspect";
    private String containerName;

    public DockerInspectCommand(String containerName) {
        super(INSPECT_COMMAND);
        this.containerName = containerName;
    }

    public DockerInspectCommand withGettingContainerStatus() {
        super.addCommandArguments("--format='{{.State.Status}}'");
        return this;
    }

    public DockerInspectCommand withGettingContainerPID() {
        super.addCommandArguments("--format='{{.State.Pid}}'");
        return this;
    }

    public DockerInspectCommand withGettingCID() {
        super.addCommandArguments("--format='{{.Id}}'");
        return this;
    }

    /**
     * Get the full command.
     * @return the full command.
     */
    @Override
    public String getCommandWithArguments() {
        List<String> argList = new ArrayList<>();
        argList.add(super.getCommandWithArguments());
        argList.add(containerName);
        return StringUtils.join(argList, " ");
    }
}

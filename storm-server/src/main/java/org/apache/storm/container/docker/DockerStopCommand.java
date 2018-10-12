package org.apache.storm.container.docker;

/**
 * Encapsulates the docker stop command and its command
 * line arguments.
 */
public class DockerStopCommand extends DockerCommand {
    private static final String STOP_COMMAND = "stop";

    public DockerStopCommand(String executable, String containerName) {
        super(executable, STOP_COMMAND);
        super.addCommandArguments(containerName);
    }

    public DockerStopCommand setGracePeriod(int value) {
        super.addCommandArguments("--time=" + Integer.toString(value));
        return this;
    }
}

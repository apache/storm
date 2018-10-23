package org.apache.storm.container.docker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang.StringUtils;

public abstract class DockerCommand  {
    private final String command;
    private final List<String> commandWithArguments;

    protected DockerCommand(String command) {
        this.command = command;
        this.commandWithArguments = new ArrayList<>();
        commandWithArguments.add(command);
    }

    /** Returns the docker sub-command string being used, e.g 'run'.
     * @return the sub-command
     */
    public final String getCommandOption() {
        return this.command;
    }

    /** Add command commandWithArguments. This method is only meant for use by sub-classes.
     * @param arguments to be added
     */
    protected final void addCommandArguments(String... arguments) {
        this.commandWithArguments.addAll(Arrays.asList(arguments));
    }

    /**
     * Get the full command.
     * @return the full command
     */
    public String getCommandWithArguments() {
        return StringUtils.join(commandWithArguments, " ");
    }
}
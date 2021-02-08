/*
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

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;

/**
 * Encapsulates the docker exec command and its command line arguments.
 */
public class DockerExecCommand extends DockerCommand {
    private static final String EXEC_COMMAND = "exec";
    private String containerName;
    private List<String> commandInContainer;

    public DockerExecCommand(String containerName) {
        super(EXEC_COMMAND);
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

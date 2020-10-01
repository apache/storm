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
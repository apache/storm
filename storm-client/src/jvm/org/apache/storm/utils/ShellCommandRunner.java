/*
 * Copyright 2017 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.utils;

import java.io.IOException;
import java.util.Map;

/**
 * Contains convenience functions for running shell commands for cases that are too simple to need a full {@link ShellUtils}
 * implementation.
 */
public interface ShellCommandRunner {

    /**
     * Method to execute a shell command. Covers most of the simple cases without requiring the user to implement the {@link ShellUtils}
     * interface.
     *
     * @param cmd shell command to execute.
     * @return the output of the executed command.
     */
    String execCommand(String... cmd) throws IOException;

    /**
     * Method to execute a shell command. Covers most of the simple cases without requiring the user to implement the {@link ShellUtils}
     * interface.
     *
     * @param env     the map of environment key=value
     * @param cmd     shell command to execute.
     * @param timeout time in milliseconds after which script should be marked timeout
     * @return the output of the executed command.
     */

    String execCommand(Map<String, String> env, String[] cmd,
                       long timeout) throws IOException;

    /**
     * Method to execute a shell command. Covers most of the simple cases without requiring the user to implement the {@link ShellUtils}
     * interface.
     *
     * @param env the map of environment key=value
     * @param cmd shell command to execute.
     * @return the output of the executed command.
     */
    String execCommand(Map<String, String> env, String... cmd)
        throws IOException;

    /**
     * Token separator regex used to parse Shell tool outputs.
     */
    String getTokenSeparatorRegex();

}

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

import static org.junit.Assert.assertEquals;


import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class DockerRunCommandTest {
    private DockerRunCommand dockerRunCommand;

    private static final String CONTAINER_NAME = "foo";
    private static final String USER_INFO = "user_id:group_id";
    private static final String IMAGE_NAME = "image_name";

    @Before
    public void setUp() {
        dockerRunCommand = new DockerRunCommand(CONTAINER_NAME, USER_INFO, IMAGE_NAME);
    }

    @Test
    public void getCommandWithArguments() {
        assertEquals("run", dockerRunCommand.getCommandOption());
    }

    @Test
    public void getCommandOption() throws IOException {
        String sourcePath = "source";
        String destPath = "dest";
        dockerRunCommand.detachOnRun()
            .addReadWriteMountLocation(sourcePath, destPath);
        List<String> commands = Arrays.asList("bash", "launch_command");
        dockerRunCommand.setOverrideCommandWithArgs(commands);
        dockerRunCommand.removeContainerOnExit();
        assertEquals("run --name=foo --user=user_id:group_id -d -v source:dest --rm "
                + "image_name bash launch_command",
            dockerRunCommand.getCommandWithArguments());
    }
}
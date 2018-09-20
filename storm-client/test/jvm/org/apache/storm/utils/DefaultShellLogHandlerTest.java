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

package org.apache.storm.utils;

import org.apache.storm.multilang.ShellMsg;
import org.apache.storm.multilang.ShellMsg.ShellLogLevel;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultShellLogHandlerTest {

    private DefaultShellLogHandler logHandler;

    @Before
    public void setUp() {
        logHandler = new DefaultShellLogHandler();
    }

    private ShellMsg mockMsg() {
        ShellMsg shellMsg = mock(ShellMsg.class);
        when(shellMsg.getMsg()).thenReturn("msg");
        when(shellMsg.getLogLevel()).thenReturn(ShellLogLevel.INFO);
        return shellMsg;
    }

    private ShellProcess mockProcess() {
        ShellProcess process = mock(ShellProcess.class);
        when(process.getProcessInfoString()).thenReturn("info");
        return process;
    }

    /**
     * It's fine to pass only null arguments to setUpContext.
     */
    @Test
    public void setUpContext_allNull() {
        ShellMsg msg = mockMsg();
        logHandler.setUpContext(null, null, null);
        logHandler.log(msg);
        verify(msg).getMsg();
    }

    /**
     * Calling setUpContext is optional.
     */
    @Test
    public void setUpContext_optional() {
        ShellMsg msg = mockMsg();
        logHandler.log(msg);
        verify(msg).getMsg();
    }

    /**
     * A null {@link ShellMsg} will throw IllegalArgumentException.
     */
    @Test(expected = IllegalArgumentException.class)
    public void handleLog_nullShellMsg() {
        logHandler.log(null);
    }

    /**
     * A null {@link ShellProcess} will not throw an exception.
     */
    @Test
    public void handleLog_nullProcess() {
        ShellMsg msg = mockMsg();
        ShellProcess process = mockProcess();
        logHandler.setUpContext(DefaultShellLogHandlerTest.class, process, null);
        logHandler.log(msg);
        verify(msg).getMsg();
    }

    /**
     * If both {@link ShellMsg} and {@link ShellProcess} are provided, both
     * will be used to build the log message.
     */
    @Test
    public void handleLog_valid() {
        ShellMsg msg = mockMsg();
        ShellProcess process = mockProcess();
        logHandler.setUpContext(DefaultShellLogHandlerTest.class, process, null);
        logHandler.log(msg);
        verify(msg).getMsg();
        verify(process).getProcessInfoString();
    }
}
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
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link ShellLogHandler}.
 */
public class DefaultShellLogHandler implements ShellLogHandler {
    private Logger log;

    /**
     * Save information about the current process.
     */
    private ShellProcess process;

    /**
     * Default constructor; used when loading with Class.forName(...).newInstance().
     */
    public DefaultShellLogHandler() {
    }

    private Logger getLogger(final Class<?> ownerCls) {
        return LoggerFactory.getLogger(
            ownerCls == null ? DefaultShellLogHandler.class : ownerCls);
    }

    /**
     * This default implementation saves the {@link ShellProcess} so it can output the process info string later.
     *
     * @param ownerCls - the class which instantiated this ShellLogHandler.
     * @param process  - the current {@link ShellProcess}.
     * @param context  - the current {@link TopologyContext}.
     * @see {@link ShellLogHandler#setUpContext}
     */
    @Override
    public void setUpContext(final Class<?> ownerCls, final ShellProcess process,
                             final TopologyContext context) {
        this.log = getLogger(ownerCls);
        this.process = process;
        // context is not used by the default implementation, but is included
        // in the interface in case it is useful to subclasses
    }

    /**
     * Log the given message.
     *
     * @param shellMsg - the {@link ShellMsg} to log.
     * @see {@link ShellLogHandler#log}
     */
    @Override
    public void log(final ShellMsg shellMsg) {
        if (shellMsg == null) {
            throw new IllegalArgumentException("shellMsg is required");
        }
        String msg = shellMsg.getMsg();
        if (this.log == null) {
            this.log = getLogger(null);
        }
        if (this.process == null) {
            msg = "ShellLog " + msg;
        } else {
            msg = "ShellLog " + process.getProcessInfoString() + " " + msg;
        }
        ShellMsg.ShellLogLevel logLevel = shellMsg.getLogLevel();

        switch (logLevel) {
            case TRACE:
                log.trace(msg);
                break;
            case DEBUG:
                log.debug(msg);
                break;
            case INFO:
                log.info(msg);
                break;
            case WARN:
                log.warn(msg);
                break;
            case ERROR:
                log.error(msg);
                break;
            default:
                log.info(msg);
                break;
        }
    }
}